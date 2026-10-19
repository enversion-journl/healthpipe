# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Per-SFI Observation builders for AKTIVITETSRESULTAT."""

import logging
import uuid
from collections import Counter
from datetime import date, datetime
from typing import Any

from DSA.CodeSystem.sfi.catalog import (
    SfiElement,
    add_sfi_catalog_element,
    sfi_datapoint_to_element,
)
from DSA.FHIR.Aktivitetsresultat.activity_metadata import (
    apply_aktivitetsudfoerelse_extensions,
    build_aktivitetsudfoerelse_extensions,
)
from DSA.FHIR.Aktivitetsresultat.questionnaire_outputs import (
    _append_questionnaire_outputs_for_row,
)
from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from DSA.FHIR.Aktivitetsresultat.routing_glue import (
    _get_routing_resolver,
    _has_value_bearing_xml_rows,
)
from DSA.FHIR.Condition.condition_aktivitetsresultat_builder import (
    _append_condition_routing_decision,
    _append_xml_diagnosis_condition_dicts,
    _build_aktivitetsresultat_condition_dict,
    _condition_observation_without_numeric_values_decision,
    _decision_with_parallel_condition,
)
from DSA.FHIR.Observation.observation_dict_builder import (
    FHIR_OBSERVATION_CATEGORY_SYSTEM,
    _to_fhir_datetime_string,
)
from DSA.FHIR.Procedure.procedure_aktivitetsresultat_builder import (
    _append_procedure_routing_decision,
    _decision_with_parallel_procedure,
    _procedure_observation_without_numeric_values_decision,
    build_aktivitetsresultat_procedure_dicts,
)
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import _is_value_carrying_row
from DSA.FHIR.utils.code_normalization import (
    is_sks_diagnosis_code,
    is_sks_procedure_code,
)
from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import _OBS_BUILD_STAGE
from DSA.FHIR.utils.ifdb.aktivitetsresultat_identifiers import _sfi_identifier_value
from DSA.FHIR.utils.ifdb.dict_validation import (
    _validate_condition_dicts_sample,
    _validate_observation_dicts_sample,
    _validate_procedure_dicts_sample,
)
from DSA.FHIR.utils.ifdb.routing import RoutingResolver, TargetResource
from DSA.FHIR.utils.ifdb.routing_audit import append_routing_decision as _append_routing_decision
from DSA.FHIR.utils.ifdb.routing_fallback import (
    RoutingDecision,
    resolve_target_with_fallback,
)
from DSA.FHIR.utils.ifdb.sfi_contract import infer_sfi_datatype
from DSA.FHIR.utils.ifdb.sfi_extraction import SfiDatapoint, extract_sfi_datapoints
from DSA.FHIR.utils.ifdb.transforms import (
    build_performer_dict,
    parse_clob_datetime,
    to_fhir_instant,
)
from DSA.FHIR.utils.ifdb.transforms import is_true_flag as _is_true_flag
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.transforms import source_deleted_to_int as _slettet_to_int
from DSA.FHIR.utils.ifdb.value_mapping import SfiValueTypeMismatch, map_sfi_value_to_fhir
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    AktivitetsresultatRow,
)
from utils.fhir.fhir_extensions import EXTENSION_EPISODE_OF_CARE, EXTENSION_SFI_INSTANCE_META
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_SFI,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension_dict

_logger = logging.getLogger(__name__)


def _build_sfi_observation_dict(
    row_dict: dict[str, Any],
    dp: SfiDatapoint,
    *,
    parent_objekt_id_norm: str | None,
    patient_id_norm: str | None,
    encounter_id_norm: str | None,
    episode_id_norm: str | None,
    status: str,
    deleted_int: int,
    start_str: str | None,
    end_str: str | None,
    issued_instant: str | None,
    observation_category: str | None,
    resolved: ResolvedRefLookup | None,
) -> tuple[str, dict] | None:
    """Build one FHIR Observation dict for a single SFI datapoint.

    Returns ``None`` when the datapoint should be skipped (empty value or a
    type mismatch -- already logged by the caller).
    """
    sfi_code = dp.sfi_code
    if not sfi_code:
        return None

    value_field = map_sfi_value_to_fhir(
        sfi_code=sfi_code,
        datatype=dp.datatype,
        raw_value=dp.raw_value,
        unit=dp.unit or None,
    )

    if value_field is None:
        return None

    if parent_objekt_id_norm is None or dp.element_nummer is None:
        sfi_id_value = None
    else:
        sfi_id_value = _sfi_identifier_value(parent_objekt_id_norm, dp.element_nummer)

    resource_id = (
        resolved.observation_id_sfi(parent_objekt_id_norm, dp.element_nummer)
        if resolved
        else str(uuid.uuid4())
    )

    obs: dict[str, Any] = {"resourceType": "Observation", "id": resource_id}

    identifiers: list[dict] = []
    if sfi_id_value is not None:
        identifiers.append({"system": SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "value": sfi_id_value})
    if identifiers:
        obs["identifier"] = identifiers

    obs["status"] = status

    coding_dict: dict[str, Any] = {
        "system": SYSTEM_IFDB_SFI,
        "code": sfi_code,
    }
    if dp.display:
        coding_dict["display"] = dp.display
    obs["code"] = {"coding": [coding_dict]}
    if dp.display:
        obs["code"]["text"] = dp.display

    if observation_category:
        obs["category"] = [
            {
                "coding": [
                    {
                        "system": FHIR_OBSERVATION_CATEGORY_SYSTEM,
                        "code": observation_category,
                    }
                ]
            }
        ]

    if patient_id_norm is not None:
        if resolved:
            obs["subject"] = resolved.patient_ref(patient_id_norm)
        else:
            obs["subject"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_PATIENT_ID,
                    "value": patient_id_norm,
                }
            }

    if encounter_id_norm is not None:
        if resolved:
            obs["encounter"] = resolved.encounter_ref(encounter_id_norm)
        else:
            obs["encounter"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_OBJECT_ID,
                    "value": encounter_id_norm,
                }
            }

    if start_str and end_str:
        obs["effectivePeriod"] = {"start": start_str, "end": end_str}
    elif start_str:
        obs["effectiveDateTime"] = start_str

    if issued_instant:
        obs["issued"] = issued_instant

    if value_field is not None:
        obs.update(value_field)

    extensions: list[dict] = [build_is_deleted_extension_dict(deleted_int)]
    if episode_id_norm:
        if resolved:
            extensions.append(
                {
                    "url": EXTENSION_EPISODE_OF_CARE,
                    "valueReference": resolved.episode_ref(episode_id_norm),
                }
            )
        else:
            extensions.append(
                {
                    "url": EXTENSION_EPISODE_OF_CARE,
                    "valueReference": {
                        "identifier": {
                            "system": SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                            "value": episode_id_norm,
                        }
                    },
                }
            )

    instance_parts: list[dict] = []
    if parent_objekt_id_norm is not None:
        instance_parts.append({"url": "aktivitetId", "valueString": parent_objekt_id_norm})
    if dp.element_nummer is not None:
        instance_parts.append({"url": "elementNummer", "valueInteger": dp.element_nummer})
    if dp.klynge_id is not None:
        instance_parts.append({"url": "klyngeId", "valueInteger": dp.klynge_id})
    if dp.klynge_id_hierarki:
        instance_parts.append({"url": "klyngeIdHierarki", "valueString": dp.klynge_id_hierarki})
    if dp.klynge_valg_ledetekst:
        instance_parts.append(
            {
                "url": "klyngeValgLedetekst",
                "valueString": dp.klynge_valg_ledetekst,
            }
        )
    if dp.klynge_parent_id is not None:
        instance_parts.append({"url": "klyngeParentId", "valueInteger": dp.klynge_parent_id})
    if dp.repetition_id is not None:
        instance_parts.append({"url": "repetitionId", "valueInteger": dp.repetition_id})
    if dp.element_kode:
        instance_parts.append({"url": "elementKode", "valueString": dp.element_kode})
    if dp.element_original_kode:
        instance_parts.append(
            {
                "url": "elementOriginalKode",
                "valueString": dp.element_original_kode,
            }
        )
    if dp.element_administrativ_enhed_id:
        instance_parts.append(
            {
                "url": "elementAdminEnhedId",
                "valueString": dp.element_administrativ_enhed_id,
            }
        )
    if dp.repetition_id_hierarki:
        instance_parts.append(
            {
                "url": "repetitionIdHierarki",
                "valueString": dp.repetition_id_hierarki,
            }
        )
    if dp.opdateret_tid:
        instance_parts.append(
            {
                "url": "opdateretTid",
                "valueString": dp.opdateret_tid,
            }
        )
    if dp.xml_status is not None:
        instance_parts.append({"url": "xmlStatus", "valueInteger": dp.xml_status})
    if dp.xml_status_text:
        instance_parts.append({"url": "xmlStatusText", "valueString": dp.xml_status_text})
    if instance_parts:
        extensions.append({"url": EXTENSION_SFI_INSTANCE_META, "extension": instance_parts})

    if extensions:
        obs["extension"] = extensions

    performer_refs = build_performer_dict(row_dict)
    if performer_refs:
        obs["performer"] = performer_refs

    return resource_id, obs


def _resolve_row_target_resource(
    row_dict: dict[str, Any],
    resolver: RoutingResolver,
    xml_rows: list[AktivitetsresultatRow] | None,
    logger: Any,
) -> RoutingDecision:
    """Resolve the FHIR target resource for one AKTIVITETSRESULTAT row."""
    period_start = row_dict.get("PERIOD_START")
    occurred_at: datetime | date | None = None
    if isinstance(period_start, datetime):
        occurred_at = period_start
    elif isinstance(period_start, str) and period_start:
        try:
            occurred_at = datetime.fromisoformat(period_start)
        except ValueError:
            occurred_at = None

    decision = resolve_target_with_fallback(
        row_dict,
        resolver,
        xml_rows,
        occurred_at=occurred_at,
    )
    return decision


def _format_counter_counts(counter: Counter[str]) -> str:
    return ", ".join(f"{key}={count}" for key, count in sorted(counter.items())) or "none"


def _log_sfi_routing_summary(
    logger: Any,
    routing_decisions: list[dict[str, str | None]],
    *,
    label: str,
) -> None:
    if not routing_decisions:
        return

    target_counts = Counter(
        record.get("target_resource") or "unknown" for record in routing_decisions
    )
    source_counts = Counter(
        record.get("routing_source") or "unknown" for record in routing_decisions
    )
    fallback_records = [
        record for record in routing_decisions if record.get("routing_source") != "routing_table"
    ]
    fallback_code_counts = Counter(
        record.get("aktivitetstype_kode") or "<missing>" for record in fallback_records
    )
    fallback_examples = ", ".join(
        f"{kode}={count}" for kode, count in fallback_code_counts.most_common(5)
    )

    logger.info(
        "[SFI] Routing summary %s: rows=%d targets=[%s] sources=[%s] fallback_rows=%d%s",
        label,
        len(routing_decisions),
        _format_counter_counts(target_counts),
        _format_counter_counts(source_counts),
        len(fallback_records),
        f" top_fallback_codes=[{fallback_examples}]" if fallback_examples else "",
    )


class SfiDuplicateOccurrenceError(Exception):
    """Two value-bearing SFI datapoints minted the same Observation resource id.

    The XML parser assigns a strictly-monotonic ``element_nummer`` per document,
    so the per-element id is unique by construction and this can only fire if that
    invariant is broken upstream. We surface it as a hard, sampled failure instead
    of silently collapsing one datapoint, because on a region-critical full load a
    silent collapse would be indistinguishable from correct output.
    """


def _is_numeric_sfi_datapoint(dp: SfiDatapoint) -> bool:
    return (dp.datatype or "").strip().lower() in {"integer", "decimal", "quantity"}


def _is_numeric_sfi_row(row: AktivitetsresultatRow) -> bool:
    """True when a parsed row's SFI datatype is numeric."""
    if (row.raekke_type or "") != "Element":
        return False
    datatype = infer_sfi_datatype(row.element_type, row.element_vaerdi)
    return (datatype or "").strip().lower() in {"integer", "decimal", "quantity"}


def _would_emit_numeric_sfi_observation(row: AktivitetsresultatRow) -> bool:
    """True when datatype and value can produce a numeric Observation."""
    if not _is_numeric_sfi_row(row):
        return False

    datatype = infer_sfi_datatype(row.element_type, row.element_vaerdi)
    try:
        return (
            map_sfi_value_to_fhir(
                sfi_code="__probe__",
                datatype=datatype,
                raw_value=row.element_vaerdi,
            )
            is not None
        )
    except SfiValueTypeMismatch:
        return False


def _build_sfi_observations_for_row(
    row_dict: dict[str, Any],
    xml_rows: list[AktivitetsresultatRow] | None,
    *,
    resolved: ResolvedRefLookup | None,
    routing_resolver: RoutingResolver,
    logger: Any,
    route_decision: RoutingDecision | None = None,
    numeric_values_only: bool = False,
) -> list[tuple[str, dict]]:
    """Build the per-SFI-element Observation dicts for one AKTIVITETSRESULTAT."""
    if route_decision is None:
        route_decision = _resolve_row_target_resource(
            row_dict,
            routing_resolver,
            xml_rows,
            logger,
        )
    if route_decision.target_resource != TargetResource.OBSERVATION:
        return []

    if not xml_rows:
        return []

    objekt_id = row_dict.get("OBJEKT_ID")
    patient_id = row_dict.get("PATIENT_ID")
    encounter_id = row_dict.get("ENCOUNTER_ID")
    episode_id = row_dict.get("EPISODE_OF_CARE_ID")
    objekt_id_norm = _normalize_oracle_id(objekt_id)
    patient_id_norm = _normalize_oracle_id(patient_id)
    encounter_id_norm = _normalize_oracle_id(encounter_id)
    episode_id_norm = _normalize_oracle_id(episode_id)
    model_name = row_dict.get("MODELLERINGSNAVN")
    model_version = row_dict.get("MODELLERINGSVERSION")
    period_start = row_dict.get("PERIOD_START")
    period_end = row_dict.get("PERIOD_END")
    kladde = row_dict.get("KLADDE")
    slettet = row_dict.get("SLETTET")
    udfoert_tidspunkt = row_dict.get("UDFOERT_TIDSPUNKT")

    is_draft = _is_true_flag(kladde)
    deleted_int = _slettet_to_int(slettet)
    is_deleted = deleted_int != 0
    status = "entered-in-error" if is_deleted else ("preliminary" if is_draft else "final")

    start_str: str | None = None
    end_str: str | None = None

    if period_start:
        try:
            if isinstance(period_start, str):
                parsed = parse_clob_datetime(period_start)
                start_str = (
                    _to_fhir_datetime_string(parsed)
                    if parsed
                    else _to_fhir_datetime_string(period_start)
                )
            else:
                start_str = _to_fhir_datetime_string(period_start)
        except Exception as exc:
            _logger.warning(
                "PERIOD_START parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )
    if period_end:
        try:
            if isinstance(period_end, str):
                parsed_end = parse_clob_datetime(period_end)
                end_str = (
                    _to_fhir_datetime_string(parsed_end)
                    if parsed_end
                    else _to_fhir_datetime_string(period_end)
                )
            else:
                end_str = _to_fhir_datetime_string(period_end)
        except Exception as exc:
            _logger.warning(
                "PERIOD_END parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )

    issued_instant: str | None = None
    if udfoert_tidspunkt:
        try:
            if isinstance(udfoert_tidspunkt, str):
                parsed_issued = parse_clob_datetime(udfoert_tidspunkt)
                issued_instant = to_fhir_instant(parsed_issued or udfoert_tidspunkt)
            else:
                issued_instant = to_fhir_instant(udfoert_tidspunkt)
        except Exception as exc:
            _logger.warning(
                "UDFOERT_TIDSPUNKT parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )

    observation_category = route_decision.observation_category

    datapoints = extract_sfi_datapoints(
        xml_rows,
        model_name=str(model_name) if model_name else None,
        model_version=str(model_version) if model_version else None,
    )

    results: list[tuple[str, dict]] = []
    seen_ids: set[str] = set()

    for dp in datapoints:
        if numeric_values_only and not _is_numeric_sfi_datapoint(dp):
            continue

        try:
            built = _build_sfi_observation_dict(
                row_dict=row_dict,
                dp=dp,
                parent_objekt_id_norm=objekt_id_norm,
                patient_id_norm=patient_id_norm,
                encounter_id_norm=encounter_id_norm,
                episode_id_norm=episode_id_norm,
                status=status,
                deleted_int=deleted_int,
                start_str=start_str,
                end_str=end_str,
                issued_instant=issued_instant,
                observation_category=observation_category,
                resolved=resolved,
            )
        except SfiValueTypeMismatch as exc:
            logger.warning(
                "[SFI] Datatype mismatch on OBJEKT_ID=%s element_nummer=%s code=%s "
                "datatype=%s raw=%r -- skipping element only",
                objekt_id,
                dp.element_nummer,
                exc.sfi_code,
                exc.datatype,
                exc.raw_value,
            )
            continue

        if built is None:
            continue

        rid, obs = built
        if rid in seen_ids:
            raise SfiDuplicateOccurrenceError(
                f"Duplicate per-element Observation id {rid!r} for "
                f"OBJEKT_ID={objekt_id} element_nummer={dp.element_nummer} "
                f"code={getattr(dp, 'sfi_code', None)!r}; the parser's per-document "
                "element_nummer is expected to be unique, so this signals a broken "
                "upstream invariant rather than a row to silently drop."
            )
        seen_ids.add(rid)
        results.append(built)

    return results


def _build_sfi_observation_dicts_for_rows(
    rows: list[dict[str, Any]],
    parsed_data_by_index: list,
    logger: Any,
    resolved: ResolvedRefLookup | None = None,
    routing_resolver: RoutingResolver | None = None,
    routing_decision_sink: list[dict[str, str | None]] | None = None,
    sfi_catalog_sink: dict[str, list[SfiElement]] | None = None,
    udf_lookup: dict[str, dict[str, Any]] | None = None,
    speciale_lookup: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[list[tuple[str, dict]], list[tuple[str, dict]], list[tuple[str, dict]], int]:
    """Orchestrate per-row SFI Observation build + parallel Procedure/Condition emission."""
    routing_resolver = routing_resolver or _get_routing_resolver()

    all_observations: list[tuple[str, dict]] = []
    procedure_dicts: list[tuple[str, dict]] = []
    condition_dicts: list[tuple[str, dict]] = []
    errors = 0
    logged_errors = 0

    for idx, row_dict in enumerate(rows):
        try:
            xml_rows = parsed_data_by_index[idx]
            if (
                not isinstance(xml_rows, list)
                or not xml_rows
                or not isinstance(xml_rows[0], AktivitetsresultatRow)
            ):
                xml_rows = None

            if sfi_catalog_sink is not None and xml_rows is not None:
                model_name = row_dict.get("MODELLERINGSNAVN")
                model_version = row_dict.get("MODELLERINGSVERSION")
                mn = str(model_name) if model_name else None
                mv = str(model_version) if model_version else None
                dps = extract_sfi_datapoints(xml_rows, model_name=mn, model_version=mv)
                for dp in dps:
                    elem = sfi_datapoint_to_element(dp, model_name=mn)
                    add_sfi_catalog_element(sfi_catalog_sink, elem)

            aktivitetstype_kode = row_dict.get("AKTIVITETTYPE_KODE")
            is_procedure_row = is_sks_procedure_code(aktivitetstype_kode)
            is_condition_row = is_sks_diagnosis_code(aktivitetstype_kode)

            xml_condition_dicts: list[tuple[str, dict]] = []
            _append_xml_diagnosis_condition_dicts(
                row_dict,
                xml_rows,
                resolved,
                xml_condition_dicts,
            )
            condition_dicts.extend(xml_condition_dicts)

            if is_procedure_row:
                procedure_dicts.extend(
                    build_aktivitetsresultat_procedure_dicts(row_dict, xml_rows, resolved)
                )
                if not _has_value_bearing_xml_rows(xml_rows):
                    _append_procedure_routing_decision(
                        routing_decision_sink,
                        row_dict,
                    )
                    continue
            elif is_condition_row:
                if not xml_condition_dicts:
                    # Structured DiagnoseKode XML elements are the clinical
                    # source of truth; when present, the coarser code-level
                    # Condition is suppressed so one diagnosis never becomes
                    # two Condition resources.
                    cond_rid, cond = _build_aktivitetsresultat_condition_dict(row_dict, resolved)
                    condition_dicts.append((cond_rid, cond))
                if not _has_value_bearing_xml_rows(xml_rows):
                    _append_condition_routing_decision(
                        routing_decision_sink,
                        row_dict,
                    )
                    continue

            route_decision = _resolve_row_target_resource(
                row_dict,
                routing_resolver,
                xml_rows,
                logger,
            )
            if is_procedure_row:
                route_decision = _decision_with_parallel_procedure(route_decision)
            elif is_condition_row:
                route_decision = _decision_with_parallel_condition(route_decision)

            _append_routing_decision(routing_decision_sink, row_dict, route_decision)

            if (
                is_procedure_row or is_condition_row
            ) and route_decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE:
                continue

            obs_dicts = _build_sfi_observations_for_row(
                row_dict=row_dict,
                xml_rows=xml_rows,
                resolved=resolved,
                routing_resolver=routing_resolver,
                logger=logger,
                route_decision=route_decision,
                numeric_values_only=is_procedure_row or is_condition_row,
            )
            udf_extensions = build_aktivitetsudfoerelse_extensions(
                row_dict, udf_lookup, speciale_lookup
            )
            for _, observation in obs_dicts:
                apply_aktivitetsudfoerelse_extensions(observation, udf_extensions)
            all_observations.extend(obs_dicts)
        except SfiDuplicateOccurrenceError:
            # Invariant violation — never swallow as a per-row error; surface it.
            raise
        except Exception as exc:
            errors += 1
            if logged_errors < _OBS_BUILD_STAGE.max_logged_errors:
                import traceback as _tb

                logger.warning(
                    "[SFI] Build error for OBJEKT_ID=%s: %s | type=%s | tb=%s",
                    row_dict.get("OBJEKT_ID"),
                    exc,
                    type(exc).__name__,
                    "".join(_tb.format_exception(type(exc), exc, exc.__traceback__)),
                )
                logged_errors += 1

    if procedure_dicts:
        logger.info(
            "Built %d Procedure dicts from AKTIVITETSRESULTAT rows with SKS procedure codes",
            len(procedure_dicts),
        )
    if condition_dicts:
        logger.info(
            "Built %d Condition dicts from AKTIVITETSRESULTAT rows with SKS diagnosis codes",
            len(condition_dicts),
        )

    _validate_observation_dicts_sample(all_observations, logger)
    _validate_procedure_dicts_sample(procedure_dicts, logger)
    _validate_condition_dicts_sample(condition_dicts, logger)

    return all_observations, procedure_dicts, condition_dicts, errors


def _build_sfi_unified_dicts_for_rows(
    rows: list[dict[str, Any]],
    parsed_data_by_index: list,
    kontekst_by_index: list[AktivitetskontekstRow | None],
    logger: Any,
    resolved: ResolvedRefLookup | None = None,
    routing_resolver: RoutingResolver | None = None,
    routing_decision_sink: list[dict[str, str | None]] | None = None,
    questionnaires_by_id: dict[str, dict] | None = None,
    dirty_questionnaire_ids: list[str] | None = None,
    sfi_catalog_sink: dict[str, list[SfiElement]] | None = None,
    udf_lookup: dict[str, dict[str, Any]] | None = None,
    speciale_lookup: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    int,
]:
    """Build Obs/Proc/Cond and Q/QR side outputs from one parsed SFI chunk."""
    routing_resolver = routing_resolver or _get_routing_resolver()
    questionnaires_by_id = questionnaires_by_id if questionnaires_by_id is not None else {}

    observation_dicts: list[tuple[str, dict]] = []
    procedure_dicts: list[tuple[str, dict]] = []
    condition_dicts: list[tuple[str, dict]] = []
    qr_dicts: list[tuple[str, dict]] = []
    errors = 0
    logged_errors = 0

    for idx, row_dict in enumerate(rows):
        try:
            xml_rows = parsed_data_by_index[idx]
            if (
                not isinstance(xml_rows, list)
                or not xml_rows
                or not isinstance(xml_rows[0], AktivitetsresultatRow)
            ):
                xml_rows = None

            if sfi_catalog_sink is not None and xml_rows is not None:
                model_name = row_dict.get("MODELLERINGSNAVN")
                model_version = row_dict.get("MODELLERINGSVERSION")
                mn = str(model_name) if model_name else None
                mv = str(model_version) if model_version else None
                dps = extract_sfi_datapoints(xml_rows, model_name=mn, model_version=mv)
                for dp in dps:
                    elem = sfi_datapoint_to_element(dp, model_name=mn)
                    add_sfi_catalog_element(sfi_catalog_sink, elem)

            aktivitetstype_kode = row_dict.get("AKTIVITETTYPE_KODE")
            is_procedure_row = is_sks_procedure_code(aktivitetstype_kode)
            is_condition_row = is_sks_diagnosis_code(aktivitetstype_kode)

            xml_condition_dicts: list[tuple[str, dict]] = []
            _append_xml_diagnosis_condition_dicts(
                row_dict,
                xml_rows,
                resolved,
                xml_condition_dicts,
            )
            condition_dicts.extend(xml_condition_dicts)

            if is_procedure_row:
                procedure_dicts.extend(
                    build_aktivitetsresultat_procedure_dicts(row_dict, xml_rows, resolved)
                )
                if not _has_value_bearing_xml_rows(xml_rows):
                    _append_procedure_routing_decision(
                        routing_decision_sink,
                        row_dict,
                    )
                    continue
            elif is_condition_row:
                if not xml_condition_dicts:
                    # Structured DiagnoseKode XML elements are the clinical
                    # source of truth; when present, the coarser code-level
                    # Condition is suppressed so one diagnosis never becomes
                    # two Condition resources.
                    cond_rid, cond = _build_aktivitetsresultat_condition_dict(
                        row_dict,
                        resolved,
                    )
                    condition_dicts.append((cond_rid, cond))
                if not _has_value_bearing_xml_rows(xml_rows):
                    _append_condition_routing_decision(
                        routing_decision_sink,
                        row_dict,
                    )
                    continue

            route_decision = _resolve_row_target_resource(
                row_dict,
                routing_resolver,
                xml_rows,
                logger,
            )
            if is_procedure_row:
                route_decision = _decision_with_parallel_procedure(route_decision)
            elif is_condition_row:
                route_decision = _decision_with_parallel_condition(route_decision)

            if route_decision.target_resource == TargetResource.OBSERVATION:
                row_observations = _build_sfi_observations_for_row(
                    row_dict=row_dict,
                    xml_rows=xml_rows,
                    resolved=resolved,
                    routing_resolver=routing_resolver,
                    logger=logger,
                    route_decision=route_decision,
                    numeric_values_only=is_procedure_row or is_condition_row,
                )
                numeric_values_only = is_procedure_row or is_condition_row
                if numeric_values_only and xml_rows is not None:
                    if not row_observations:
                        # The SKS row produced no numeric Observation, so preserve
                        # the whole row as a QuestionnaireResponse — nothing is lost.
                        if is_procedure_row:
                            qr_decision = _decision_with_parallel_procedure(
                                _procedure_observation_without_numeric_values_decision()
                            )
                        else:
                            qr_decision = _decision_with_parallel_condition(
                                _condition_observation_without_numeric_values_decision()
                            )
                        _append_routing_decision(
                            routing_decision_sink,
                            row_dict,
                            qr_decision,
                        )
                        _append_questionnaire_outputs_for_row(
                            row_dict=row_dict,
                            xml_rows=xml_rows,
                            kontekst=kontekst_by_index[idx],
                            resolved=resolved,
                            questionnaires_by_id=questionnaires_by_id,
                            dirty_questionnaire_ids=dirty_questionnaire_ids,
                            qr_dicts=qr_dicts,
                        )
                        continue

                    # Preserve rows that did not become numeric Observations.
                    qr_rows = [
                        row for row in xml_rows if not _would_emit_numeric_sfi_observation(row)
                    ]
                    if any(_is_value_carrying_row(row) for row in qr_rows):
                        _append_questionnaire_outputs_for_row(
                            row_dict=row_dict,
                            xml_rows=qr_rows,
                            kontekst=kontekst_by_index[idx],
                            resolved=resolved,
                            questionnaires_by_id=questionnaires_by_id,
                            dirty_questionnaire_ids=dirty_questionnaire_ids,
                            qr_dicts=qr_dicts,
                        )

                _append_routing_decision(routing_decision_sink, row_dict, route_decision)
                udf_extensions = build_aktivitetsudfoerelse_extensions(
                    row_dict, udf_lookup, speciale_lookup
                )
                for _, observation in row_observations:
                    apply_aktivitetsudfoerelse_extensions(observation, udf_extensions)
                observation_dicts.extend(row_observations)
            elif (
                route_decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
                and xml_rows is not None
            ):
                _append_routing_decision(routing_decision_sink, row_dict, route_decision)
                _append_questionnaire_outputs_for_row(
                    row_dict=row_dict,
                    xml_rows=xml_rows,
                    kontekst=kontekst_by_index[idx],
                    resolved=resolved,
                    questionnaires_by_id=questionnaires_by_id,
                    dirty_questionnaire_ids=dirty_questionnaire_ids,
                    qr_dicts=qr_dicts,
                )
            else:
                _append_routing_decision(routing_decision_sink, row_dict, route_decision)
        except SfiDuplicateOccurrenceError:
            # Invariant violation — never swallow as a per-row error; surface it.
            raise
        except Exception as exc:
            errors += 1
            if logged_errors < _OBS_BUILD_STAGE.max_logged_errors:
                import traceback as _tb

                logger.warning(
                    "[SFI] Unified build error for OBJEKT_ID=%s: %s | type=%s | tb=%s",
                    row_dict.get("OBJEKT_ID"),
                    exc,
                    type(exc).__name__,
                    "".join(_tb.format_exception(type(exc), exc, exc.__traceback__)),
                )
                logged_errors += 1

    if procedure_dicts:
        logger.info(
            "Built %d Procedure dicts from AKTIVITETSRESULTAT rows with SKS procedure codes",
            len(procedure_dicts),
        )
    if condition_dicts:
        logger.info(
            "Built %d Condition dicts from AKTIVITETSRESULTAT rows with SKS diagnosis codes",
            len(condition_dicts),
        )

    _validate_observation_dicts_sample(observation_dicts, logger)
    _validate_procedure_dicts_sample(procedure_dicts, logger)
    _validate_condition_dicts_sample(condition_dicts, logger)

    if resolved is not None:
        misses = resolved.miss_counts
        if any(v > 0 for v in misses.values()):
            logger.warning(
                "Reference cache misses (fallback to identifier-only ref): "
                "Patient=%d, Encounter=%d, EpisodeOfCare=%d -- "
                "these rows will project NULL in SoF flat views.",
                misses["Patient"],
                misses["Encounter"],
                misses["EpisodeOfCare"],
            )

    return observation_dicts, procedure_dicts, condition_dicts, qr_dicts, errors
