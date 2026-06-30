# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Procedure builders for AKTIVITETSRESULTAT rows with SKS procedure codes."""

import uuid
from typing import Any

from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from DSA.FHIR.Aktivitetsresultat.routing_glue import _audit_text
from DSA.FHIR.utils.code_normalization import normalize_sks_coding_code
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.routing import TargetResource
from DSA.FHIR.utils.ifdb.routing_fallback import RoutingDecision
from DSA.FHIR.utils.ifdb.transforms import (
    build_performer_dict,
)
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.transforms import (
    parse_clob_datetime,
)
from DSA.FHIR.utils.ifdb.transforms import source_deleted_to_int as _slettet_to_int
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META,
    EXTENSION_EPISODE_OF_CARE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension_dict


def _build_aktivitetsresultat_procedure_dict(
    row_dict: dict[str, Any],
    resolved: ResolvedRefLookup | None = None,
) -> tuple[str, dict]:
    """Build a Procedure dict from an AKTIVITETSRESULTAT row with SKS procedure code.

    Only called for rows where ``is_sks_procedure_code(AKTIVITETTYPE_KODE)``
    is True. The caller owns whether Procedure is emitted as a side projection
    or as an exclusive target.

    Returns ``(resource_id, procedure_dict)``.
    """
    objekt_id = row_dict.get("OBJEKT_ID")
    patient_id = row_dict.get("PATIENT_ID")
    encounter_id = row_dict.get("ENCOUNTER_ID")
    episode_id = row_dict.get("EPISODE_OF_CARE_ID")
    objekt_id_norm = _normalize_oracle_id(objekt_id)
    patient_id_norm = _normalize_oracle_id(patient_id)
    encounter_id_norm = _normalize_oracle_id(encounter_id)
    episode_id_norm = _normalize_oracle_id(episode_id)
    activity_type_code = row_dict.get("AKTIVITETTYPE_KODE")
    activity_type_text = row_dict.get("AKTIVITETTYPE_KODETEKST")
    period_start = row_dict.get("PERIOD_START")
    period_end = row_dict.get("PERIOD_END")
    slettet = row_dict.get("SLETTET")

    deleted_int = _slettet_to_int(slettet)
    is_deleted = deleted_int != 0
    status = "entered-in-error" if is_deleted else "completed"

    start_str: str | None = None
    end_str: str | None = None
    if period_start:
        try:
            if isinstance(period_start, str):
                parsed = parse_clob_datetime(period_start)
                start_str = (
                    str(fhir_datetime(parsed)) if parsed else str(fhir_datetime(period_start))
                )
            else:
                start_str = str(fhir_datetime(period_start))
        except Exception:
            pass
    if period_end:
        try:
            if isinstance(period_end, str):
                parsed_end = parse_clob_datetime(period_end)
                end_str = (
                    str(fhir_datetime(parsed_end)) if parsed_end else str(fhir_datetime(period_end))
                )
            else:
                end_str = str(fhir_datetime(period_end))
        except Exception:
            pass

    resource_id = resolved.procedure_id(objekt_id_norm) if resolved else str(uuid.uuid4())

    proc: dict[str, Any] = {"resourceType": "Procedure", "id": resource_id}

    if objekt_id_norm is not None:
        proc["identifier"] = [
            {
                "system": SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
                "value": objekt_id_norm,
            }
        ]

    proc["status"] = status

    code_dict: dict[str, Any] = {}
    if activity_type_code:
        coding_code = normalize_sks_coding_code(activity_type_code)
        if coding_code:
            coding: dict[str, str] = {"system": SYSTEM_SKS, "code": str(coding_code)}
            if activity_type_text:
                coding["display"] = str(activity_type_text)
            code_dict["coding"] = [coding]
        if activity_type_text:
            code_dict["text"] = str(activity_type_text)
    if code_dict:
        proc["code"] = code_dict

    if patient_id_norm is not None:
        if resolved:
            proc["subject"] = resolved.patient_ref(patient_id_norm)
        else:
            proc["subject"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_PATIENT_ID,
                    "value": patient_id_norm,
                }
            }

    if encounter_id_norm is not None:
        if resolved:
            proc["encounter"] = resolved.encounter_ref(encounter_id_norm)
        else:
            proc["encounter"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_OBJECT_ID,
                    "value": encounter_id_norm,
                }
            }

    if start_str and end_str:
        proc["performedPeriod"] = {"start": start_str, "end": end_str}
    elif start_str:
        proc["performedDateTime"] = start_str

    performer_entries: list[dict] = []
    performer_refs = build_performer_dict(row_dict)
    if performer_refs:
        performer_entries.extend([{"actor": ref} for ref in performer_refs])

    udfoert_sorcode = row_dict.get("UDFOERT_ADMENHED_SORCODE")
    if udfoert_sorcode is not None:
        sor_val = str(udfoert_sorcode).strip()
        if sor_val:
            performer_entries.append(
                {
                    "actor": {
                        "identifier": {"system": SYSTEM_SOR, "value": sor_val},
                    },
                }
            )

    if performer_entries:
        proc["performer"] = performer_entries

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

    source_parts: list[dict] = []

    def _add(key: str, val: Any) -> None:
        if val is None:
            return
        s = str(val).strip()
        if s:
            source_parts.append({"url": key, "valueString": s})

    _add("udfoertBrugerrolle", row_dict.get("UDFOERT_BRUGERROLLE"))
    _add("udfoertBrugerrolleNavn", row_dict.get("UDFOERT_BRUGERROLLE_NAME"))
    _add("udfoertAdmenhedKode", row_dict.get("UDFOERT_ADMENHED_KODE"))
    _add("udfoertAdmenhedNavn", row_dict.get("UDFOERT_ADMENHED_NAVN"))
    _add("udfoertAdmenhedOrgkode", row_dict.get("UDFOERT_ADMENHED_ORGKODE"))
    _add("udfoertAdmenhedSorcode", row_dict.get("UDFOERT_ADMENHED_SORCODE"))
    _add("modtidspunkt", row_dict.get("MODTIDSPUNKT"))

    if source_parts:
        extensions.append(
            {
                "url": EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META,
                "extension": source_parts,
            }
        )

    if extensions:
        proc["extension"] = extensions

    return resource_id, proc


def _append_procedure_routing_decision(
    sink: list[dict[str, str | None]] | None,
    row_dict: dict[str, Any],
    *,
    reason: str | None = None,
) -> None:
    if sink is None:
        return

    ifdb_aktivitet_id = _normalize_oracle_id(row_dict.get("OBJEKT_ID"))
    if not ifdb_aktivitet_id:
        return

    sink.append(
        {
            "ifdb_aktivitet_id": ifdb_aktivitet_id,
            "aktivitetstype_kode": _audit_text(row_dict.get("AKTIVITETTYPE_KODE")),
            "target_resource": "Procedure",
            "observation_category": None,
            "routing_source": "procedure_code",
            "routing_confidence": "high",
            "routing_reason": (
                reason
                or "AKTIVITETTYPE_KODE matched IFDB Procedure code rules; "
                "Procedure emitted, and no value-bearing XML was routed to "
                "QuestionnaireResponse/Observation."
            ),
        }
    )


def _decision_with_parallel_procedure(decision: RoutingDecision) -> RoutingDecision:
    """Preserve the XML route while making the audit reason explicit."""
    return RoutingDecision(
        target_resource=decision.target_resource,
        observation_category=decision.observation_category,
        routing_source=decision.routing_source,
        routing_confidence=decision.routing_confidence,
        routing_reason=(
            f"{decision.routing_reason} Procedure emitted in parallel because "
            "AKTIVITETTYPE_KODE matched IFDB Procedure code rules; XML values "
            f"preserved as {decision.target_resource.value}."
        ),
    )


def _procedure_observation_without_numeric_values_decision() -> RoutingDecision:
    return RoutingDecision(
        target_resource=TargetResource.QUESTIONNAIRE_RESPONSE,
        observation_category=None,
        routing_source="xml_fallback",
        routing_confidence="low",
        routing_reason=(
            "Matched Observation route for a procedure-coded measurement, but "
            "no numeric SFI datapoints were emitted; defaulted XML preservation "
            "to QuestionnaireResponse."
        ),
    )
