# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Transform, post, and chunk orchestration for AKTIVITETSRESULTAT."""

from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any

import dagster as dg
from core.fhir.child_resource_reconcile import ChildFamily
from core.parallel import ParallelCall, run_parallel_calls
from core.resources import FHIRDatabaseResource, IFDBOracleResource, MSSQLResource
from DSA.CodeSystem.sfi.catalog import (
    SfiElement,
    merge_sfi_catalog_from_datapoints,
    post_sfi_codesystem_to_aidbox,
)
from DSA.FHIR.Aktivitetsresultat.activity_metadata import (
    _fetch_aktivitetsudfoerelse_data_arc,
    _fetch_aktivitetsudfoerelse_speciale_data_arc,
    apply_aktivitetsudfoerelse_extensions,
    build_aktivitetsudfoerelse_extensions,
)
from DSA.FHIR.Aktivitetsresultat.questionnaire_outputs import (
    _metadata_int,
    _post_dirty_questionnaires,
    _sfi_side_output_result,
)
from DSA.FHIR.Aktivitetsresultat.references import (
    ResolvedRefLookup,
    _ensure_reference_dependencies,
    _pre_resolve_references,
    resolve_procedure_organizations,
)
from DSA.FHIR.Aktivitetsresultat.routing_glue import _get_routing_resolver, _sfi_mode_enabled
from DSA.FHIR.Condition.condition_aktivitetsresultat_builder import (
    _append_xml_diagnosis_condition_dicts,
    _build_aktivitetsresultat_condition_dict,
)
from DSA.FHIR.Observation.observation_dict_builder import (
    _build_observation_from_row_dict,
    _build_observation_lossless_dict,
)
from DSA.FHIR.Observation.observation_sfi_builder import (
    _build_sfi_observation_dicts_for_rows,
    _build_sfi_unified_dicts_for_rows,
    _log_sfi_routing_summary,
)
from DSA.FHIR.Procedure.procedure_aktivitetsresultat_builder import (
    _extract_xml_procedure_payload,
    build_aktivitetsresultat_procedure_dicts,
)
from DSA.FHIR.utils.code_normalization import (
    is_sks_diagnosis_code,
    is_sks_procedure_code,
)
from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import (
    _AKTIVITETSRESULTAT_CHUNK_SIZE,
    _FAST_MODE,
    _OBS_BUILD_STAGE,
)
from DSA.FHIR.utils.ifdb.aktivitetsresultat_identifiers import _sfi_identifier_value
from DSA.FHIR.utils.ifdb.dict_validation import (
    _validate_condition_dicts_sample,
    _validate_observation_dicts_sample,
    _validate_procedure_dicts_sample,
)
from DSA.FHIR.utils.ifdb.posting_config import aktivitetsresultat_chunk_size_from_context
from DSA.FHIR.utils.ifdb.routing import RoutingResolver
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    AktivitetsresultatRow,
)
from DSA.FHIR.utils.ifdb.xml_child_reconcile import (
    FAMILY_DELOP,
    FAMILY_SFI_OBS,
    FAMILY_XML_CONDITION,
    reconcile_aktivitetsresultat_xml_children,
)
from DSA.FHIR.utils.ifdb.xml_parsing_workers import (
    _parse_xml_components_for_rows,
    _parse_xml_lossless_for_rows,
)


def _collect_udf_ids(rows: list[dict[str, Any]]) -> list[str]:
    udf_ids: set[str] = set()
    for row in rows:
        udf_id = _normalize_oracle_id(row.get("UDFOERELSE_ID"))
        if udf_id is not None:
            udf_ids.add(udf_id)
    return sorted(udf_ids)


def _collect_chunk_sfi_identifiers(
    rows: list[dict[str, Any]],
    parsed_data_by_index: list[list[AktivitetsresultatRow] | None],
) -> list[str]:
    sfi_identifiers: set[str] = set()
    for row_dict, parsed_rows in zip(rows, parsed_data_by_index, strict=True):
        objekt_id = _normalize_oracle_id(row_dict.get("OBJEKT_ID"))
        if objekt_id is None or not parsed_rows:
            continue
        for parsed_row in parsed_rows:
            if not isinstance(parsed_row, AktivitetsresultatRow):
                continue
            if parsed_row.element_nummer is None:
                continue
            if parsed_row.xml_status == -1:
                continue
            if parsed_row.raekke_type not in {"Element", "Selektion"}:
                continue
            sfi_identifiers.add(_sfi_identifier_value(objekt_id, parsed_row.element_nummer))
    return sorted(sfi_identifiers)


def _collect_chunk_xml_condition_identifiers(
    rows: list[dict[str, Any]],
    parsed_data_by_index: list[list[AktivitetsresultatRow] | None],
) -> list[str]:
    identifiers: set[str] = set()
    for row_dict, parsed_rows in zip(rows, parsed_data_by_index, strict=True):
        objekt_id = _normalize_oracle_id(row_dict.get("OBJEKT_ID"))
        if objekt_id is None or not parsed_rows:
            continue
        for parsed_row in parsed_rows:
            if not isinstance(parsed_row, AktivitetsresultatRow):
                continue
            if parsed_row.xml_status == -1:
                continue
            if parsed_row.raekke_type != "Element":
                continue
            if parsed_row.element_nummer is None:
                continue
            identifiers.add(FAMILY_XML_CONDITION.child_value(objekt_id, parsed_row.element_nummer))
    return sorted(identifiers)


def _collect_chunk_delop_procedure_identifiers(
    rows: list[dict[str, Any]],
    parsed_data_by_index: list[list[AktivitetsresultatRow] | None],
) -> list[str]:
    identifiers: set[str] = set()
    for row_dict, parsed_rows in zip(rows, parsed_data_by_index, strict=True):
        objekt_id = _normalize_oracle_id(row_dict.get("OBJEKT_ID"))
        if objekt_id is None:
            continue
        payload = _extract_xml_procedure_payload(parsed_rows)
        if not payload:
            continue
        for seq, _delop in enumerate(payload.delops, start=1):
            identifiers.add(FAMILY_DELOP.child_value(objekt_id, seq))
    return sorted(identifiers)


def _reconcile_posted_xml_children(
    *,
    fhir_database: FHIRDatabaseResource,
    rows: list[dict[str, Any]],
    parsed_data_by_index: list[list[AktivitetsresultatRow] | None] | None,
    observation_dicts: list[tuple[str, dict]],
    procedure_dicts: list[tuple[str, dict]],
    condition_dicts: list[tuple[str, dict]],
    build_errors: int,
    logger: Any,
) -> None:
    if parsed_data_by_index is None:
        # Bevidst tavs på stier uden XML-parse (ikke-SFI): uden parse findes
        # ingen forventet mængde, og en reconcile her ville pensionere ALLE
        # de berørte forældres børn. None er værnet, ikke en forglemmelse.
        return
    reconcile_aktivitetsresultat_xml_children(
        fhir_database=fhir_database,
        rows=rows,
        parsed_data_by_index=parsed_data_by_index,
        observation_dicts=observation_dicts,
        procedure_dicts=procedure_dicts,
        condition_dicts=condition_dicts,
        build_errors=build_errors,
        logger=logger,
    )


def _hydrate_lookups_for_rows(
    *,
    ifdb_oracle: Any,
    context: Any,
    rows: list[dict[str, Any]],
    udf_lookup: dict[str, dict[str, Any]],
    speciale_lookup: dict[str, list[dict[str, Any]]],
    mssql_target_staging: MSSQLResource | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Fetch partition-shared UDF and speciale lookups for missing identifiers.

    Lookups read ``arc.*`` via MSSQL; without ``mssql_target_staging`` there is
    nothing to hydrate from. (``ifdb_oracle`` stays in the signature because the
    orchestrator's other dependency paths still take it.)
    """
    udf_ids = _collect_udf_ids(rows)
    if not udf_ids or mssql_target_staging is None:
        return udf_lookup, speciale_lookup

    missing_udf_ids = [udf_id for udf_id in udf_ids if udf_id not in udf_lookup]
    if missing_udf_ids:
        udf_lookup.update(
            _fetch_aktivitetsudfoerelse_data_arc(
                missing_udf_ids,
                mssql_target_staging,
                context,
            )
        )

    missing_speciale_lookup_ids = [udf_id for udf_id in udf_ids if udf_id not in speciale_lookup]
    if missing_speciale_lookup_ids:
        speciale_lookup.update(
            _fetch_aktivitetsudfoerelse_speciale_data_arc(
                missing_speciale_lookup_ids,
                mssql_target_staging,
                context,
            )
        )

    return udf_lookup, speciale_lookup


@dataclass(frozen=True)
class ChunkIdSpec:
    """Chunk-opslaget for én barnefamilie: hvad der samles, og hvor det flettes ind.

    Familien ejer ressourcetype, identifier-system og barne-id-formatet
    (``ChildFamily.child_value``); ``collect`` ejer, hvilke XML-elementer i
    chunken der bliver til børn. Det er hele forskellen mellem familierne —
    selve opslaget er ens og ligger ét sted, i ``_pre_resolve_chunk_ids``.
    """

    family: ChildFamily
    collect: Callable[
        [list[dict[str, Any]], list[list[AktivitetsresultatRow] | None]],
        list[str],
    ]
    label: str


_SFI_OBS_ID_SPEC = ChunkIdSpec(FAMILY_SFI_OBS, _collect_chunk_sfi_identifiers, "SFI")
_XML_CONDITION_ID_SPEC = ChunkIdSpec(
    FAMILY_XML_CONDITION, _collect_chunk_xml_condition_identifiers, "XMLCondition"
)
_DELOP_ID_SPEC = ChunkIdSpec(FAMILY_DELOP, _collect_chunk_delop_procedure_identifiers, "Delop")

# Rækkefølgen er den hidtidige: SFI-observationer, XML-diagnoser, deloperationer.
_CHUNK_ID_SPECS: tuple[ChunkIdSpec, ...] = (
    _SFI_OBS_ID_SPEC,
    _XML_CONDITION_ID_SPEC,
    _DELOP_ID_SPEC,
)


def _pre_resolve_chunk_ids(
    spec: ChunkIdSpec,
    *,
    fhir_database: FHIRDatabaseResource,
    resolved: ResolvedRefLookup,
    rows: list[dict[str, Any]],
    parsed_rows: list[list[AktivitetsresultatRow] | None],
    logger: Any,
) -> None:
    """Slå chunkens eksisterende børn op for én familie og flet dem ind i opslaget.

    Kun værdier, partitionens opslag ikke allerede kender, spørges der efter i
    databasen. Uden opslaget ville byggeren minte et nyt id til et barn, der
    allerede findes, og upserten ramme identifier-indekset — dét er den
    manglende pre-resolve, som recovery-tælleren i materialiserings-metadata
    afslører.
    """
    identifiers = spec.collect(rows, parsed_rows)
    if not identifiers:
        return

    family = spec.family
    lookup = resolved.lookups_by_resource_type()[family.resource_type]
    missing = [
        value for value in identifiers if lookup.get((family.identifier_system, value)) is None
    ]
    if not missing:
        return

    start = perf_counter()
    resolved_values = fhir_database.resolve_identifier_values(
        resource_type=family.resource_type,
        identifier_system=family.identifier_system,
        values=missing,
    )
    added = resolved.merge_identifiers(
        family.resource_type,
        family.identifier_system,
        resolved_values,
    )
    logger.info(
        "[%s] chunk-scope %s lookup: requested=%d resolved=%d added_to_cache=%d elapsed=%.2fs",
        spec.label,
        family.resource_type,
        len(missing),
        len(resolved_values),
        added,
        perf_counter() - start,
    )


def _pre_resolve_chunk_families(
    *,
    fhir_database: FHIRDatabaseResource,
    resolved: ResolvedRefLookup,
    rows: list[dict[str, Any]],
    parsed_rows: list[list[AktivitetsresultatRow] | None],
    logger: Any,
) -> None:
    """Alle barnefamilier — samme opslag i samme rækkefølge, uanset sti."""
    for spec in _CHUNK_ID_SPECS:
        _pre_resolve_chunk_ids(
            spec,
            fhir_database=fhir_database,
            resolved=resolved,
            rows=rows,
            parsed_rows=parsed_rows,
            logger=logger,
        )


def _dedupe_chunk_families(
    obs_dicts: list[tuple[str, dict]],
    proc_dicts: list[tuple[str, dict]],
    cond_dicts: list[tuple[str, dict]],
    qr_dicts: list[tuple[str, dict]],
    logger: Any,
) -> tuple[
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    dict[str, int],
]:
    """Dubletpolitikken for een chunk — ens for begge stier, med synlig taeller.

    Forretnings-id-dedupe FOER post er den eneste haandtering af dubletter i
    chunken (upserten haandterer id-kollisioner); tallene returneres, saa de
    kan surfaces i asset-metadata i stedet for kun at staa i loggen.
    """
    duplicate_counts: dict[str, int] = {}
    obs_dicts, duplicate_counts["Observation"] = _dedupe_resource_dicts_by_business_id(
        obs_dicts, logger=logger, resource_type="Observation"
    )
    proc_dicts, duplicate_counts["Procedure"] = _dedupe_resource_dicts_by_business_id(
        proc_dicts, logger=logger, resource_type="Procedure"
    )
    cond_dicts, duplicate_counts["Condition"] = _dedupe_resource_dicts_by_business_id(
        cond_dicts, logger=logger, resource_type="Condition"
    )
    if qr_dicts:
        qr_dicts, duplicate_counts["QuestionnaireResponse"] = _dedupe_resource_dicts_by_business_id(
            qr_dicts, logger=logger, resource_type="QuestionnaireResponse"
        )
    return obs_dicts, proc_dicts, cond_dicts, qr_dicts, duplicate_counts


def _lookup_key_for_resource_dict(resource: dict[str, Any]) -> tuple[Any, ...] | None:
    identifiers = resource.get("identifier") or []
    if not isinstance(identifiers, list):
        return None

    ifdb_pairs: set[tuple[str, str]] = set()
    for ident in identifiers:
        if not isinstance(ident, dict):
            continue
        system = ident.get("system")
        value = ident.get("value")
        if not system or value is None:
            continue
        system_str = str(system)
        if "ifdb" in system_str.lower() or "questionnaire" in system_str.lower():
            ifdb_pairs.add((system_str, str(value)))

    if not ifdb_pairs:
        return None
    return ("ifdb_identifier",) + tuple(sorted(ifdb_pairs))


def _dedupe_resource_dicts_by_business_id(
    resources: list[tuple[str, dict]],
    logger: Any,
    resource_type: str,
) -> tuple[list[tuple[str, dict]], int]:
    deduped: list[tuple[str, dict]] = []
    seen: dict[tuple[Any, ...] | str, tuple[str, dict]] = {}
    duplicates = 0

    for rid, resource in resources:
        key = _lookup_key_for_resource_dict(resource)
        if key is None:
            dedupe_key: tuple[Any, ...] | str = f"rid:{rid}"
        else:
            dedupe_key = key

        if dedupe_key in seen:
            duplicates += 1
            continue
        seen[dedupe_key] = (rid, resource)
        deduped.append((rid, resource))

    if duplicates:
        logger.info(
            "[Dedupe] %s: removed %d duplicate resource dictionaries by business identifier in chunk",
            resource_type,
            duplicates,
        )
    return deduped, duplicates


def _build_observation_dicts_for_rows(
    rows: list[dict[str, Any]],
    parsed_data_by_index: list,
    logger: Any,
    kontekst_by_index: list[AktivitetskontekstRow | None] | None = None,
    resolved: ResolvedRefLookup | None = None,
    udf_lookup: dict[str, dict[str, Any]] | None = None,
    speciale_lookup: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[list[tuple[str, dict]], list[tuple[str, dict]], list[tuple[str, dict]], int]:
    """Build Observation, Procedure and Condition dicts for all rows with deterministic ordering.

    Returns ``(observation_dicts, procedure_dicts, condition_dicts, error_count)``.

    Rows where ``AKTIVITETTYPE_KODE`` is an SKS procedure or diagnosis code
    additionally produce a Procedure or Condition dict. The Observation is
    always emitted (lossless).
    """
    observation_dicts: list[tuple[str, dict]] = []
    procedure_dicts: list[tuple[str, dict]] = []
    condition_dicts: list[tuple[str, dict]] = []
    errors = 0
    logged = 0

    for idx, row_dict in enumerate(rows):
        try:
            k = kontekst_by_index[idx] if kontekst_by_index else None
            pd = parsed_data_by_index[idx]
            if _FAST_MODE:
                rid, obs = _build_observation_from_row_dict(row_dict, pd, resolved)
            else:
                rid, obs = _build_observation_lossless_dict(row_dict, pd, k, resolved)

            apply_aktivitetsudfoerelse_extensions(
                obs,
                build_aktivitetsudfoerelse_extensions(row_dict, udf_lookup, speciale_lookup),
            )

            observation_dicts.append((rid, obs))

            aktivitetstype_kode = row_dict.get("AKTIVITETTYPE_KODE")

            xml_condition_dicts: list[tuple[str, dict]] = []
            if (
                not _FAST_MODE
                and isinstance(pd, list)
                and pd
                and isinstance(pd[0], AktivitetsresultatRow)
            ):
                _append_xml_diagnosis_condition_dicts(row_dict, pd, resolved, xml_condition_dicts)

            if is_sks_procedure_code(aktivitetstype_kode):
                xml_rows_for_proc = (
                    pd
                    if (
                        not _FAST_MODE
                        and isinstance(pd, list)
                        and pd
                        and isinstance(pd[0], AktivitetsresultatRow)
                    )
                    else None
                )
                procedure_dicts.extend(
                    build_aktivitetsresultat_procedure_dicts(row_dict, xml_rows_for_proc, resolved)
                )
            elif is_sks_diagnosis_code(aktivitetstype_kode) and not xml_condition_dicts:
                # Structured DiagnoseKode XML elements are the clinical source of
                # truth. When a row emits XML-level Conditions, the coarser
                # code-level Condition for the same OBJEKT_ID is suppressed so one
                # diagnosis never becomes two Condition resources.
                cond_rid, cond = _build_aktivitetsresultat_condition_dict(row_dict, resolved)
                condition_dicts.append((cond_rid, cond))

            condition_dicts.extend(xml_condition_dicts)
        except Exception as exc:
            errors += 1
            if logged < _OBS_BUILD_STAGE.max_logged_errors:
                import traceback as _tb

                logger.warning(
                    "Dict build error for OBJEKT_ID=%s: %s | type=%s | tb=%s",
                    row_dict.get("OBJEKT_ID"),
                    exc,
                    type(exc).__name__,
                    "".join(_tb.format_exception(type(exc), exc, exc.__traceback__)),
                )
                logged += 1

    if errors:
        logger.warning("Dict build failed for %d / %d rows — skipped", errors, len(rows))

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

    return observation_dicts, procedure_dicts, condition_dicts, errors


def _transform_aktivitetsresultat_to_fhir_dicts(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    resolved: ResolvedRefLookup | None = None,
    udf_lookup: dict[str, dict[str, Any]] | None = None,
    speciale_lookup: dict[str, list[dict[str, Any]]] | None = None,
    fhir_database: FHIRDatabaseResource | None = None,
) -> tuple[list[tuple[str, dict]], list[tuple[str, dict]], list[tuple[str, dict]]]:
    """Legacy-transformen (SFI-mode slået fra): rækker til Observation + Procedure/Condition.

    Returns ``(observation_dicts, procedure_dicts, condition_dicts)``.

    Rows where ``AKTIVITETTYPE_KODE`` is an SKS procedure or diagnosis code
    additionally produce a Procedure or Condition dict alongside the Observation.

    SFI-sporene bygges ikke her, men i ``_transform_chunk``. Et tidligere
    SFI-spor i denne funktion var uden kaldere fra orkestreringen og er
    fjernet, så barnefamiliernes opslag kun findes ét sted.
    """
    logger = dg.get_dagster_logger()

    if _FAST_MODE:
        logger.info("AKTIVITETSRESULTAT dict-pipeline mode: FAST (Navn/Vaerdi projection)")
        parsed_components, _ = _parse_xml_components_for_rows(rows, logger)
        obs_dicts, proc_dicts, cond_dicts, _ = _build_observation_dicts_for_rows(
            rows=rows,
            parsed_data_by_index=parsed_components,
            logger=logger,
            resolved=resolved,
            udf_lookup=udf_lookup,
            speciale_lookup=speciale_lookup,
        )
    else:
        logger.info("AKTIVITETSRESULTAT dict-pipeline mode: LOSSLESS (all C# fields)")
        parsed_rows, kontekst_rows, _ = _parse_xml_lossless_for_rows(rows, logger)
        if resolved is not None and fhir_database is not None:
            _pre_resolve_chunk_ids(
                _DELOP_ID_SPEC,
                fhir_database=fhir_database,
                resolved=resolved,
                rows=rows,
                parsed_rows=parsed_rows,
                logger=logger,
            )
        obs_dicts, proc_dicts, cond_dicts, _ = _build_observation_dicts_for_rows(
            rows=rows,
            parsed_data_by_index=parsed_rows,
            logger=logger,
            kontekst_by_index=kontekst_rows,
            resolved=resolved,
            udf_lookup=udf_lookup,
            speciale_lookup=speciale_lookup,
        )

    if resolved is not None:
        misses = resolved.miss_counts
        if any(v > 0 for v in misses.values()):
            logger.warning(
                "Reference cache misses (fallback to identifier-only ref): "
                "Patient=%d, Encounter=%d, EpisodeOfCare=%d — "
                "these rows will project NULL in SoF flat views.",
                misses["Patient"],
                misses["Encounter"],
                misses["EpisodeOfCare"],
            )

    return obs_dicts, proc_dicts, cond_dicts


# Chunk-posten: højst én tråd pr. ressourcefamilie (Observation, Procedure,
# Condition). Cache-politikken står ved kaldet — invalidate_cache=False, fordi
# partitionens id'er er låst via de forvarmede opslag, og én samlet rydning
# sker i pipelinens finally.
_POST_FAMILY_MAX_WORKERS = 3
_CHUNK_FAMILIES: tuple[str, ...] = ("Observation", "Procedure", "Condition")


@dataclass(frozen=True)
class _PostCounts:
    """Postede, fejlede og genoprettede rækker for ét post-kald — eller summen af flere."""

    posted: int = 0
    failed: int = 0
    recovered: int = 0

    def __add__(self, other: "_PostCounts") -> "_PostCounts":
        return _PostCounts(
            self.posted + other.posted,
            self.failed + other.failed,
            self.recovered + other.recovered,
        )

    @classmethod
    def from_result(cls, result: dg.MaterializeResult | None) -> "_PostCounts":
        if result is None:
            return cls()
        return cls(
            posted=_metadata_int(result, "posted_count"),
            failed=_metadata_int(result, "failed_count"),
            recovered=_metadata_int(result, "unique_violation_recovered_count"),
        )


@dataclass(frozen=True)
class _ChunkTally:
    """Det, én chunk efterlader til kaldestedets sammenlægning — og intet andet."""

    families: dict[str, _PostCounts]
    duplicates_removed: int
    questionnaire_ids: list[str]
    questionnaire_failed: int
    questionnaire_recovered: int
    questionnaire_responses: _PostCounts


def _transform_chunk(
    *,
    chunk: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    resolved: ResolvedRefLookup,
    udf_lookup: dict[str, dict[str, Any]],
    speciale_lookup: dict[str, list[dict[str, Any]]],
    routing_resolver: RoutingResolver | None,
    routing_decisions: list[dict[str, str | None]],
    sfi_catalog_sink: dict[str, list[SfiElement]] | None,
    questionnaires_by_id: dict[str, dict],
    dirty_questionnaire_ids: list[str],
    unified_sfi_outputs: bool,
    logger: Any,
) -> tuple[
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    list[tuple[str, dict]],
    list[list[AktivitetsresultatRow] | None] | None,
    int,
]:
    """Byg chunkens dicts ad den sti, kørselstilstanden vælger.

    Returnerer ``(observationer, procedurer, conditions, qr'er, parsede rækker,
    byggefejl)``. De parsede rækker er ``None`` på legacy-sporet — dér findes
    ingen forventet børnemængde, og reconcile skal derfor ikke køre (se
    ``_reconcile_posted_xml_children``).

    Sporene:
    - SFI med skema-output (produktionskaldet): lossless parse, opslag af
      alle barnefamilier, samlet bygning af Observation/Procedure/Condition/QR.
    - SFI uden skema-output: samme parse og opslag, kun de tre familier.
    - Legacy: den hidtidige transform (FAST eller LOSSLESS).
    """
    if _sfi_mode_enabled():
        parsed_rows, kontekst_rows, _ = _parse_xml_lossless_for_rows(chunk, logger)
        _pre_resolve_chunk_families(
            fhir_database=fhir_database,
            resolved=resolved,
            rows=chunk,
            parsed_rows=parsed_rows,
            logger=logger,
        )
        if unified_sfi_outputs:
            obs_dicts, proc_dicts, cond_dicts, qr_dicts, build_errors = (
                _build_sfi_unified_dicts_for_rows(
                    rows=chunk,
                    parsed_data_by_index=parsed_rows,
                    kontekst_by_index=kontekst_rows,
                    logger=logger,
                    resolved=resolved,
                    routing_resolver=routing_resolver,
                    routing_decision_sink=routing_decisions,
                    questionnaires_by_id=questionnaires_by_id,
                    dirty_questionnaire_ids=dirty_questionnaire_ids,
                    sfi_catalog_sink=sfi_catalog_sink,
                    udf_lookup=udf_lookup,
                    speciale_lookup=speciale_lookup,
                )
            )
            return obs_dicts, proc_dicts, cond_dicts, qr_dicts, parsed_rows, build_errors

        obs_dicts, proc_dicts, cond_dicts, build_errors = _build_sfi_observation_dicts_for_rows(
            rows=chunk,
            parsed_data_by_index=parsed_rows,
            logger=logger,
            resolved=resolved,
            routing_resolver=routing_resolver,
            routing_decision_sink=routing_decisions,
            sfi_catalog_sink=sfi_catalog_sink,
            udf_lookup=udf_lookup,
            speciale_lookup=speciale_lookup,
        )
        return obs_dicts, proc_dicts, cond_dicts, [], parsed_rows, build_errors

    obs_dicts, proc_dicts, cond_dicts = _transform_aktivitetsresultat_to_fhir_dicts(
        chunk,
        context,
        resolved=resolved,
        udf_lookup=udf_lookup,
        speciale_lookup=speciale_lookup,
        fhir_database=fhir_database,
    )
    return obs_dicts, proc_dicts, cond_dicts, [], None, 0


def _process_chunk(
    *,
    chunk: list[dict[str, Any]],
    chunk_idx: int,
    n_chunks: int,
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource | None,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource | None,
    resolved: ResolvedRefLookup,
    routing_resolver: RoutingResolver | None,
    sfi_catalog_sink: dict[str, list[SfiElement]] | None,
    questionnaires_by_id: dict[str, dict],
    unified_sfi_outputs: bool,
    logger: Any,
) -> _ChunkTally:
    """Én chunk fra rækker til postede ressourcer — den ENESTE sti gennem chunk-forløbet.

    Kaldestedet kender kun løkken og sammenlægningen; alt, hvad der sker med
    en chunk, sker her og i samme rækkefølge, uanset om kørslen har én eller
    tusind chunks: opslag → parse og byg → dubletfrasortering → post →
    oprydning af forsvundne XML-børn → skema-sideudgange → frigivelse af
    chunkens id'er.
    """
    from DSA.FHIR.utils.ifdb.fhir_posting import post_resource_dicts_direct

    logger.info("Chunk %d/%d: transforming %d rows", chunk_idx, n_chunks, len(chunk))
    fhir_database.log_identifier_lookup_status(
        f"pre-chunk-{chunk_idx}",
        resolved.lookups_by_resource_type(),
    )
    fhir_database.suspend_identifier_caches()

    routing_decisions: list[dict[str, str | None]] = []
    dirty_questionnaire_ids: list[str] = []
    # UDF/speciale-opslagene er chunk-lokale: hver række slår kun sin egen
    # udførelse op, og udførelserne i én chunk går stort set aldrig igen i
    # de næste. Deles opslagene på tværs af chunks, vokser de med én
    # arkivrække pr. udførelse i hele perioden (millioner ved backfill),
    # og barneprocessen dræbes. Start derfor fra tomme opslag pr. chunk.
    udf_lookup, speciale_lookup = _hydrate_lookups_for_rows(
        ifdb_oracle=ifdb_oracle,
        context=context,
        rows=chunk,
        udf_lookup={},
        speciale_lookup={},
        mssql_target_staging=mssql_target_staging,
    )
    obs_dicts, proc_dicts, cond_dicts, qr_dicts, parsed_rows, build_errors = _transform_chunk(
        chunk=chunk,
        context=context,
        fhir_database=fhir_database,
        resolved=resolved,
        udf_lookup=udf_lookup,
        speciale_lookup=speciale_lookup,
        routing_resolver=routing_resolver,
        routing_decisions=routing_decisions,
        sfi_catalog_sink=sfi_catalog_sink,
        questionnaires_by_id=questionnaires_by_id,
        dirty_questionnaire_ids=dirty_questionnaire_ids,
        unified_sfi_outputs=unified_sfi_outputs,
        logger=logger,
    )
    obs_dicts, proc_dicts, cond_dicts, qr_dicts, dup_counts = _dedupe_chunk_families(
        obs_dicts, proc_dicts, cond_dicts, qr_dicts, logger
    )
    _log_sfi_routing_summary(
        logger,
        routing_decisions,
        label=f"chunk {chunk_idx}/{n_chunks}",
    )

    families = _post_chunk_families(
        context=context,
        observation_dicts=obs_dicts,
        procedure_dicts=proc_dicts,
        condition_dicts=cond_dicts,
        fhir_database=fhir_database,
        extracted_count=len(chunk),
        stage_id=f"ifdb.observation.post.chunk.{chunk_idx}",
        thread_name_prefix=f"aktres-post-c{chunk_idx}",
        max_workers=_POST_FAMILY_MAX_WORKERS,
        invalidate_cache=False,
    )
    _reconcile_posted_xml_children(
        fhir_database=fhir_database,
        rows=chunk,
        parsed_data_by_index=parsed_rows,
        observation_dicts=obs_dicts,
        procedure_dicts=proc_dicts,
        condition_dicts=cond_dicts,
        build_errors=build_errors,
        logger=logger,
    )

    questionnaire_ids: list[str] = []
    questionnaire_failed = 0
    questionnaire_recovered = 0
    qr_counts = _PostCounts()
    if unified_sfi_outputs:
        _, questionnaire_failed, questionnaire_ids, questionnaire_recovered = (
            _post_dirty_questionnaires(
                context=context,
                questionnaires_by_id=questionnaires_by_id,
                dirty_questionnaire_ids=dirty_questionnaire_ids,
                fhir_database=fhir_database,
            )
        )
        if qr_dicts:
            qr_counts = _PostCounts.from_result(
                post_resource_dicts_direct(
                    context=context,
                    resource_dicts=qr_dicts,
                    fhir_database=fhir_database,
                    resource_type="QuestionnaireResponse",
                    source_name="ifdb_oracle",
                    extracted_count=len(qr_dicts),
                    invalidate_cache=False,
                )
            )

    # Chunken er postet — hverken de mintede id'er (nye rækker) eller de
    # flettede opslag (genpostede rækker) behøves længere: kildenøglen er
    # unik pr. række, og næste chunk slår sine identifikatorer op selv.
    released_ids = resolved.release_chunk_scoped_ids()
    if released_ids:
        logger.info(
            "Chunk %d/%d: frigav %d chunk-lokale id-poster",
            chunk_idx,
            n_chunks,
            released_ids,
        )

    return _ChunkTally(
        families=families,
        duplicates_removed=sum(dup_counts.values()),
        questionnaire_ids=questionnaire_ids,
        questionnaire_failed=questionnaire_failed,
        questionnaire_recovered=questionnaire_recovered,
        questionnaire_responses=qr_counts,
    )


def _run_aktivitetsresultat_pipeline_chunked(
    *,
    extracted_data: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource | None,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource | None = None,
    emit_questionnaire_outputs: bool = False,
) -> dg.MaterializeResult | tuple[dg.MaterializeResult, dg.MaterializeResult, dg.MaterializeResult]:
    """Transform and post AKTIVITETSRESULTAT Observations + Procedures for a
    single partition using chunked streaming.

    Én sti: hver chunk går gennem ``_process_chunk`` — også når partitionen
    fylder én chunk. Denne funktion kender kun opsætningen før løkken, løkken
    selv og sammenlægningen af tællerne bagefter.

    Memory semantics:
      - One-time setup outside the chunk loop: dependency resolution + cache
        warmup. These must happen once per partition regardless of chunking.
      - Per-chunk: hydrate UDF/speciale lookups for the chunk's rows, parse
        XML, build dicts, post. Chunk-local state (incl. the UDF/speciale
        lookups) is freed when the next iteration starts, so peak memory is
        bounded by the chunk size, not the partition size.

    Output semantics:
      - IDENTICAL to the pre-chunk monolithic path (modulo random uuids on
        new resources). This is validated by
        ``tests/integration/test_aktivitetsresultat_chunked_parity.py``
        which must PASS before any change to this function is deployed.
      - Specifically: ResolvedRefLookup is built once and shared across all
        chunks, so reference lookups produce the same FHIR UUID for a given
        IFDB identifier regardless of which chunk the row lands in.
      - Output dicts are concatenated in the same order as the input rows.
      - Materialiserings-metadata har samme form uanset chunk-antal: postede
        og fejlede pr. familie, dubletter, recovery-tæller og chunk-tal.

    Error semantics:
      - A build/post failure in chunk N means chunks 0..N-1 are already
        persisted in FHIR. This matches the pre-chunk behaviour at the
        batch-upsert level (batches are also not atomic). FHIR UUIDs are
        derived from IFDB identifiers via the pre-loaded cache, so a retry
        is idempotent — the same OBJEKT_ID produces the same FHIR id and
        ON CONFLICT (id) DO UPDATE on the FHIR side.
    """
    logger = dg.get_dagster_logger()
    total_rows = len(extracted_data)
    unified_sfi_outputs = emit_questionnaire_outputs and _sfi_mode_enabled()
    cleanup_resource_types = {"Observation", "Procedure", "Condition"}
    if unified_sfi_outputs:
        cleanup_resource_types |= {"Questionnaire", "QuestionnaireResponse"}
    questionnaires_by_id: dict[str, dict] = {}

    # -------- One-time setup (shared across all chunks) --------
    dependency_lookups = _ensure_reference_dependencies(
        extracted_data,
        mssql_target_staging,
        fhir_database,
        context,
    )
    resolved = _pre_resolve_references(
        extracted_data,
        fhir_database,
        dependency_lookups=dependency_lookups,
        include_questionnaire_lookups=unified_sfi_outputs,
    )
    routing_resolver = _get_routing_resolver(mssql_target_staging) if _sfi_mode_enabled() else None
    sfi_catalog_sink: dict[str, list[SfiElement]] | None = (
        {} if (_sfi_mode_enabled() and mssql_target_staging is not None) else None
    )

    # -------- Chunked transform + post --------
    chunk_size = aktivitetsresultat_chunk_size_from_context(context, _AKTIVITETSRESULTAT_CHUNK_SIZE)
    n_chunks = (total_rows + chunk_size - 1) // chunk_size
    logger.info(
        "AKTIVITETSRESULTAT chunked transform+post: %d rows in %d chunks of %d",
        total_rows,
        n_chunks,
        chunk_size,
    )
    iter_batches = getattr(extracted_data, "iter_batches", None)
    if callable(iter_batches):
        chunks_iter = iter_batches(chunk_size)
    else:
        chunks_iter = (
            extracted_data[start : start + chunk_size] for start in range(0, total_rows, chunk_size)
        )

    family_totals = {family: _PostCounts() for family in _CHUNK_FAMILIES}
    total_duplicates = 0
    q_posted_ids: set[str] = set()
    q_failed_count = 0
    q_recovered_count = 0
    qr_totals = _PostCounts()

    try:
        for chunk_idx, chunk in enumerate(chunks_iter, start=1):
            tally = _process_chunk(
                chunk=chunk,
                chunk_idx=chunk_idx,
                n_chunks=n_chunks,
                context=context,
                ifdb_oracle=ifdb_oracle,
                fhir_database=fhir_database,
                mssql_target_staging=mssql_target_staging,
                resolved=resolved,
                routing_resolver=routing_resolver,
                sfi_catalog_sink=sfi_catalog_sink,
                questionnaires_by_id=questionnaires_by_id,
                unified_sfi_outputs=unified_sfi_outputs,
                logger=logger,
            )
            # Rækkerne frigives, før næste batch bygges — ellers ligger to
            # chunks i hukommelsen på én gang.
            del chunk
            for family, counts in tally.families.items():
                family_totals[family] = family_totals[family] + counts
            total_duplicates += tally.duplicates_removed
            q_posted_ids.update(tally.questionnaire_ids)
            q_failed_count += tally.questionnaire_failed
            q_recovered_count += tally.questionnaire_recovered
            qr_totals = qr_totals + tally.questionnaire_responses

        if sfi_catalog_sink and mssql_target_staging is not None:
            merge_sfi_catalog_from_datapoints(
                sfi_catalog_sink,
                mssql_target_staging,
                logger,
            )
            post_sfi_codesystem_to_aidbox(sfi_catalog_sink, fhir_database, logger)
    finally:
        del resolved
        fhir_database.safe_clear_identifier_caches_for_resources(
            cleanup_resource_types,
            reason="post-partition cleanup",
        )

    obs = family_totals["Observation"]
    proc = family_totals["Procedure"]
    cond = family_totals["Condition"]
    partition_key = context.partition_key if context.has_partition_key else "N/A"
    logger.info(
        "AKTIVITETSRESULTAT chunked complete: %d Observations + %d Procedures + "
        "%d Conditions posted across %d chunks (partition=%s)",
        obs.posted,
        proc.posted,
        cond.posted,
        n_chunks,
        partition_key,
    )

    obs_result: dg.MaterializeResult = dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(obs.posted),
            "procedure_posted_count": dg.MetadataValue.int(proc.posted),
            "condition_posted_count": dg.MetadataValue.int(cond.posted),
            "failed_count": dg.MetadataValue.int(obs.failed + proc.failed + cond.failed),
            "procedure_failed_count": dg.MetadataValue.int(proc.failed),
            "condition_failed_count": dg.MetadataValue.int(cond.failed),
            "duplicates_removed": dg.MetadataValue.int(total_duplicates),
            "unique_violation_recovered_count": dg.MetadataValue.int(
                obs.recovered + proc.recovered + cond.recovered
            ),
            "chunk_count": dg.MetadataValue.int(n_chunks),
            "chunk_size": dg.MetadataValue.int(chunk_size),
            "extracted_count": dg.MetadataValue.int(total_rows),
            "source": dg.MetadataValue.text("ifdb_oracle"),
            "partition_key": dg.MetadataValue.text(partition_key),
            "method": dg.MetadataValue.text("direct_postgres_dict_chunked"),
        }
    )
    if not unified_sfi_outputs:
        return obs_result

    return (
        obs_result,
        _sfi_side_output_result(
            context=context,
            posted_count=len(q_posted_ids),
            failed_count=q_failed_count,
            chunk_count=n_chunks,
            chunk_size=chunk_size,
            method="direct_postgres_dict_chunked",
            unique_violation_recovered_count=q_recovered_count,
        ),
        _sfi_side_output_result(
            context=context,
            posted_count=qr_totals.posted,
            failed_count=qr_totals.failed,
            chunk_count=n_chunks,
            chunk_size=chunk_size,
            method="direct_postgres_dict_chunked",
            unique_violation_recovered_count=qr_totals.recovered,
        ),
    )


def _post_chunk_families(
    *,
    context: dg.AssetExecutionContext,
    observation_dicts: list[tuple[str, dict]],
    procedure_dicts: list[tuple[str, dict]],
    condition_dicts: list[tuple[str, dict]],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    stage_id: str,
    thread_name_prefix: str,
    max_workers: int,
    invalidate_cache: bool,
) -> dict[str, _PostCounts]:
    """Post én chunks tre ressourcefamilier — den ENESTE post-politik.

    Parallelitet (``max_workers``) og cache-politik (``invalidate_cache``) er
    parametre, så politikken står ved kaldestedet i stedet for gemt i
    hjælperen. Observation postes altid (også tom, så tallene findes);
    Procedure og Condition kun når chunken har nogen. Tallene er FAKTISK
    postede, fejlede og genoprettede rækker fra resultaterne — ikke indsendte.
    """
    from DSA.FHIR.utils.ifdb.fhir_posting import post_resource_dicts_direct

    logger = dg.get_dagster_logger()

    resolve_procedure_organizations(procedure_dicts, fhir_database)

    calls = [
        ParallelCall(
            "Observation",
            post_resource_dicts_direct,
            kwargs={
                "context": context,
                "resource_dicts": observation_dicts,
                "fhir_database": fhir_database,
                "resource_type": "Observation",
                "source_name": "ifdb_oracle",
                "extracted_count": extracted_count,
                "invalidate_cache": invalidate_cache,
            },
        )
    ]
    for family, dicts in (("Procedure", procedure_dicts), ("Condition", condition_dicts)):
        if dicts:
            calls.append(
                ParallelCall(
                    family,
                    post_resource_dicts_direct,
                    kwargs={
                        "context": context,
                        "resource_dicts": dicts,
                        "fhir_database": fhir_database,
                        "resource_type": family,
                        "source_name": "ifdb_oracle",
                        "extracted_count": len(dicts),
                        "invalidate_cache": invalidate_cache,
                    },
                )
            )

    results = run_parallel_calls(
        stage_id=stage_id,
        calls=calls,
        logger=logger,
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix,
    )
    return {family: _PostCounts.from_result(results.get(family)) for family in _CHUNK_FAMILIES}
