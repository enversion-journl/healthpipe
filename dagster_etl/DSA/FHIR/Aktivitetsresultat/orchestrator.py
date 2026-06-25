# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Transform, post, and chunk orchestration for AKTIVITETSRESULTAT."""

from typing import Any

import dagster as dg
from core.parallel import ParallelCall, run_parallel_calls
from core.resources import FHIRDatabaseResource, IFDBOracleResource, MSSQLResource
from DSA.CodeSystem.sfi.catalog import (
    SfiElement,
    merge_sfi_catalog_from_datapoints,
    post_sfi_codesystem_to_aidbox,
)
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
    _build_aktivitetsresultat_procedure_dict,
)
from DSA.FHIR.utils.code_normalization import is_sks_diagnosis_code, is_sks_procedure_code
from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import (
    _AKTIVITETSRESULTAT_CHUNK_SIZE,
    _FAST_MODE,
    _OBS_BUILD_STAGE,
    _SFI_MODE,
)
from DSA.FHIR.utils.ifdb.aktivitetsresultat_identifiers import _sfi_identifier_value
from DSA.FHIR.utils.ifdb.dict_validation import (
    _validate_condition_dicts_sample,
    _validate_observation_dicts_sample,
    _validate_procedure_dicts_sample,
)
from DSA.FHIR.utils.ifdb.routing import RoutingResolver
from DSA.FHIR.utils.ifdb.routing_audit import upsert_routing_decisions as _upsert_routing_decisions
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    AktivitetsresultatRow,
)
from DSA.FHIR.utils.ifdb.xml_parsing_workers import (
    _parse_xml_components_for_rows,
    _parse_xml_lossless_for_rows,
)
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
)
from utils.fhir.fhir_extensions import EXTENSION_EPISODE_OF_CARE
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_STAFF,
    SYSTEM_SOR,
)

from .activity_metadata import (
    _build_level_extensions_dict,
    _build_speciale_extensions_dict,
    _build_udf_extensions_dict,
    _fetch_aktivitetsudfoerelse_data,
    _fetch_aktivitetsudfoerelse_speciale_data,
)
from .questionnaire_outputs import (
    _metadata_int,
    _post_dirty_questionnaires,
    _sfi_side_output_result,
)
from .references import (
    ResolvedRefLookup,
    _ensure_reference_dependencies,
    _pre_resolve_references,
)
from .routing_glue import _get_routing_resolver, _sfi_mode_enabled


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
    for row_dict, parsed_rows in zip(rows, parsed_data_by_index, strict=False):
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
    for row_dict, parsed_rows in zip(rows, parsed_data_by_index, strict=False):
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
            identifiers.add(f"{objekt_id}/{parsed_row.element_nummer}")
    return sorted(identifiers)


def _hydrate_lookups_for_rows(
    *,
    ifdb_oracle: Any,
    context: Any,
    rows: list[dict[str, Any]],
    udf_lookup: dict[str, dict[str, Any]],
    speciale_lookup: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Fetch partition-shared UDF and speciale lookups for missing identifiers."""
    udf_ids = _collect_udf_ids(rows)
    if not udf_ids:
        return udf_lookup, speciale_lookup

    if ifdb_oracle is None:
        return udf_lookup, speciale_lookup

    missing_udf_ids = [udf_id for udf_id in udf_ids if udf_id not in udf_lookup]
    if missing_udf_ids:
        udf_lookup.update(
            _fetch_aktivitetsudfoerelse_data(
                missing_udf_ids,
                ifdb_oracle,
                context,
            )
        )

    missing_speciale_lookup_ids = [udf_id for udf_id in udf_ids if udf_id not in speciale_lookup]
    if missing_speciale_lookup_ids:
        speciale_lookup.update(
            _fetch_aktivitetsudfoerelse_speciale_data(
                missing_speciale_lookup_ids,
                ifdb_oracle,
                context,
            )
        )

    return udf_lookup, speciale_lookup


def _pre_resolve_chunk_sfi_observation_ids(
    *,
    fhir_database: FHIRDatabaseResource,
    resolved: ResolvedRefLookup,
    chunk_sfi_identifiers: list[str],
    logger: Any,
) -> None:
    if not chunk_sfi_identifiers:
        return

    # Keep existing resolved IDs in-memory; only query for unresolved identifiers.
    observation_lookup = resolved.lookups_by_resource_type()["Observation"]
    missing = [
        value
        for value in chunk_sfi_identifiers
        if observation_lookup.get((SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, value)) is None
    ]
    if not missing:
        return

    from time import perf_counter

    start = perf_counter()
    resolved_values = fhir_database.resolve_identifier_values(
        resource_type="Observation",
        identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
        values=missing,
    )
    added = resolved.merge_observation_identifiers(
        SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
        resolved_values,
    )
    logger.info(
        "[SFI] chunk-scope SFI lookup: requested=%d resolved=%d added_to_cache=%d elapsed=%.2fs",
        len(missing),
        len(resolved_values),
        added,
        perf_counter() - start,
    )


def _pre_resolve_chunk_xml_condition_ids(
    *,
    fhir_database: FHIRDatabaseResource,
    resolved: ResolvedRefLookup,
    chunk_xml_condition_identifiers: list[str],
    logger: Any,
) -> None:
    if not chunk_xml_condition_identifiers:
        return

    condition_lookup = resolved.lookups_by_resource_type()["Condition"]
    missing = [
        value
        for value in chunk_xml_condition_identifiers
        if condition_lookup.get((SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION, value)) is None
    ]
    if not missing:
        return

    from time import perf_counter

    start = perf_counter()
    resolved_values = fhir_database.resolve_identifier_values(
        resource_type="Condition",
        identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
        values=missing,
    )
    added = resolved.merge_condition_identifiers(
        SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
        resolved_values,
    )
    logger.info(
        "[XMLCondition] chunk XML Condition lookup: requested=%d resolved=%d added_to_cache=%d elapsed=%.2fs",
        len(missing),
        len(resolved_values),
        added,
        perf_counter() - start,
    )


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
) -> list[tuple[str, dict]]:
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
    return deduped


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

            extra_extensions: list[dict] = []
            if udf_lookup:
                udf_id = row_dict.get("UDFOERELSE_ID")
                udf_id_norm = _normalize_oracle_id(udf_id)
                if udf_id_norm is not None:
                    udf_row = udf_lookup.get(udf_id_norm)
                    if udf_row:
                        extra_extensions.extend(_build_udf_extensions_dict(udf_row))

                    if speciale_lookup:
                        speciale_rows = speciale_lookup.get(udf_id_norm, [])
                        extra_extensions.extend(_build_speciale_extensions_dict(speciale_rows))

            activity_code = row_dict.get("AKTIVITETTYPE_KODE")
            extra_extensions.extend(_build_level_extensions_dict(activity_code))

            if extra_extensions:
                existing = obs.get("extension", [])
                obs["extension"] = existing + extra_extensions

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
                proc_rid, proc = _build_aktivitetsresultat_procedure_dict(row_dict, resolved)
                procedure_dicts.append((proc_rid, proc))
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
    routing_resolver: RoutingResolver | None = None,
    routing_decision_sink: list[dict[str, str | None]] | None = None,
) -> tuple[list[tuple[str, dict]], list[tuple[str, dict]], list[tuple[str, dict]]]:
    """Transform AKTIVITETSRESULTAT rows to FHIR Observation + Procedure/Condition dicts.

    Returns ``(observation_dicts, procedure_dicts, condition_dicts)``.

    Rows where ``AKTIVITETTYPE_KODE`` is an SKS procedure or diagnosis code
    additionally produce a Procedure or Condition dict alongside the Observation.
    """
    logger = dg.get_dagster_logger()

    if _sfi_mode_enabled():
        # SFI per-element pipeline. The fast (Navn,Vaerdi) parser drops the
        # metadata the SFI extractor needs, so
        # the lossless parser is always used in SFI mode regardless of
        # IFDB_XML_FAST_MODE.
        logger.info(
            "AKTIVITETSRESULTAT dict-pipeline mode: SFI per-element (%s)",
            _SFI_MODE,
        )
        parsed_rows, _kontekst_rows, _ = _parse_xml_lossless_for_rows(rows, logger)
        obs_dicts, proc_dicts, cond_dicts, _ = _build_sfi_observation_dicts_for_rows(
            rows=rows,
            parsed_data_by_index=parsed_rows,
            logger=logger,
            resolved=resolved,
            routing_resolver=routing_resolver,
            routing_decision_sink=routing_decision_sink,
        )
    elif _FAST_MODE:
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


def _run_aktivitetsresultat_pipeline_chunked(
    *,
    extracted_data: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource | None = None,
    emit_questionnaire_outputs: bool = False,
) -> dg.MaterializeResult | tuple[dg.MaterializeResult, dg.MaterializeResult, dg.MaterializeResult]:
    """Transform and post AKTIVITETSRESULTAT Observations + Procedures for a
    single partition using chunked streaming.

    Memory semantics:
      - One-time setup outside the chunk loop: dependency resolution + cache
        warmup + auxiliary lookups (udf_lookup, speciale_lookup). These must
        happen once per partition regardless of chunking.
      - Per-chunk: parse XML, build dicts, post. Chunk-local state is freed
        when the next iteration starts, so peak memory is bounded by the
        chunk size, not the partition size.

    Output semantics:
      - IDENTICAL to the pre-chunk monolithic path (modulo random uuids on
        new resources). This is validated by
        ``tests/integration/test_aktivitetsresultat_chunked_parity.py``
        which must PASS before any change to this function is deployed.
      - Specifically: ResolvedRefLookup is built once and shared across all
        chunks, so reference lookups produce the same FHIR UUID for a given
        IFDB identifier regardless of which chunk the row lands in.
      - Output dicts are concatenated in the same order as the input rows.

    Error semantics:
      - A build/post failure in chunk N means chunks 0..N-1 are already
        persisted in FHIR. This matches the pre-chunk behaviour at the
        batch-upsert level (batches are also not atomic). FHIR UUIDs are
        derived from IFDB identifiers via the pre-loaded cache, so a retry
        is idempotent — the same OBJEKT_ID produces the same FHIR id and
        ON CONFLICT (id) DO UPDATE on the FHIR side.
    """
    from DSA.FHIR.utils.ifdb.fhir_posting import post_resource_dicts_direct

    logger = dg.get_dagster_logger()
    total_rows = len(extracted_data)
    unified_sfi_outputs = emit_questionnaire_outputs and _sfi_mode_enabled()
    cleanup_resource_types = {"Observation", "Procedure", "Condition"}
    if unified_sfi_outputs:
        cleanup_resource_types |= {"Questionnaire", "QuestionnaireResponse"}
    questionnaires_by_id: dict[str, dict] = {}
    q_posted_ids: set[str] = set()
    q_failed_count = 0
    qr_posted_count = 0
    qr_failed_count = 0

    # -------- One-time setup (shared across all chunks) --------
    dependency_lookups = _ensure_reference_dependencies(
        extracted_data,
        ifdb_oracle,
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

    udf_lookup: dict[str, dict[str, Any]] = {}
    speciale_lookup: dict[str, list[dict[str, Any]]] = {}

    # -------- Chunked transform + post --------
    chunk_size = _AKTIVITETSRESULTAT_CHUNK_SIZE
    iter_batches = getattr(extracted_data, "iter_batches", None)
    if total_rows <= chunk_size:
        if callable(iter_batches):
            chunk = next(iter_batches(chunk_size), [])
        else:
            chunk = extracted_data
        fhir_database.suspend_identifier_caches()
        try:
            routing_decisions: list[dict[str, str | None]] = []
            qr_dicts: list[tuple[str, dict]] = []
            dirty_questionnaire_ids: list[str] = []
            udf_lookup, speciale_lookup = _hydrate_lookups_for_rows(
                ifdb_oracle=ifdb_oracle,
                context=context,
                rows=chunk,
                udf_lookup=udf_lookup,
                speciale_lookup=speciale_lookup,
            )
            if unified_sfi_outputs:
                parsed_rows, kontekst_rows, _ = _parse_xml_lossless_for_rows(
                    chunk,
                    logger,
                )
                chunk_sfi_identifiers = _collect_chunk_sfi_identifiers(
                    chunk,
                    parsed_rows,
                )
                _pre_resolve_chunk_sfi_observation_ids(
                    fhir_database=fhir_database,
                    resolved=resolved,
                    chunk_sfi_identifiers=chunk_sfi_identifiers,
                    logger=logger,
                )
                _pre_resolve_chunk_xml_condition_ids(
                    fhir_database=fhir_database,
                    resolved=resolved,
                    chunk_xml_condition_identifiers=_collect_chunk_xml_condition_identifiers(
                        chunk, parsed_rows
                    ),
                    logger=logger,
                )
                obs_dicts, proc_dicts, cond_dicts, qr_dicts, _ = _build_sfi_unified_dicts_for_rows(
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
                )
            elif _sfi_mode_enabled():
                parsed_rows, _kontekst_rows, _ = _parse_xml_lossless_for_rows(
                    chunk,
                    logger,
                )
                obs_dicts, proc_dicts, cond_dicts, _ = _build_sfi_observation_dicts_for_rows(
                    rows=chunk,
                    parsed_data_by_index=parsed_rows,
                    logger=logger,
                    resolved=resolved,
                    routing_resolver=routing_resolver,
                    routing_decision_sink=routing_decisions,
                    sfi_catalog_sink=sfi_catalog_sink,
                )
            else:
                obs_dicts, proc_dicts, cond_dicts = _transform_aktivitetsresultat_to_fhir_dicts(
                    chunk,
                    context,
                    resolved=resolved,
                    udf_lookup=udf_lookup,
                    speciale_lookup=speciale_lookup,
                    routing_resolver=routing_resolver,
                    routing_decision_sink=routing_decisions,
                )
            obs_dicts = _dedupe_resource_dicts_by_business_id(
                obs_dicts,
                logger=logger,
                resource_type="Observation",
            )
            proc_dicts = _dedupe_resource_dicts_by_business_id(
                proc_dicts,
                logger=logger,
                resource_type="Procedure",
            )
            cond_dicts = _dedupe_resource_dicts_by_business_id(
                cond_dicts,
                logger=logger,
                resource_type="Condition",
            )
            if qr_dicts:
                qr_dicts = _dedupe_resource_dicts_by_business_id(
                    qr_dicts,
                    logger=logger,
                    resource_type="QuestionnaireResponse",
                )
            _upsert_routing_decisions(
                mssql_target_staging,
                routing_decisions,
                logger,
            )
            _log_sfi_routing_summary(
                logger,
                routing_decisions,
                label="single chunk",
            )

            if sfi_catalog_sink and mssql_target_staging is not None:
                merge_sfi_catalog_from_datapoints(
                    sfi_catalog_sink,
                    mssql_target_staging,
                    logger,
                )
                post_sfi_codesystem_to_aidbox(sfi_catalog_sink, fhir_database, logger)

            obs_result = _post_aktivitetsresultat_dicts_to_fhir(
                context=context,
                observation_dicts=obs_dicts,
                procedure_dicts=proc_dicts,
                condition_dicts=cond_dicts,
                fhir_database=fhir_database,
                extracted_count=total_rows,
            )
            if not unified_sfi_outputs:
                return obs_result

            _, q_failed_now, qids = _post_dirty_questionnaires(
                context=context,
                questionnaires_by_id=questionnaires_by_id,
                dirty_questionnaire_ids=dirty_questionnaire_ids,
                fhir_database=fhir_database,
            )
            q_posted_ids.update(qids)
            q_failed_count += q_failed_now

            if qr_dicts:
                qr_result = post_resource_dicts_direct(
                    context=context,
                    resource_dicts=qr_dicts,
                    fhir_database=fhir_database,
                    resource_type="QuestionnaireResponse",
                    source_name="ifdb_oracle",
                    extracted_count=len(qr_dicts),
                    invalidate_cache=False,
                )
                qr_posted_count += _metadata_int(qr_result, "posted_count")
                qr_failed_count += _metadata_int(qr_result, "failed_count")

            return (
                obs_result,
                _sfi_side_output_result(
                    context=context,
                    posted_count=len(q_posted_ids),
                    failed_count=q_failed_count,
                    chunk_count=1 if total_rows else 0,
                    chunk_size=chunk_size,
                    method="direct_postgres_dict_chunked",
                ),
                _sfi_side_output_result(
                    context=context,
                    posted_count=qr_posted_count,
                    failed_count=qr_failed_count,
                    chunk_count=1 if total_rows else 0,
                    chunk_size=chunk_size,
                    method="direct_postgres_dict_chunked",
                ),
            )
        finally:
            del resolved
            fhir_database.safe_clear_identifier_caches_for_resources(
                cleanup_resource_types,
                reason="post-partition cleanup",
            )

    n_chunks = (total_rows + chunk_size - 1) // chunk_size
    logger.info(
        "AKTIVITETSRESULTAT chunked transform+post: %d rows in %d chunks of %d",
        total_rows,
        n_chunks,
        chunk_size,
    )

    total_obs_posted = 0
    total_proc_posted = 0
    total_cond_posted = 0
    total_obs_failed = 0
    total_proc_failed = 0

    try:
        if callable(iter_batches):
            chunks_iter = iter_batches(chunk_size)
        else:
            chunks_iter = (
                extracted_data[start : start + chunk_size]
                for start in range(0, total_rows, chunk_size)
            )

        for chunk_idx, chunk in enumerate(chunks_iter, start=1):
            logger.info("Chunk %d/%d: transforming %d rows", chunk_idx, n_chunks, len(chunk))

            fhir_database.log_identifier_lookup_status(
                f"pre-chunk-{chunk_idx}",
                resolved.lookups_by_resource_type(),
            )

            fhir_database.suspend_identifier_caches()

            routing_decisions: list[dict[str, str | None]] = []
            qr_dicts: list[tuple[str, dict]] = []
            dirty_questionnaire_ids: list[str] = []
            udf_lookup, speciale_lookup = _hydrate_lookups_for_rows(
                ifdb_oracle=ifdb_oracle,
                context=context,
                rows=chunk,
                udf_lookup=udf_lookup,
                speciale_lookup=speciale_lookup,
            )
            if unified_sfi_outputs:
                parsed_rows, kontekst_rows, _ = _parse_xml_lossless_for_rows(
                    chunk,
                    logger,
                )
                chunk_sfi_identifiers = _collect_chunk_sfi_identifiers(
                    chunk,
                    parsed_rows,
                )
                _pre_resolve_chunk_sfi_observation_ids(
                    fhir_database=fhir_database,
                    resolved=resolved,
                    chunk_sfi_identifiers=chunk_sfi_identifiers,
                    logger=logger,
                )
                _pre_resolve_chunk_xml_condition_ids(
                    fhir_database=fhir_database,
                    resolved=resolved,
                    chunk_xml_condition_identifiers=_collect_chunk_xml_condition_identifiers(
                        chunk, parsed_rows
                    ),
                    logger=logger,
                )
                obs_dicts, proc_dicts, cond_dicts, qr_dicts, _ = _build_sfi_unified_dicts_for_rows(
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
                )
            elif _sfi_mode_enabled():
                parsed_rows, _kontekst_rows, _ = _parse_xml_lossless_for_rows(
                    chunk,
                    logger,
                )
                chunk_sfi_identifiers = _collect_chunk_sfi_identifiers(
                    chunk,
                    parsed_rows,
                )
                _pre_resolve_chunk_sfi_observation_ids(
                    fhir_database=fhir_database,
                    resolved=resolved,
                    chunk_sfi_identifiers=chunk_sfi_identifiers,
                    logger=logger,
                )
                _pre_resolve_chunk_xml_condition_ids(
                    fhir_database=fhir_database,
                    resolved=resolved,
                    chunk_xml_condition_identifiers=_collect_chunk_xml_condition_identifiers(
                        chunk, parsed_rows
                    ),
                    logger=logger,
                )
                obs_dicts, proc_dicts, cond_dicts, _ = _build_sfi_observation_dicts_for_rows(
                    rows=chunk,
                    parsed_data_by_index=parsed_rows,
                    logger=logger,
                    resolved=resolved,
                    routing_resolver=routing_resolver,
                    routing_decision_sink=routing_decisions,
                    sfi_catalog_sink=sfi_catalog_sink,
                )
            else:
                obs_dicts, proc_dicts, cond_dicts = _transform_aktivitetsresultat_to_fhir_dicts(
                    chunk,
                    context,
                    resolved=resolved,
                    udf_lookup=udf_lookup,
                    speciale_lookup=speciale_lookup,
                    routing_resolver=routing_resolver,
                    routing_decision_sink=routing_decisions,
                )
            obs_dicts = _dedupe_resource_dicts_by_business_id(
                obs_dicts,
                logger=logger,
                resource_type="Observation",
            )
            proc_dicts = _dedupe_resource_dicts_by_business_id(
                proc_dicts,
                logger=logger,
                resource_type="Procedure",
            )
            cond_dicts = _dedupe_resource_dicts_by_business_id(
                cond_dicts,
                logger=logger,
                resource_type="Condition",
            )
            if qr_dicts:
                qr_dicts = _dedupe_resource_dicts_by_business_id(
                    qr_dicts,
                    logger=logger,
                    resource_type="QuestionnaireResponse",
                )
            _upsert_routing_decisions(
                mssql_target_staging,
                routing_decisions,
                logger,
            )
            _log_sfi_routing_summary(
                logger,
                routing_decisions,
                label=f"chunk {chunk_idx}/{n_chunks}",
            )

            # invalidate_cache=False: resolved ids are intentionally fixed for
            # the partition while chunks are posting. Those ids come from
            # shared pre-warmed identifier caches. A single clear after the
            # chunk loop is enough.
            #
            post_calls = [
                ParallelCall(
                    "Observation",
                    post_resource_dicts_direct,
                    kwargs={
                        "context": context,
                        "resource_dicts": obs_dicts,
                        "fhir_database": fhir_database,
                        "resource_type": "Observation",
                        "source_name": "ifdb_oracle",
                        "extracted_count": len(chunk),
                        "invalidate_cache": False,
                    },
                )
            ]
            if proc_dicts:
                post_calls.append(
                    ParallelCall(
                        "Procedure",
                        post_resource_dicts_direct,
                        kwargs={
                            "context": context,
                            "resource_dicts": proc_dicts,
                            "fhir_database": fhir_database,
                            "resource_type": "Procedure",
                            "source_name": "ifdb_oracle",
                            "extracted_count": len(proc_dicts),
                            "invalidate_cache": False,
                        },
                    )
                )
            if cond_dicts:
                post_calls.append(
                    ParallelCall(
                        "Condition",
                        post_resource_dicts_direct,
                        kwargs={
                            "context": context,
                            "resource_dicts": cond_dicts,
                            "fhir_database": fhir_database,
                            "resource_type": "Condition",
                            "source_name": "ifdb_oracle",
                            "extracted_count": len(cond_dicts),
                            "invalidate_cache": False,
                        },
                    )
                )

            run_parallel_calls(
                stage_id=f"ifdb.observation.post.chunk.{chunk_idx}",
                calls=post_calls,
                logger=logger,
                max_workers=3,
                thread_name_prefix=f"aktres-post-c{chunk_idx}",
            )
            total_obs_posted += len(obs_dicts)
            total_proc_posted += len(proc_dicts)
            total_cond_posted += len(cond_dicts)

            if unified_sfi_outputs:
                _, q_failed_now, qids = _post_dirty_questionnaires(
                    context=context,
                    questionnaires_by_id=questionnaires_by_id,
                    dirty_questionnaire_ids=dirty_questionnaire_ids,
                    fhir_database=fhir_database,
                )
                q_posted_ids.update(qids)
                q_failed_count += q_failed_now

                if qr_dicts:
                    qr_result = post_resource_dicts_direct(
                        context=context,
                        resource_dicts=qr_dicts,
                        fhir_database=fhir_database,
                        resource_type="QuestionnaireResponse",
                        source_name="ifdb_oracle",
                        extracted_count=len(qr_dicts),
                        invalidate_cache=False,
                    )
                    qr_posted_count += _metadata_int(qr_result, "posted_count")
                    qr_failed_count += _metadata_int(qr_result, "failed_count")

            del obs_dicts
            del proc_dicts
            del cond_dicts
            del qr_dicts
            del chunk

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

    partition_key = context.partition_key if context.has_partition_key else "N/A"
    logger.info(
        "AKTIVITETSRESULTAT chunked complete: %d Observations + %d Procedures + "
        "%d Conditions posted across %d chunks (partition=%s)",
        total_obs_posted,
        total_proc_posted,
        total_cond_posted,
        n_chunks,
        partition_key,
    )

    obs_result = dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(total_obs_posted),
            "procedure_posted_count": dg.MetadataValue.int(total_proc_posted),
            "condition_posted_count": dg.MetadataValue.int(total_cond_posted),
            "failed_count": dg.MetadataValue.int(total_obs_failed + total_proc_failed),
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
        ),
        _sfi_side_output_result(
            context=context,
            posted_count=qr_posted_count,
            failed_count=qr_failed_count,
            chunk_count=n_chunks,
            chunk_size=chunk_size,
            method="direct_postgres_dict_chunked",
        ),
    )


def _post_aktivitetsresultat_dicts_to_fhir(
    context: dg.AssetExecutionContext,
    observation_dicts: list[tuple[str, dict]],
    procedure_dicts: list[tuple[str, dict]],
    condition_dicts: list[tuple[str, dict]],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
) -> dg.MaterializeResult:
    """Post pre-built Observation, Procedure and Condition dicts to the FHIR database.

    Bypasses Pydantic serialisation entirely — calls
    ``batch_upsert_resource_dicts`` which uses orjson for final JSON encoding.
    """
    from DSA.FHIR.utils.ifdb.fhir_posting import post_resource_dicts_direct

    logger = dg.get_dagster_logger()

    obs_result = post_resource_dicts_direct(
        context=context,
        resource_dicts=observation_dicts,
        fhir_database=fhir_database,
        resource_type="Observation",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )

    if procedure_dicts:
        logger.info(
            "Posting %d Procedure dicts derived from AKTIVITETSRESULTAT SKS codes",
            len(procedure_dicts),
        )
        post_resource_dicts_direct(
            context=context,
            resource_dicts=procedure_dicts,
            fhir_database=fhir_database,
            resource_type="Procedure",
            source_name="ifdb_oracle",
            extracted_count=len(procedure_dicts),
        )

    if condition_dicts:
        logger.info(
            "Posting %d Condition dicts derived from AKTIVITETSRESULTAT SKS codes",
            len(condition_dicts),
        )
        post_resource_dicts_direct(
            context=context,
            resource_dicts=condition_dicts,
            fhir_database=fhir_database,
            resource_type="Condition",
            source_name="ifdb_oracle",
            extracted_count=len(condition_dicts),
        )

    return obs_result


AKTIVITETSRESULTAT_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
    ReferenceFieldConfig(
        field_name="encounter",
        target_resource_type="Encounter",
        identifier_system=SYSTEM_IFDB_OBJECT_ID,
        is_required=False,
        description="Encounter reference from IFDB ENCOUNTER_ID resolved via Encounter OBJECT_ID",
    ),
]

AKTIVITETSRESULTAT_NESTED_REFERENCE_CONFIGS = [
    NestedReferenceFieldConfig(
        field_path="extension.valueReference",
        identifier_system_to_type={
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        extension_url=EXTENSION_EPISODE_OF_CARE,
        is_required=False,
        description="Episode of care reference from extension",
    ),
    NestedReferenceFieldConfig(
        field_path="performer",
        identifier_system_to_type={
            SYSTEM_SOR: "Organization",
            SYSTEM_IFDB_STAFF: "Practitioner",
        },
        is_required=False,
        description="Performer (Organization from SOR or Practitioner from staff ID)",
    ),
]
