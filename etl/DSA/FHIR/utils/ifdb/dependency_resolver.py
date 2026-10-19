# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dependency resolver for FHIR IFDB pipeline.

This module provides utilities to resolve missing dependencies (Patient, EpisodeOfCare, etc.)
by fetching them from the IFDB archive layer and posting to FHIR server on-demand.

The problem: When processing Encounters in partition 2024-03, we may reference Patients
that were last modified in a different partition (e.g., 2022-01). The Dagster partition
dependency only ensures same-partition ordering, not cross-partition data availability.

The solution: Before posting resources that have references, check if the referenced
resources exist in FHIR server. If not, fetch them from the archive (``arc.ifdb_*``)
and post them first.

Patients are never built or written here: callers use
``DSA/FHIR/Patient/patient_service.ensure_patients``, and
``resolve_missing_patients_by_cpr`` adds the BOM_IFDB.PATIENTINFO fallback to it.

Patient/Encounter/EpisodeOfCare lookups read the archive tables (MSSQL). The
CPR fallback in :func:`resolve_missing_patients_by_cpr` reads arc.bom_ifdb_patientinfo
first and goes to Oracle only for CPRs the archive does not cover — and only when an
Oracle resource is given; without one the archive is still read.
"""

from typing import Any

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import resource_source
from core.resources import FHIRDatabaseResource, IFDBOracleResource, MSSQLResource
from DSA.FHIR.Patient.patient_service import NEED_REFERENCE, ensure_patients
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch
from DSA.FHIR.utils.reference_resolver import batch_resolve_references
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)


def resolve_missing_patients_by_cpr(
    cprs: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource | None,
) -> dict[str, str]:
    """Ensure these CPRs exist as Patient, falling back to BOM_IFDB.PATIENTINFO; ``{cpr: FHIR id}``."""
    if not cprs:
        return {}
    logger = dg.get_dagster_logger()
    first = ensure_patients(
        context,
        mssql,
        fhir_database,
        cprs=cprs,
        source_name="bom_ifdb_cpr_resolver",
        need=NEED_REFERENCE,
    )
    resolved = dict(first.by_cpr)
    written = first.written
    if first.unresolved_cprs:
        rows = _fetch_bom_ifdb_patientinfo_by_cpr(
            list(first.unresolved_cprs), ifdb_oracle, context, mssql=mssql
        )
        # Keep every row per CPR: appointments reference each row's OID.
        by_cpr: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if row.get("CPRNR"):
                by_cpr.setdefault(str(row["CPRNR"]).strip(), []).append(row)
        still = set(first.unresolved_cprs)
        if by_cpr:
            second = ensure_patients(
                context,
                mssql,
                fhir_database,
                cprs=by_cpr,
                bom_rows_by_cpr=by_cpr,
                source_name="bom_ifdb_cpr_resolver",
            )
            resolved.update(second.by_cpr)
            written += second.written
            still -= set(second.by_cpr)
        if still:
            logger.warning(
                "[DEPENDENCY] CPR resolver: %d CPRs neither in IFDB, the register nor in "
                "BOM_IFDB.PATIENTINFO%s — Appointments referencing them will fail "
                "reference resolution",
                len(still),
                " (archive only; no Oracle resource provided)" if ifdb_oracle is None else "",
            )
    if written:
        fhir_database.mark_stale("Patient")
        fhir_database.safe_clear_identifier_caches_for_resources(
            {"Patient"}, reason="Patient written by CPR resolver"
        )
    return resolved


def _fetch_bom_ifdb_patientinfo_by_cpr(
    cprs: list[str],
    ifdb_oracle: IFDBOracleResource | None,
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource | None = None,
) -> list[dict[str, Any]]:
    """Fetch BOM_IFDB.PATIENTINFO rows by CPR.

    Reads arc.bom_ifdb_patientinfo when MSSQL is available and falls back to
    Oracle for the CPRs the archive did not cover — only when an Oracle
    resource is given; without one the archive rows are the answer.

    The fallback is per-CPR, not all-or-nothing: an earlier version returned the
    archive result whenever it was non-empty, so a batch where the archive held
    1 of 100 CPRs returned that one row and silently dropped the other 99 —
    while a batch where the archive held none fell through and resolved all 100.
    Partial archive coverage (the normal state before the initial load
    completes, or whenever the archive lags) was therefore worse than no
    coverage, and the caller only warns when the result is *completely* empty.
    """
    columns = ["PATIENTINFOOID", "CPRNR", "FORNAVN", "EFTERNAVN", "FOEDSELSDATO", "KOEN"]

    archive_rows: list[dict[str, Any]] = []
    remaining = list(cprs)

    if mssql is not None:
        from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch

        archive_rows = chunked_in_fetch(
            cprs,
            table="arc.bom_ifdb_patientinfo",
            columns=columns,
            where_column="CPRNR",
            mssql=mssql,
        )
        covered = {str(row["CPRNR"]).strip() for row in archive_rows if row.get("CPRNR")}
        remaining = [cpr for cpr in cprs if str(cpr).strip() not in covered]
        if remaining and ifdb_oracle is not None:
            context.log.info(
                f"[DEPENDENCY] CPR resolver: {len(covered)}/{len(cprs)} CPRs resolved from "
                f"arc.bom_ifdb_patientinfo; falling back to Oracle for {len(remaining)}"
            )

    if not remaining or ifdb_oracle is None:
        return archive_rows

    dfm = run_ctx = None
    if mssql is not None:
        dfm = DfmManager(mssql, context)
        run_ctx = dfm.start_dfm_run(
            "fhir_server",
            "Patient",
            dagster_run_id=context.run_id,
            mode="ids",
            checkpoint_from=("fhir_server", "Patient"),
            **resource_source(ifdb_oracle, "BOM_IFDB.PATIENTINFO"),
        )
    try:
        oracle_rows = _fetch_rows_by_ids_from_ifdb(
            ids=remaining,
            table="BOM_IFDB.PATIENTINFO",
            columns=columns,
            where_column="CPRNR",
            ifdb_oracle=ifdb_oracle,
            context=context,
        )
        if dfm is not None:
            dfm.complete_dfm_run(run_ctx.log_id, rows_affected=len(oracle_rows))
    except Exception as exc:
        if dfm is not None:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
    return archive_rows + oracle_rows


def resolve_missing_episodes_of_care(
    eoc_ids: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> dict[str, str]:
    """
    Check which episodes of care exist in FHIR database and create any missing ones.

    This also handles the patient dependency - if an EpisodeOfCare is missing,
    we also need to ensure its Patient exists first.

    Args:
        eoc_ids: Set of IFDB episode of care IDs to verify
        mssql: Staging MSSQL resource for fetching EOC data from arc.ifdb_episode_of_care
        fhir_database: FHIR database resource for ID resolution and posting
        context: Dagster asset execution context

    Returns:
        Dictionary mapping EOC IFDB IDs to FHIR database IDs (all EOCs, existing + new)
    """
    if not eoc_ids:
        return {}

    logger = dg.get_dagster_logger()
    eoc_ids_list = list(eoc_ids)

    # First, try to resolve existing episodes
    logger.info(f"[DEPENDENCY] Checking {len(eoc_ids_list)} episodes of care in FHIR server")

    eoc_identifiers = {eid: "" for eid in eoc_ids_list}
    resolved_eocs = batch_resolve_references(
        identifiers=eoc_identifiers,
        resource_type="EpisodeOfCare",
        identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        fhir_database=fhir_database,
    )

    # Find missing episodes
    missing_eoc_ids = [eid for eid in eoc_ids_list if eid not in resolved_eocs]

    if not missing_eoc_ids:
        logger.info(
            f"[DEPENDENCY] All {len(eoc_ids_list)} episodes of care already exist in FHIR server"
        )
        return resolved_eocs

    logger.info(
        f"[DEPENDENCY] Found {len(missing_eoc_ids)} missing episodes of care - fetching from archive"
    )

    # Fetch missing episodes from the archive
    missing_eoc_rows = _fetch_episodes_of_care_from_archive(missing_eoc_ids, mssql)

    if not missing_eoc_rows:
        logger.warning(
            f"[DEPENDENCY] Could not fetch any of the {len(missing_eoc_ids)} missing episodes from archive"
        )
        return resolved_eocs

    logger.info(
        f"[DEPENDENCY] Fetched {len(missing_eoc_rows)} episode of care records from archive"
    )

    # First, ensure all referenced patients exist
    patient_ids = {str(row["PATIENT_ID"]) for row in missing_eoc_rows if row.get("PATIENT_ID")}
    if patient_ids:
        logger.info(f"[DEPENDENCY] Resolving {len(patient_ids)} patients referenced by episodes")
        ensure_patients(
            ifdb_patient_ids=patient_ids,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
            need=NEED_REFERENCE,
        )

    # Transform and post the missing episodes
    from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import (
        _post_episodes_to_fhir,
        _transform_episodes_to_fhir,
    )

    episodes = _transform_episodes_to_fhir(missing_eoc_rows, context)
    logger.info(f"[DEPENDENCY] Transformed {len(episodes)} FHIR EpisodeOfCare resources")

    # Post the missing episodes
    _post_episodes_to_fhir(
        context=context,
        episodes=episodes,
        extracted_count=len(missing_eoc_rows),
        fhir_database=fhir_database,
        mssql=mssql,
    )

    fhir_database.mark_stale("EpisodeOfCare")
    fhir_database.safe_clear_identifier_caches_for_resources(
        {"EpisodeOfCare"}, reason="new EpisodeOfCare posted by dependency resolver"
    )

    all_resolved = batch_resolve_references(
        identifiers=eoc_identifiers,
        resource_type="EpisodeOfCare",
        identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        fhir_database=fhir_database,
        force_reload=True,
    )

    newly_created = len(all_resolved) - len(resolved_eocs)
    logger.info(f"[DEPENDENCY] Created {newly_created} new EpisodeOfCare resources in FHIR server")

    return all_resolved


def resolve_missing_encounters(
    encounter_ids: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> dict[str, str]:
    """
    Check which encounters exist in FHIR database and create any missing ones.

    Encounter identifiers in IFDB are still persisted under SYSTEM_IFDB_OBJECT_ID,
    so dependency resolution follows the same convention used by the Encounter asset
    and the existing idempotency tests.

    Args:
        encounter_ids: Set of IFDB encounter OBJECT_ID values to verify
        mssql: Staging MSSQL resource for fetching Encounter data from arc.ifdb_encounter
        fhir_database: FHIR database resource for ID resolution and posting
        context: Dagster asset execution context

    Returns:
        Dictionary mapping IFDB encounter OBJECT_ID values to FHIR database IDs
        (all encounters, existing + newly created)
    """
    if not encounter_ids:
        return {}

    logger = dg.get_dagster_logger()
    encounter_ids_list = list(encounter_ids)

    logger.info(f"[DEPENDENCY] Checking {len(encounter_ids_list)} encounters in FHIR server")

    encounter_identifiers = {eid: "" for eid in encounter_ids_list}
    resolved_encounters = batch_resolve_references(
        identifiers=encounter_identifiers,
        resource_type="Encounter",
        identifier_system=SYSTEM_IFDB_OBJECT_ID,
        fhir_database=fhir_database,
    )

    missing_encounter_ids = [eid for eid in encounter_ids_list if eid not in resolved_encounters]

    if not missing_encounter_ids:
        logger.info(
            f"[DEPENDENCY] All {len(encounter_ids_list)} encounters already exist in FHIR server"
        )
        return resolved_encounters

    logger.info(
        f"[DEPENDENCY] Found {len(missing_encounter_ids)} missing encounters - fetching from archive"
    )

    missing_encounter_rows = _fetch_encounters_from_archive(missing_encounter_ids, mssql)

    if not missing_encounter_rows:
        logger.warning(
            f"[DEPENDENCY] Could not fetch any of the {len(missing_encounter_ids)} missing encounters from archive"
        )
        return resolved_encounters

    logger.info(
        f"[DEPENDENCY] Fetched {len(missing_encounter_rows)} encounter records from archive"
    )

    from DSA.FHIR.Encounter.Encounter_IFDB_dfm import (
        _fetch_absence_data_for_batch,
        _fetch_bed_space_data_for_batch,
        _fetch_location_data_for_batch,
        _post_encounters_to_fhir,
        _transform_encounters_to_fhir,
    )

    locations_by_encounter = _fetch_location_data_for_batch(missing_encounter_ids, mssql)
    beds_by_encounter = _fetch_bed_space_data_for_batch(missing_encounter_ids, mssql)
    absences_by_encounter = _fetch_absence_data_for_batch(missing_encounter_ids, mssql)
    encounters = _transform_encounters_to_fhir(
        missing_encounter_rows,
        locations_by_encounter,
        context,
        beds_by_encounter=beds_by_encounter,
        absences_by_encounter=absences_by_encounter,
    )
    logger.info(f"[DEPENDENCY] Transformed {len(encounters)} FHIR Encounter resources")

    post_kwargs: dict[str, Any] = {
        "context": context,
        "encounters": encounters,
        "fhir_database": fhir_database,
        "extracted_count": len(missing_encounter_rows),
        "mssql": mssql,
        "resolve_missing_dependencies": True,
    }
    _post_encounters_to_fhir(**post_kwargs)

    fhir_database.mark_stale("Encounter")
    fhir_database.safe_clear_identifier_caches_for_resources(
        {"Encounter"}, reason="new Encounter posted by dependency resolver"
    )

    all_resolved = batch_resolve_references(
        identifiers=encounter_identifiers,
        resource_type="Encounter",
        identifier_system=SYSTEM_IFDB_OBJECT_ID,
        fhir_database=fhir_database,
        force_reload=True,
    )

    newly_created = len(all_resolved) - len(resolved_encounters)
    logger.info(f"[DEPENDENCY] Created {newly_created} new Encounter resources in FHIR server")

    return all_resolved


def _fetch_episodes_of_care_from_archive(
    eoc_ids: list[str],
    mssql: MSSQLResource,
) -> list[dict[str, Any]]:
    """Fetch archived episode of care records from arc.ifdb_episode_of_care by their IDs."""
    from DSI.oracle.ifdb_extract import EOC_COLUMNS

    # include_deleted=True: DELETED-soft-deleted rows must reach the transform,
    # which tombstones them — required references stay resolvable.
    return chunked_in_fetch(
        eoc_ids,
        table="arc.ifdb_episode_of_care",
        columns=EOC_COLUMNS,
        where_column="OBJECT_ID",
        mssql=mssql,
        include_deleted=True,
    )


def _fetch_encounters_from_archive(
    encounter_ids: list[str],
    mssql: MSSQLResource,
) -> list[dict[str, Any]]:
    """Fetch archived encounter records from arc.ifdb_encounter by their OBJECT_ID values."""
    from DSI.oracle.ifdb_extract import ENCOUNTER_COLUMNS

    # include_deleted=True: DELETED-soft-deleted rows must reach the transform,
    # which tombstones them — required references stay resolvable.
    return chunked_in_fetch(
        encounter_ids,
        table="arc.ifdb_encounter",
        columns=ENCOUNTER_COLUMNS,
        where_column="OBJECT_ID",
        mssql=mssql,
        include_deleted=True,
    )


def _fetch_rows_by_ids_from_ifdb(
    ids: list[str],
    table: str,
    columns: list[str],
    where_column: str,
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> list[dict[str, Any]]:
    """Fetch rows from IFDB by a list of IDs (chunked, bind-parameter safe)."""
    from DSA.FHIR.utils.ifdb.oracle_lookup import chunked_in_fetch

    return chunked_in_fetch(
        ids,
        table=table,
        columns=columns,
        where_column=where_column,
        ifdb_oracle=ifdb_oracle,
        context=context,
    )


def resolve_careplan_dependencies(
    careplans: list,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> dict[str, str]:
    """
    Resolve missing Patient and EpisodeOfCare dependencies for CarePlan resources.

    Collects referenced Patient IDs (from subject.identifier) and EpisodeOfCare IDs
    (from activity.extension.valueReference.identifier) and ensures they exist in the
    FHIR server before reference resolution runs.

    EpisodeOfCare resolution also cascades to resolve its own Patient dependencies.

    Args:
        careplans: List of FHIR CarePlan resources
        mssql: Staging MSSQL resource for fetching missing data from the archive
        fhir_database: FHIR database resource
        context: Dagster asset execution context

    Returns:
        Mapping of IFDB PATIENT_ID to FHIR Patient id for every patient
        referenced by *careplans*. Two PATIENT_IDs that belong to the same
        person map to the same FHIR id, because the CPR bridge above resolves a
        re-issued record to the patient that already exists. Callers key the
        CarePlan on that value so a re-issue updates the resource instead of
        creating a second one.
    """
    logger = dg.get_dagster_logger()

    patient_ids: set[str] = set()
    eoc_ids: set[str] = set()

    for cp in careplans:
        # Patient from subject.identifier
        subject = getattr(cp, "subject", None)
        if subject and getattr(subject, "identifier", None):
            ident = subject.identifier
            if getattr(ident, "system", None) == SYSTEM_IFDB_PATIENT_ID and ident.value:
                patient_ids.add(str(ident.value))

        # EpisodeOfCare from activity[].extension[].valueReference.identifier
        for activity in getattr(cp, "activity", None) or []:
            for ext in getattr(activity, "extension", None) or []:
                ref = getattr(ext, "valueReference", None)
                if ref and getattr(ref, "identifier", None):
                    ident = ref.identifier
                    if (
                        getattr(ident, "system", None) == SYSTEM_IFDB_EPISODE_OF_CARE_ID
                        and ident.value
                    ):
                        eoc_ids.add(str(ident.value))

    # Resolve EoC first (this also resolves Patient deps for the new EoCs)
    if eoc_ids:
        logger.info(f"[DEPENDENCY] Ensuring {len(eoc_ids)} episodes of care exist")
        resolve_missing_episodes_of_care(
            eoc_ids=eoc_ids,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )

    # Then resolve any remaining patient dependencies. The map is returned
    # rather than discarded: it is what lets the caller give the CarePlan the
    # person's identity instead of the IFDB patient record's.
    if not patient_ids:
        return {}

    logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids)} patients exist")
    return ensure_patients(
        ifdb_patient_ids=patient_ids,
        mssql=mssql,
        fhir_database=fhir_database,
        context=context,
        need=NEED_REFERENCE,
    ).by_ifdb_id


def ensure_patient_dependencies(
    patient_ids: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> None:
    """
    Ensure all required patients exist in FHIR database before proceeding.

    This is a convenience wrapper that raises an error if patients cannot be resolved.

    Args:
        patient_ids: Set of IFDB patient IDs that must exist
        mssql: Staging MSSQL resource
        fhir_database: FHIR database resource
        context: Dagster asset execution context

    Raises:
        ValueError: If any required patients cannot be resolved or created
    """
    resolved = ensure_patients(
        ifdb_patient_ids=patient_ids,
        mssql=mssql,
        fhir_database=fhir_database,
        context=context,
        need=NEED_REFERENCE,
    ).by_ifdb_id
    _raise_if_unresolved(required_ids=patient_ids, resolved=resolved, entity_label="patients")


def ensure_episode_of_care_dependencies(
    eoc_ids: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> None:
    """
    Ensure all required episodes of care exist in FHIR database before proceeding.

    This also ensures patient dependencies are met.

    Args:
        eoc_ids: Set of IFDB episode of care IDs that must exist
        mssql: Staging MSSQL resource
        fhir_database: FHIR database resource
        context: Dagster asset execution context

    Raises:
        ValueError: If any required episodes cannot be resolved or created
    """
    resolved = resolve_missing_episodes_of_care(
        eoc_ids=eoc_ids,
        mssql=mssql,
        fhir_database=fhir_database,
        context=context,
    )
    _raise_if_unresolved(required_ids=eoc_ids, resolved=resolved, entity_label="episodes of care")


def ensure_encounter_dependencies(
    encounter_ids: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> None:
    """
    Ensure all required encounters exist in FHIR database before proceeding.

    Encounter creation reuses the Encounter asset's own dependency-resolution
    logic, so Patient and EpisodeOfCare prerequisites are handled consistently
    with the Encounter asset's existing implementation.

    Args:
        encounter_ids: Set of IFDB encounter OBJECT_ID values that must exist
        mssql: Staging MSSQL resource
        fhir_database: FHIR database resource
        context: Dagster asset execution context

    Raises:
        ValueError: If any required encounters cannot be resolved or created
    """
    resolved = resolve_missing_encounters(
        encounter_ids=encounter_ids,
        mssql=mssql,
        fhir_database=fhir_database,
        context=context,
    )
    _raise_if_unresolved(required_ids=encounter_ids, resolved=resolved, entity_label="encounters")


def _raise_if_unresolved(
    *,
    required_ids: set[str],
    resolved: dict[str, str],
    entity_label: str,
) -> None:
    """Raise a consistent error when dependency IDs cannot be resolved."""
    missing = [dep_id for dep_id in required_ids if dep_id not in resolved]
    if missing:
        raise ValueError(
            f"Could not resolve {len(missing)} required {entity_label}: "
            f"{missing[:5]}{'...' if len(missing) > 5 else ''}"
        )
