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
and post them first. CPR-matched patients additionally get their new IFDB PATIENT_ID
persisted as an identifier on the existing FHIR resource so downstream resolution can
find them.

Patient/Encounter/EpisodeOfCare lookups read the archive tables (MSSQL). The
CPR fallback in :func:`resolve_missing_patients_by_cpr` reads arc.bom_ifdb_patientinfo
first and goes to Oracle only for CPRs the archive does not cover.
"""

from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource, IFDBOracleResource, MSSQLResource
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch
from DSA.FHIR.utils.ifdb.fhir_posting import post_resources_direct
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id
from DSA.FHIR.utils.reference_resolver import batch_resolve_references
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from utils.fhir.identifier_systems import (
    SYSTEM_BOM_IFDB_PATIENTINFO_OID,
    SYSTEM_CPR,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)


def resolve_missing_patients(
    patient_ids: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> dict[str, str]:
    """
    Check which patients exist in FHIR database and create any missing ones.

    Args:
        patient_ids: Set of IFDB patient IDs to verify
        mssql: Staging MSSQL resource for fetching patient data from arc.ifdb_patient
        fhir_database: FHIR database resource for ID resolution and posting
        context: Dagster asset execution context

    Returns:
        Dictionary mapping patient IFDB IDs to FHIR database IDs (all patients, existing + new)
    """
    if not patient_ids:
        return {}

    logger = dg.get_dagster_logger()
    patient_ids_list = list(patient_ids)

    # First, try to resolve existing patients
    logger.info(f"[DEPENDENCY] Checking {len(patient_ids_list)} patients in FHIR server")

    patient_identifiers = {pid: "" for pid in patient_ids_list}
    resolved_patients = batch_resolve_references(
        identifiers=patient_identifiers,
        resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        fhir_database=fhir_database,
    )

    # Find missing patients
    missing_patient_ids = [pid for pid in patient_ids_list if pid not in resolved_patients]

    if not missing_patient_ids:
        logger.info(
            f"[DEPENDENCY] All {len(patient_ids_list)} patients already exist in FHIR server"
        )
        return resolved_patients

    logger.info(
        f"[DEPENDENCY] Found {len(missing_patient_ids)} missing patients - fetching from archive"
    )

    # Fetch missing patients from the archive
    missing_patient_rows = _fetch_patients_from_archive(missing_patient_ids, mssql)

    if not missing_patient_rows:
        logger.warning(
            f"[DEPENDENCY] Could not fetch any of the {len(missing_patient_ids)} "
            "missing patients from archive"
        )
        return resolved_patients

    logger.info(f"[DEPENDENCY] Fetched {len(missing_patient_rows)} patient records from archive")

    # CPR deduplication guard: if a missing IFDB patient shares a CPR with an
    # already-existing FHIR patient, reuse that patient instead of posting a
    # new (possibly soft-deleted) duplicate that would overwrite the active one.
    ifdb_id_to_cpr = {
        str(row["PATIENT_ID"]): str(row["CPR"]) for row in missing_patient_rows if row.get("CPR")
    }
    cpr_values = list(ifdb_id_to_cpr.values())
    cpr_to_fhir_id: dict[str, str] = {}
    if cpr_values:
        cpr_to_fhir_id = batch_resolve_references(
            identifiers={cpr: "" for cpr in cpr_values},
            resource_type="Patient",
            identifier_system=SYSTEM_CPR,
            fhir_database=fhir_database,
        )

    cpr_matched: dict[str, str] = {}
    rows_to_post = []
    for row in missing_patient_rows:
        pid = str(row["PATIENT_ID"])
        cpr = ifdb_id_to_cpr.get(pid)
        existing_fhir_id = cpr_to_fhir_id.get(cpr) if cpr else None
        if existing_fhir_id:
            logger.info(
                f"[DEPENDENCY] Patient {pid} resolved via CPR match to existing FHIR patient "
                f"— skipping post to avoid overwriting active record"
            )
            cpr_matched[pid] = existing_fhir_id
        else:
            rows_to_post.append(row)

    resolved_patients.update(cpr_matched)

    # Persist CPR-match bridges into FHIR so the existing patient has the new
    # IFDB PATIENT_ID in her identifier array. Without this, the in-memory
    # cpr_matched mapping is invisible to downstream reference resolution
    # (which only consults the identifier cache built from FHIR state), and
    # a ValueError is raised for patient IDs that were actually "found".
    if cpr_matched:
        # Return value (added count) is logged by the callee itself.
        _persist_cpr_matched_identifiers(
            cpr_matched=cpr_matched,
            fhir_database=fhir_database,
            context=context,
        )

    if not rows_to_post:
        logger.info(
            f"[DEPENDENCY] All {len(missing_patient_ids)} missing patients resolved via CPR "
            "— no posts needed"
        )
        if cpr_matched:
            # Always invalidate when CPR-matched patients exist: even when
            # the bridge identifier was already persisted (0 added), the
            # initial targeted lookup missed these patients (they were in
            # the "missing" set). The identifier cache must be rebuilt so
            # downstream reference resolution can find them by IFDB
            # PATIENT_ID.
            fhir_database.mark_stale("Patient")
            fhir_database.safe_clear_identifier_caches_for_resources(
                {"Patient"}, reason="CPR-bridge patients resolved"
            )
            all_resolved = batch_resolve_references(
                identifiers=patient_identifiers,
                resource_type="Patient",
                identifier_system=SYSTEM_IFDB_PATIENT_ID,
                fhir_database=fhir_database,
                force_reload=True,
            )
            all_resolved.update(cpr_matched)
            return all_resolved
        return resolved_patients

    logger.info(
        f"[DEPENDENCY] {len(rows_to_post)} patients have no CPR match in FHIR "
        "— will post from IFDB"
    )

    # Transform and post the missing patients
    from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir
    from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb

    patients = transform_fhir_patient_ifdb(context, rows_to_post)
    logger.info(f"[DEPENDENCY] Transformed {len(patients)} FHIR Patient resources")

    # Post the missing patients
    _post_patients_to_fhir(
        context=context,
        patients=patients,
        fhir_database=fhir_database,
        extracted_count=len(rows_to_post),
        source_name="ifdb_oracle_dependency_resolver",
    )

    fhir_database.mark_stale("Patient")
    fhir_database.safe_clear_identifier_caches_for_resources(
        {"Patient"}, reason="new Patient posted by dependency resolver"
    )

    all_resolved = batch_resolve_references(
        identifiers=patient_identifiers,
        resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        fhir_database=fhir_database,
        force_reload=True,
    )

    # Merge back patients that were resolved via CPR match. After the CPR-bridge
    # persist step above, these should now show up in all_resolved too — but we
    # keep this union as a belt-and-braces safeguard against any transient miss.
    all_resolved.update(cpr_matched)

    newly_created = len(all_resolved) - len(resolved_patients)
    logger.info(f"[DEPENDENCY] Created {newly_created} new Patient resources in FHIR server")

    return all_resolved


def _persist_cpr_matched_identifiers(
    cpr_matched: dict[str, str],
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> int:
    """Add an SYSTEM_IFDB_PATIENT_ID identifier to existing FHIR patients that
    were resolved via CPR match.

    Without this, the in-memory CPR→FHIR mapping is forgotten as soon as
    resolve_missing_patients returns. Downstream reference resolution consults
    the identifier cache built from FHIR state and fails to find the new
    IFDB PATIENT_ID because only the old CPR identifier is on the patient
    resource.

    The UPDATE is atomic and idempotent:
      - Row-level locking serialises concurrent writers for the same patient.
      - The CASE expression checks identifier containment before append, so
        repeated calls or parallel calls never produce duplicate identifiers.

    Args:
        cpr_matched: {ifdb_patient_id: existing_fhir_id}
        fhir_database: FHIR DB resource
        context: Dagster context (unused but matches sibling signatures)

    Returns:
        Number of FHIR patients where an identifier was actually added
        (excluding idempotent no-ops where the identifier was already present).
    """
    if not cpr_matched:
        return 0

    logger = dg.get_dagster_logger()
    values = [
        (fhir_id, SYSTEM_IFDB_PATIENT_ID, ifdb_id) for ifdb_id, fhir_id in cpr_matched.items()
    ]

    # The WHERE clause with NOT-containment guards against duplicate identifier
    # entries. Row-level locking (PostgreSQL MVCC) serialises concurrent
    # writers: if two workers try to add the same (system, value) to the same
    # patient, the second one sees the already-appended identifier and becomes
    # a zero-row no-op.
    update_sql = """
        UPDATE patient p
        SET resource = jsonb_set(
            p.resource,
            '{identifier}',
            COALESCE(p.resource->'identifier', '[]'::jsonb) || jsonb_build_array(
                jsonb_build_object('system', %s::text, 'value', %s::text)
            )
        )
        WHERE p.id::text = %s
          AND NOT (
            COALESCE(p.resource->'identifier', '[]'::jsonb) @> jsonb_build_array(
                jsonb_build_object('system', %s::text, 'value', %s::text)
            )
          )
    """

    added = 0
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            for fhir_id, system, value in values:
                cur.execute(update_sql, (system, value, fhir_id, system, value))
                if cur.rowcount > 0:
                    added += 1
        conn.commit()

    logger.info(
        "[DEPENDENCY] CPR-bridge: added %s identifier to %d/%d existing FHIR "
        "patients (rest already had the identifier from a previous run)",
        SYSTEM_IFDB_PATIENT_ID,
        added,
        len(cpr_matched),
    )
    return added


def resolve_missing_patients_by_cpr(
    cprs: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource | None,
) -> dict[str, str]:
    """
    Ensure FHIR Patient resources exist for a set of CPR values.

    BOM_IFDB.AFTALE references patients via PATIENTINFOOID -> PATIENTINFO.CPRNR, not via
    IFDB PATIENT_ID. The existing :func:`resolve_missing_patients` expects IFDB patient IDs
    (per skill rule #23 — passing CPRs there silently fails). This sibling resolver works
    CPR-first with two fallback layers:

      1. Check the FHIR server for Patients with ``Identifier(system=SYSTEM_CPR, value=cpr)``.
      2. For missing CPRs, look them up in ``arc.ifdb_patient`` by CPR. If found, delegate
         to :func:`resolve_missing_patients` by IFDB PATIENT_ID — this re-uses the existing
         IFDB Patient pipeline (full demographic transform).
      3. CPRs still unresolved are extracted from ``BOM_IFDB.PATIENTINFO`` (Oracle — the
         only remaining Oracle read in this module; requires ``ifdb_oracle``) and posted
         as minimal Patient resources keyed by ``SYSTEM_CPR`` (primary) and
         ``SYSTEM_BOM_IFDB_PATIENTINFO_OID`` (secondary).

    Args:
        cprs: Set of CPR strings to ensure exist as FHIR Patient resources.
        mssql: Staging MSSQL resource for the arc.ifdb_patient lookup.
        ifdb_oracle: Oracle resource for the BOM_IFDB.PATIENTINFO fallback. When None,
            that fallback is skipped (CPRs without an IFDB row stay unresolved).

    Returns:
        Mapping ``cpr -> fhir_patient_id`` for all resolved patients (existing + new).
    """
    if not cprs:
        return {}

    logger = dg.get_dagster_logger()
    cpr_list = sorted(cprs)
    logger.info(f"[DEPENDENCY] CPR resolver: checking {len(cpr_list)} CPRs in FHIR server")

    cpr_identifiers = {cpr: "" for cpr in cpr_list}
    resolved_by_cpr = batch_resolve_references(
        identifiers=cpr_identifiers,
        resource_type="Patient",
        identifier_system=SYSTEM_CPR,
        fhir_database=fhir_database,
    )

    missing_cprs = [cpr for cpr in cpr_list if cpr not in resolved_by_cpr]
    if not missing_cprs:
        logger.info(f"[DEPENDENCY] CPR resolver: all {len(cpr_list)} CPRs already in FHIR")
        return resolved_by_cpr

    logger.info(
        f"[DEPENDENCY] CPR resolver: {len(missing_cprs)} CPRs missing — "
        "trying arc.ifdb_patient fallback"
    )

    ifdb_rows = _fetch_ifdb_patients_by_cpr(missing_cprs, mssql)
    ifdb_patient_ids = {str(row["PATIENT_ID"]) for row in ifdb_rows if row.get("PATIENT_ID")}

    if ifdb_patient_ids:
        resolve_missing_patients(
            patient_ids=ifdb_patient_ids,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )

        # Re-resolve CPRs after the IFDB Patient pipeline has run; the IFDB Patient
        # pipeline attaches SYSTEM_CPR as a secondary identifier on every resource it
        # creates, so this picks up everything we just routed through it.
        resolved_by_cpr = batch_resolve_references(
            identifiers=cpr_identifiers,
            resource_type="Patient",
            identifier_system=SYSTEM_CPR,
            fhir_database=fhir_database,
            force_reload=True,
        )
        missing_cprs = [cpr for cpr in cpr_list if cpr not in resolved_by_cpr]

    if not missing_cprs:
        logger.info(
            f"[DEPENDENCY] CPR resolver: all CPRs resolved via IFDB "
            f"(routed {len(ifdb_patient_ids)} through IFDB Patient pipeline)"
        )
        return resolved_by_cpr

    # Final fallback — patients that exist in BOM_IFDB.PATIENTINFO but not in IFDB.
    # Post minimal Patient resources keyed by CPR; downstream consumers can enrich
    # them later when the corresponding IFDB row appears.
    if ifdb_oracle is None:
        logger.warning(
            f"[DEPENDENCY] CPR resolver: {len(missing_cprs)} CPRs not in the archive and "
            "no Oracle resource provided — skipping BOM_IFDB.PATIENTINFO fallback"
        )
        return resolved_by_cpr

    logger.info(
        f"[DEPENDENCY] CPR resolver: {len(missing_cprs)} CPRs not in IFDB — "
        "creating minimal Patients from BOM_IFDB.PATIENTINFO"
    )

    patientinfo_rows = _fetch_bom_ifdb_patientinfo_by_cpr(
        missing_cprs, ifdb_oracle, context, mssql=mssql
    )
    if not patientinfo_rows:
        logger.warning(
            f"[DEPENDENCY] CPR resolver: could not fetch any of {len(missing_cprs)} "
            "missing CPRs from BOM_IFDB.PATIENTINFO either — Appointments referencing "
            "these CPRs will fail reference resolution"
        )
        return resolved_by_cpr

    minimal_patients = _build_minimal_patients_from_patientinfo(patientinfo_rows)
    if minimal_patients:
        post_resources_direct(
            context=context,
            resources=minimal_patients,
            fhir_database=fhir_database,
            resource_type="Patient",
            source_name="bom_ifdb_cpr_resolver",
            extracted_count=len(patientinfo_rows),
        )
        fhir_database.mark_stale("Patient")
        fhir_database.safe_clear_identifier_caches_for_resources(
            {"Patient"}, reason="CPR-based Patient posted by dependency resolver"
        )

        resolved_by_cpr = batch_resolve_references(
            identifiers=cpr_identifiers,
            resource_type="Patient",
            identifier_system=SYSTEM_CPR,
            fhir_database=fhir_database,
            force_reload=True,
        )

    return resolved_by_cpr


def _fetch_ifdb_patients_by_cpr(
    cprs: list[str],
    mssql: MSSQLResource,
) -> list[dict[str, Any]]:
    """Fetch archived IFDB patient rows by CPR from arc.ifdb_patient."""
    from DSI.oracle.ifdb_extract import PATIENT_COLUMNS

    # include_deleted=True: arc.ifdb_patient soft-deletes via SLETTET, and the
    # transform tombstones such rows — filtering them out here would make
    # required references unresolvable.
    return chunked_in_fetch(
        cprs,
        table="arc.ifdb_patient",
        columns=PATIENT_COLUMNS,
        where_column="CPR",
        mssql=mssql,
        include_deleted=True,
    )


def _fetch_bom_ifdb_patientinfo_by_cpr(
    cprs: list[str],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource | None = None,
) -> list[dict[str, Any]]:
    """Fetch BOM_IFDB.PATIENTINFO rows by CPR.

    Reads arc.bom_ifdb_patientinfo when MSSQL is available and falls back to
    Oracle for the CPRs the archive did not cover.

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
        if remaining:
            context.log.info(
                f"[DEPENDENCY] CPR resolver: {len(covered)}/{len(cprs)} CPRs resolved from "
                f"arc.bom_ifdb_patientinfo; falling back to Oracle for {len(remaining)}"
            )

    if not remaining:
        return archive_rows

    oracle_rows = _fetch_rows_by_ids_from_ifdb(
        ids=remaining,
        table="BOM_IFDB.PATIENTINFO",
        columns=columns,
        where_column="CPRNR",
        ifdb_oracle=ifdb_oracle,
        context=context,
    )
    return archive_rows + oracle_rows


def _build_minimal_patients_from_patientinfo(rows: list[dict[str, Any]]) -> list[Patient]:
    """Build minimal FHIR Patient resources from BOM_IFDB.PATIENTINFO rows.

    These are bridge resources so AFTALE-driven appointments can resolve their
    patient reference even when no corresponding IFDB row exists. Demographics
    are intentionally sparse — the IFDB Patient pipeline owns the rich shape.
    """
    patients: list[Patient] = []
    for row in rows:
        cpr = row.get("CPRNR")
        if not cpr:
            continue
        identifiers = [Identifier(system=SYSTEM_CPR, value=str(cpr).strip())]
        patientinfo_oid = row.get("PATIENTINFOOID")
        if patientinfo_oid is not None:
            identifiers.append(
                Identifier(
                    system=SYSTEM_BOM_IFDB_PATIENTINFO_OID,
                    value=normalize_oracle_id(patientinfo_oid),
                )
            )

        name = None
        given = row.get("FORNAVN")
        family = row.get("EFTERNAVN")
        if given or family:
            name = [
                HumanName(
                    given=[given] if given else None,
                    family=family if family else None,
                )
            ]

        gender = None
        koen = (row.get("KOEN") or "").strip().upper()
        if koen in {"M", "MALE"}:
            gender = "male"
        elif koen in {"K", "F", "FEMALE"}:
            gender = "female"
        elif koen:
            gender = "unknown"

        patients.append(
            Patient(
                identifier=identifiers,
                name=name,
                gender=gender,
            )
        )
    return patients


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
        resolve_missing_patients(
            patient_ids=patient_ids,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
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
        _fetch_location_data_for_batch,
        _post_encounters_to_fhir,
        _transform_encounters_to_fhir,
    )

    locations_by_encounter = _fetch_location_data_for_batch(missing_encounter_ids, mssql)
    encounters = _transform_encounters_to_fhir(
        missing_encounter_rows,
        locations_by_encounter,
        context,
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


def _fetch_patients_from_archive(
    patient_ids: list[str],
    mssql: MSSQLResource,
) -> list[dict[str, Any]]:
    """Fetch archived patient records from arc.ifdb_patient by their IDs."""
    from DSI.oracle.ifdb_extract import PATIENT_COLUMNS

    # include_deleted=True: SLETTET-soft-deleted rows must reach the transform,
    # which tombstones them — required references stay resolvable.
    return chunked_in_fetch(
        patient_ids,
        table="arc.ifdb_patient",
        columns=PATIENT_COLUMNS,
        where_column="PATIENT_ID",
        mssql=mssql,
        include_deleted=True,
    )


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
    return resolve_missing_patients(
        patient_ids=patient_ids,
        mssql=mssql,
        fhir_database=fhir_database,
        context=context,
    )


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
    resolved = resolve_missing_patients(
        patient_ids=patient_ids,
        mssql=mssql,
        fhir_database=fhir_database,
        context=context,
    )
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
