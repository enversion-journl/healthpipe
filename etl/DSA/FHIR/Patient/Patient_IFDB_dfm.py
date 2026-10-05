# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Patient asset using DFM (Data Flow Management).

Single-asset pattern: fetches from arc.ifdb_patient in batches, transforms to
FHIR, and posts to the FHIR server — all within one asset to keep memory bounded.

This module also owns the shared Patient transform/post helpers (used by the
dependency resolver), which lived in Patient_IFDB.py before the partitioned
assets were removed.

No Dagster partitions — checkpoint tracking replaces partition boundaries.
"""

from datetime import UTC, datetime

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb
from fhir.resources.R4B.patient import Patient
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

BATCH_SIZE = 10_000


# ============================================================================
# Shared Helper Functions
# ============================================================================


def _dedup_rows_by_cpr(rows: list[dict]) -> list[dict]:
    """For rows sharing the same CPR, keep only the best candidate.

    Selection priority:
      1. Non-deleted (SLETTET = '0' / NULL) beats deleted (SLETTET = '1')
      2. Most recently updated (OPDATERET_TID descending) as tiebreaker

    Rows without a CPR value are always kept (no dedup possible).
    O(n) single-pass — no sorting required.
    """
    best_by_cpr: dict[str, dict] = {}
    no_cpr: list[dict] = []

    for row in rows:
        cpr = row.get("CPR") or row.get("cpr")
        if not cpr:
            no_cpr.append(row)
            continue

        cpr_str = str(cpr).strip()
        existing = best_by_cpr.get(cpr_str)
        if existing is None:
            best_by_cpr[cpr_str] = row
            continue

        new_deleted = _is_row_deleted(row)
        old_deleted = _is_row_deleted(existing)

        if old_deleted and not new_deleted:
            best_by_cpr[cpr_str] = row
        elif old_deleted == new_deleted:
            new_ts = str(row.get("OPDATERET_TID") or row.get("opdateret_tid") or "")
            old_ts = str(existing.get("OPDATERET_TID") or existing.get("opdateret_tid") or "")
            if new_ts > old_ts:
                best_by_cpr[cpr_str] = row

    return list(best_by_cpr.values()) + no_cpr


def _is_row_deleted(row: dict) -> bool:
    """Check if a patient row is marked as deleted (SLETTET != 0)."""
    slettet = row.get("SLETTET") or row.get("slettet")
    if slettet is None:
        return False
    return str(slettet).strip() not in ("0", "0.0")


def _transform_patient_rows_to_fhir(
    patient_rows: list[dict],
    context: dg.AssetExecutionContext,
) -> list[Patient]:
    """
    Transform IFDB patient rows into FHIR Patient resources.

    Pre-deduplicates rows by CPR (preferring non-deleted, then most recently
    updated) *before* the FHIR transform to avoid wasting transform cycles.

    Args:
        patient_rows: List of patient row dictionaries from IFDB extraction
        context: Dagster execution context

    Returns:
        List of unique Patient resources
    """
    deduped_rows = _dedup_rows_by_cpr(patient_rows)
    if len(deduped_rows) < len(patient_rows):
        context.log.info(
            f"CPR dedup: {len(patient_rows)} rows → {len(deduped_rows)} "
            f"({len(patient_rows) - len(deduped_rows)} duplicate CPR rows removed)"
        )

    patients = transform_fhir_patient_ifdb(
        context=context,
        patient_rows=deduped_rows,
    )

    # Remove duplicates based on IFDB patient identifier (belt-and-suspenders)
    seen_ids: set[str] = set()
    unique_patients: list[Patient] = []
    for patient in patients:
        patient_id = None
        if patient.identifier:
            for identifier in patient.identifier:
                if identifier.system == SYSTEM_IFDB_PATIENT_ID:
                    patient_id = identifier.value
                    break
        if patient_id and patient_id not in seen_ids:
            seen_ids.add(patient_id)
            unique_patients.append(patient)
        elif not patient_id:
            unique_patients.append(patient)

    return unique_patients


def _post_patients_to_fhir(
    context: dg.AssetExecutionContext,
    patients: list[Patient],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    source_name: str = "ifdb_oracle",
) -> dg.MaterializeResult:
    """
    Post Patient resources to FHIR database using direct DB insertion.

    Uses executemany() for 4-7x faster performance than FHIR API.
    Resolves existing FHIR IDs before posting to ensure upsert behavior
    (updates existing patients instead of creating duplicates).

    Args:
        context: Dagster execution context
        patients: List of Patient resources to post
        fhir_database: FHIR database resource for direct insertion
        extracted_count: Number of records extracted (for logging)
        source_name: Name of the data source for metadata

    Returns:
        MaterializeResult with posting statistics
    """
    partition_key = context.partition_key if context.has_partition_key else "N/A"
    transformed_count = len(patients)

    if not patients:
        context.log.info(
            f"Summary[{partition_key}]: {extracted_count} ext -> 0 trans -> 0 posted (0 failed)"
        )
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(0),
                "failed_count": dg.MetadataValue.int(0),
                "resolved_count": dg.MetadataValue.int(0),
                "new_count": dg.MetadataValue.int(0),
                "source": dg.MetadataValue.text(source_name),
                "partition_key": dg.MetadataValue.text(partition_key),
                "method": dg.MetadataValue.text("direct_postgres"),
            }
        )

    when = datetime.now(UTC)

    # Resolve existing FHIR IDs before posting to enable proper upsert behavior.
    # This ensures that patients already in the database get their existing UUID
    # attached, so ON CONFLICT (id) triggers an UPDATE instead of INSERT.
    # Use IFDB patient ID as primary identifier, with CPR as fallback.
    patients_before_resolve = len([p for p in patients if p.id is not None])
    patients = prepare_resources_for_upsert(
        resources=patients,
        fhir_database=fhir_database,
        resource_type="Patient",
        preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR],
        logger=context.log,
    )
    patients_after_resolve = len([p for p in patients if p.id is not None])
    resolved_count = patients_after_resolve - patients_before_resolve
    new_count = len(patients) - patients_after_resolve

    context.log.info(
        f"ID resolution: {resolved_count} existing patients found, {new_count} new patients"
    )

    # Use batch_upsert_resources which uses executemany() internally
    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=patients,
        resource_type="Patient",
    )

    # Strict mode: a resource that could not be persisted must NOT be silently
    # left behind the checkpoint. Raising aborts before checkpoint advancement
    # so the window is reprocessed next run (idempotent upsert). Consistent with
    # Encounter (post_resources_direct) and Location.
    if failures > 0:
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {transformed_count} Patient "
                f"resources — checkpoint NOT advanced. Failed IDs: {failed_ids[:10]}"
            )
        )

    # Log concise summary
    context.log.info(
        f"Summary[{partition_key}]: {extracted_count} ext -> {transformed_count} trans -> {successes} posted ({failures} failed) [{resolved_count} updated, {new_count} new]"
    )

    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(successes),
            "failed_count": dg.MetadataValue.int(failures),
            "failed_ids": dg.MetadataValue.json(failed_ids[:100]),
            "resolved_count": dg.MetadataValue.int(resolved_count),
            "new_count": dg.MetadataValue.int(new_count),
            "partition_key": dg.MetadataValue.text(partition_key),
            "source": dg.MetadataValue.text(source_name),
            "method": dg.MetadataValue.text("direct_postgres"),
            "decorated_at": dg.MetadataValue.text(when.isoformat()),
        }
    )


def _build_patient_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Build a CPR-deduplicated query against arc.ifdb_patient.

    Picks one row per CPR via ROW_NUMBER():
      1. Non-deleted (SLETTET='0' or NULL) preferred over deleted
      2. Most recently updated (dw_timestamp_update DESC) as tiebreaker

    Crucially, dedup ranks over the **whole archive** for every CPR that appears
    in the run window — NOT just the windowed rows. Consider patient P1 (CPR X,
    active, posted long ago, outside the delta window) and a duplicate P2 (same
    CPR, SLETTET=1, touched now). If we ranked only the window, P2 would be the
    sole row for CPR X and "win"; the CPR fallback in prepare_resources_for_upsert
    would then resolve to P1's existing FHIR resource and overwrite the active
    patient with isDeleted=1. Ranking across the whole archive lets the active P1
    win (matching full_load behaviour), so the active patient is never clobbered
    by a touched-but-deleted duplicate.

    Patients with no CPR are always included from the window (cannot dedup
    without a CPR, and they can't collide via the CPR fallback). A CPR with a
    single archive row is included regardless of delete status — so a genuinely
    deleted patient with no active replacement still gets the isDeleted
    extension posted.

    Requires IX_arc_ifdb_patient_CPR for the windowed-CPR → archive join to be a
    seek (see infrastructure/ddl/tables/ifdb_archive/01_patient_eoc.sql).
    """
    # CPRs touched in this run window (mode-aware: delta / period / ids /
    # full_load), including deleted rows.
    windowed_cpr = build_fhir_archive_query(
        "arc.ifdb_patient",
        checkpoint,
        config,
        include_deleted=True,
        select_clause="SELECT DISTINCT [CPR]",
    )
    windowed_cpr += " AND [CPR] IS NOT NULL AND [CPR] != ''"

    # Windowed rows without a CPR (resolved by PATIENT_ID only, never deduped).
    windowed_rows = build_fhir_archive_query(
        "arc.ifdb_patient", checkpoint, config, include_deleted=True
    )

    return f"""
        WITH windowed_cpr AS (
            {windowed_cpr}
        ),
        ranked AS (
            SELECT a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.[CPR]
                    ORDER BY
                        CASE WHEN ISNULL(a.[SLETTET], '0') IN ('0', '0.0') THEN 0 ELSE 1 END,
                        a.[dw_timestamp_update] DESC
                ) AS _rn
            FROM [arc].[ifdb_patient] a
            INNER JOIN windowed_cpr w ON w.[CPR] = a.[CPR]
        )
        SELECT * FROM ranked WHERE _rn = 1
        UNION ALL
        SELECT *, 1 AS _rn FROM (
            {windowed_rows}
        ) nc
        WHERE nc.[CPR] IS NULL OR nc.[CPR] = ''
    """


# ---------------------------------------------------------------------------
# Single DFM Asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_patient_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=["arc_patient_ifdb"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch patients from arc.ifdb_patient in batches, transform to FHIR, "
        "and post to FHIR server. Memory-bounded streaming — never loads full "
        "archive into memory. Checkpoint owner for fhir_server.Patient."
    ),
)
def fhir_server_patient_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for Patient resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Patient",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = _build_patient_query(run_ctx.checkpoint, config)
        context.log.info(f"Querying arc.ifdb_patient (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]

                while True:
                    raw_rows = cursor.fetchmany(BATCH_SIZE)
                    if not raw_rows:
                        break

                    batch_num += 1
                    batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
                    total_fetched += len(batch)

                    context.log.info(
                        f"Batch {batch_num}: processing {len(batch)} patients "
                        f"(total so far: {total_fetched})"
                    )

                    patients = _transform_patient_rows_to_fhir(batch, context)

                    if patients:
                        _post_patients_to_fhir(
                            context=context,
                            patients=patients,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                            source_name="ifdb_archive_dfm",
                        )
                        total_posted += len(patients)

            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted " f"in {batch_num} batches"
        )

        new_checkpoint: str | None = None
        if config.should_advance_checkpoint and total_fetched > 0:
            with mssql_target_staging.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                    row = cursor.fetchone()
                    new_checkpoint = row[0] if row else None
                finally:
                    cursor.close()

            if new_checkpoint:
                dfm.set_checkpoint("fhir_server", "Patient", value=new_checkpoint, mode=config.mode)

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "mode": config.mode,
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
