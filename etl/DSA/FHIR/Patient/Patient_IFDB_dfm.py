# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Patient asset using DFM (Data Flow Management).

Fetches arc.ifdb_patient in batches and writes each batch through the Patient
entry point (patient_service.write_rows), which resolves identities, merges the
EPJ and register contributions in patient_builder and posts once. No Dagster
partitions — checkpoint tracking replaces partition boundaries.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Patient.patient_service import write_rows
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS

BATCH_SIZE = 10_000


# ============================================================================
# Shared Helper Functions
# ============================================================================


def _build_patient_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Build a CPR-deduplicated query against arc.ifdb_patient.

    Picks one row per CPR via ROW_NUMBER():
      1. Non-deleted (SLETTET='0' or NULL) preferred over deleted
      2. Most recently updated (OPDATERET_TID DESC, then PATIENT_ID ASC) as tiebreaker —
         the same rule as patient_identity.order_rows

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
                        a.[OPDATERET_TID] DESC,
                        a.[PATIENT_ID] ASC
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
    deps=["arc_patient_ifdb", "ref_patient_identity"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch patients from arc.ifdb_patient in batches, build FHIR Patient "
        "via patient_builder (EPJ and CPR register contributions) and post to FHIR server. "
        "Memory-bounded streaming — never loads full archive into memory. "
        "Checkpoint owner for fhir_server.Patient."
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
        total_withheld = 0
        total_uninterpreted = 0
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

                    written = write_rows(
                        context,
                        mssql_target_staging,
                        fhir_database,
                        batch,
                        source_name="ifdb_archive_dfm",
                    )
                    total_posted += len(written)
                    total_withheld += getattr(written, "withheld", 0)
                    total_uninterpreted += getattr(written, "uninterpreted", 0)

            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted " f"in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(dfm, run_ctx, "Patient", config, total_fetched)

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "withheld_count": total_withheld,
                "uninterpreted_count": total_uninterpreted,
                "batch_count": batch_num,
                "mode": config.mode,
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
