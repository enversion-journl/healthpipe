# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR Location asset (DFM): rebuilds the SOR codes changed in arc.ifdb_location."""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Location.location_service import IFDB_ARCHIVE, build_ifdb_code_query, write_codes
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id

BATCH_SIZE = 10_000


# ---------------------------------------------------------------------------
# Single DFM Asset — fetch, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_location_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=["arc_location_ifdb", "arc_bom_ifdb_orgenhed_sor"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch the SOR codes changed in arc.ifdb_location, merge each code's IFDB and BOM_IFDB "
        "contributions by the Location ownership table and post to FHIR server. "
        "Checkpoint owner for fhir_server.Location."
    ),
)
def fhir_server_location_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch of changed SOR codes → merge → post for Location resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Location",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = build_ifdb_code_query(run_ctx.checkpoint, config)
        context.log.info(f"Querying {IFDB_ARCHIVE} (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        skipped_no_sor = 0
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
                        f"Batch {batch_num}: processing {len(batch)} locations "
                        f"(total so far: {total_fetched})"
                    )

                    codes = [normalize_oracle_id(row.get("LOCATION_SORCODE")) for row in batch]
                    skipped_no_sor += sum(1 for code in codes if not code)
                    total_posted += write_codes(
                        context,
                        mssql_target_staging,
                        fhir_database,
                        [code for code in codes if code],
                        source_name="ifdb_oracle",
                    )

            finally:
                cursor.close()

        if skipped_no_sor:
            context.log.warning(
                f"Skipped {skipped_no_sor} IFDB Location rows without LOCATION_SORCODE"
            )
        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted " f"in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(dfm, run_ctx, "Location", config, total_fetched)

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
