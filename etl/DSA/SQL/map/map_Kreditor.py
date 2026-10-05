# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Kreditor

Assign dw_ek_Kreditor via IDENTITY using clean_code_kreditor as natural key,
and maintain sticky dw_source_<system> presence flags.

MERGE from stg.dim_Kreditor (the DELTA of changed CVRs + per-source flags,
extracted from the archives on the dim.Kreditor checkpoint):
- New CVRs are inserted (IDENTITY assigns dw_ek_Kreditor).
- Flags are sticky: once a CVR has been seen in a source, the flag stays 1.
- Every staged (i.e. changed) CVR gets dw_timestamp_update stamped — so
  map.Kreditor.dw_timestamp_update is the complete change signal (new CVRs,
  flag flips AND attribute drift) for the delta-driven dim_Kreditor.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

MERGE_SQL = """
    MERGE [map].[Kreditor] AS tgt
    USING [stg].[dim_Kreditor] AS src
        ON tgt.clean_code_kreditor = LTRIM(RTRIM(src.clean_code_Kreditor))

    -- src is the delta: presence in staging means the CVR changed in a
    -- source archive, so every matched row is stamped (dim delta signal).
    WHEN MATCHED THEN
        UPDATE SET
            -- Sticky: never flip 1 -> 0 (a CVR whose change came from one
            -- source keeps its presence flags for the other sources)
            tgt.dw_source_sydax = CASE WHEN src.dw_source_sydax = 1 THEN 1 ELSE tgt.dw_source_sydax END,
            tgt.dw_source_ils = CASE WHEN src.dw_source_ils = 1 THEN 1 ELSE tgt.dw_source_ils END,
            tgt.dw_source_mysupply = CASE WHEN src.dw_source_mysupply = 1 THEN 1 ELSE tgt.dw_source_mysupply END,
            tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            clean_code_kreditor,
            dw_source_sydax, dw_source_ils, dw_source_mysupply,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (
            LTRIM(RTRIM(src.clean_code_Kreditor)),
            src.dw_source_sydax, src.dw_source_ils, src.dw_source_mysupply,
            CAST(SYSDATETIME() AS datetime2(0)),
            CAST(SYSDATETIME() AS datetime2(0))
        );
"""


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_dim_Kreditor"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign dw_ek_Kreditor via IDENTITY and maintain sticky dw_source_<system> flags; "
        "stamps dw_timestamp_update for every staged (changed) CVR (delta signal for dim_Kreditor)"
    ),
)
def map_Kreditor(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: MERGE unified Kreditor key set into map.Kreditor."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("map", "Kreditor", dagster_run_id=context.run_id, mode=config.mode)

    affected_count = 0
    total_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Kreditor map MERGE from stg.dim_Kreditor...")

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Kreditor]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging has {staging_count} changed CVRs")

                cursor.execute(MERGE_SQL)
                affected_count = cursor.rowcount
                conn.commit()

                context.log.info(f"MERGE complete: {affected_count} rows inserted or stamped")

                cursor.execute("SELECT COUNT(*) FROM [map].[Kreditor]")
                total_count = cursor.fetchone()[0]

                context.log.info(f"Map update complete: {total_count} total Kreditor entries")
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=affected_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_inserted_or_stamped": affected_count,
            "total_rows": total_count,
            "table": "map.Kreditor",
            "natural_key": "clean_code_kreditor",
            "source_tables": "stg.dim_Kreditor (delta)",
            "pattern": "MERGE (insert + sticky flags; timestamp stamped for every staged CVR)",
            "clean_architecture_note": "dw_ek_Kreditor assigned via IDENTITY - source of truth",
            "mode": config.mode,
        }
    )
