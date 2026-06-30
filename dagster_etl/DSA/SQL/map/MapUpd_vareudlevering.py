# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Vareudlevering

Assign DW_EK_Vareudlevering to Vareudlevering via IDENTITY.
Uses the dimension staging table: stg.dim_vareudlevering_ils
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="maps_vareudlevering",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_vareudlevering_ils"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_Vareudlevering to Vareudlevering via IDENTITY using CleanCode_Vareudlevering",
)
def map_vareudlevering(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new Vareudlevering entries into map.Vareudlevering."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map",
        "Vareudlevering",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Vareudlevering map update...")

                cursor.execute(
                    "SELECT COUNT(DISTINCT CleanCode_Vareudlevering) FROM [stg].[dim_vareudlevering_ils] "
                    "WHERE CleanCode_Vareudlevering IS NOT NULL"
                )
                source_count = cursor.fetchone()[0]
                context.log.info(f"Staging source count: {source_count}")

                cursor.execute(
                    """
                INSERT INTO [map].[Vareudlevering] (
                    CleanCode_Vareudlevering,
                    [bk_prrowid],
                    DW_Timestamp_Insert
                )
                SELECT
                    CleanCode_Vareudlevering,
                    CAST(prrowid AS NVARCHAR(100)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[dim_vareudlevering_ils] src
                WHERE src.CleanCode_Vareudlevering IS NOT NULL
                  AND src.CleanCode_Vareudlevering <> ''
                  AND NOT EXISTS (
                    SELECT 1
                    FROM [map].[Vareudlevering] m
                    WHERE m.CleanCode_Vareudlevering = src.CleanCode_Vareudlevering
                );
            """
                )

                new_count = cursor.rowcount
                context.log.info(f"Inserted {new_count} new Vareudlevering entries")

                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM map.Vareudlevering")
                total_count = cursor.fetchone()[0]

                context.log.info(
                    f"Map update complete: {new_count} new, {total_count} total entries"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Vareudlevering",
            "natural_key": "CleanCode_Vareudlevering",
            "source_table": "stg.dim_vareudlevering_ils",
            "mode": config.mode,
        }
    )
