# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: aftale

Assign dw_ek_Aftale via IDENTITY using clean_code_Aftale as natural key.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "stg_dim_Aftale_ils"])],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_Aftale via IDENTITY using clean_code_Aftale as natural key",
)
def map_Aftale(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new clean_code_Aftale values into map.Aftale."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map",
        "Aftale",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting aftale map update from stg.dim_Aftale_ils...")

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Aftale_ils]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"stg.dim_Aftale_ils has {stg_count} rows")

                cursor.execute(
                    """
                INSERT INTO [map].[Aftale] (
                    clean_code_Aftale,
                    dw_timestamp_insert
                )
                SELECT DISTINCT
                    s.clean_code_Aftale,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[dim_Aftale_ils] s
                WHERE s.clean_code_Aftale IS NOT NULL
                  AND s.clean_code_Aftale <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [map].[Aftale] m
                      WHERE m.clean_code_Aftale = s.clean_code_Aftale
                  );
            """
                )

                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[Aftale]")
                total_count = cursor.fetchone()[0]

                context.log.info(
                    f"Map update complete: {new_count} new, {total_count} total aftale entries"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Aftale",
            "natural_key": "clean_code_Aftale",
            "source_table": "stg.dim_Aftale_ils",
            "mode": config.mode,
        }
    )
