# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: ILSLeverandoer

Assign dw_ek_ILSLeverandoer via IDENTITY using clean_code_ILSLeverandoer as natural key.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "stg_dim_ILSLeverandoer"])],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_ILSLeverandoer via IDENTITY using clean_code_ILSLeverandoer as natural key",
)
def map_ILSLeverandoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new clean_code_ILSLeverandoer values into map.ILSLeverandoer."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map",
        "ILSLeverandoer",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Starting ILSLeverandoer map update from stg.dim_ILSLeverandoer..."
                )

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_ILSLeverandoer]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"stg.dim_ILSLeverandoer has {stg_count} rows")

                cursor.execute(
                    """
                INSERT INTO [map].[ILSLeverandoer] (
                    clean_code_ILSLeverandoer,
                    dw_timestamp_insert
                )
                SELECT DISTINCT
                    s.clean_code_ILSLeverandoer,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[dim_ILSLeverandoer] s
                WHERE s.clean_code_ILSLeverandoer IS NOT NULL
                  AND s.clean_code_ILSLeverandoer <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [map].[ILSLeverandoer] m
                      WHERE m.clean_code_ILSLeverandoer = s.clean_code_ILSLeverandoer
                  );
            """
                )

                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[ILSLeverandoer]")
                total_count = cursor.fetchone()[0]

                context.log.info(
                    f"Map update complete: {new_count} new, {total_count} total ILSLeverandoer entries"
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
            "table": "map.ILSLeverandoer",
            "natural_key": "clean_code_ILSLeverandoer",
            "source_table": "stg.dim_ILSLeverandoer",
            "mode": config.mode,
        }
    )
