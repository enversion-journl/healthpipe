# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Lager

Assign dw_ek_Lager to Lager codes via IDENTITY.
Reads from stg.dim_Lager, INSERT into map.Lager on clean_code_Lager when not present.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["stg_dim_Lager"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_Lager via IDENTITY, INSERT from stg.dim_Lager into map.Lager",
)
def map_Lager(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new Lager codes into map.Lager."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("map", "Lager", dagster_run_id=context.run_id, mode=config.mode)

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Lager]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"Staging has {stg_count} Lager entries")

                cursor.execute(
                    """
                INSERT INTO [map].[Lager] (
                    clean_code_Lager,
                    bk_code,
                    dw_timestamp_insert,
                    dw_timestamp_update
                )
                SELECT
                    source.clean_code_Lager,
                    source.code,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[dim_Lager] source
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [map].[Lager] m
                    WHERE m.clean_code_Lager = source.clean_code_Lager
                );
            """
                )

                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[Lager]")
                total_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"map.Lager: {new_count} inserted, {total_count} total")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total": total_count,
            "table": "map.Lager",
            "pattern": "INSERT (insert-only)",
            "mode": config.mode,
        }
    )
