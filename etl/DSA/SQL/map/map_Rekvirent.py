# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Rekvirent

Assign dw_ek_Rekvirent to Rekvirent codes via IDENTITY.
Reads from stg.dim_Rekvirent, INSERT-only into map.Rekvirent on clean_code_Rekvirent.
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
        AssetKey(["stg_dim_Rekvirent"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "rekvirent"},
    description="Assign dw_ek_Rekvirent via IDENTITY, INSERT from stg.dim_Rekvirent into map.Rekvirent",
)
def map_Rekvirent(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new Rekvirent clean codes into map.Rekvirent."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("map", "Rekvirent", dagster_run_id=context.run_id, mode=config.mode)

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Rekvirent]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"Staging has {stg_count} Rekvirent entries")

                cursor.execute(
                    """
                INSERT INTO [map].[Rekvirent] (
                    clean_code_Rekvirent,
                    bk_code,
                    dw_timestamp_insert,
                    dw_timestamp_update
                )
                SELECT DISTINCT
                    source.clean_code_Rekvirent,
                    source.code,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[dim_Rekvirent] source
                WHERE source.clean_code_Rekvirent IS NOT NULL
                  AND source.clean_code_Rekvirent <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [map].[Rekvirent] target
                      WHERE target.clean_code_Rekvirent = source.clean_code_Rekvirent
                  );
            """
                )

                new_count = cursor.rowcount

                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[Rekvirent]")
                total_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"map.Rekvirent: {new_count} inserted, {total_count} total")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Rekvirent",
            "pattern": "INSERT (insert-only)",
            "mode": config.mode,
        }
    )
