# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: ean

Assign dw_ek_EAN to ean codes via IDENTITY.
Reads from stg.dim_EAN, INSERT into map.ean on clean_code_EAN when not present.
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
        AssetKey(["stg_dim_EAN"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_EAN via IDENTITY, INSERT from stg.dim_EAN into map.ean",
)
def map_EAN(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new ean codes into map.ean."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("map", "ean", dagster_run_id=context.run_id, mode=config.mode)

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_EAN]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"Staging has {stg_count} ean entries")

                cursor.execute(
                    """
                INSERT INTO [map].[ean] (
                    clean_code_EAN,
                    dw_timestamp_insert,
                    dw_timestamp_update
                )
                SELECT
                    source.clean_code_EAN,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[dim_EAN] source
                WHERE source.clean_code_EAN IS NOT NULL
                  AND source.clean_code_EAN <> ''
                  AND NOT EXISTS (
                    SELECT 1
                    FROM [map].[ean] m
                    WHERE m.clean_code_EAN = source.clean_code_EAN
                );
            """
                )

                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[ean]")
                total_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"map.ean: {new_count} inserted, {total_count} total")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.ean",
            "pattern": "INSERT (insert-only)",
        }
    )
