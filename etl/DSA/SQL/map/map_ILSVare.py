# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: ILSVare

Assign dw_ek_ILSVare to ILS item numbers via IDENTITY.
Reads distinct CleanCodes from stg.dim_ILSVare (delta), INSERT into map.ILSVare
on clean_code when not present.
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
        AssetKey(["erp", "stg_dim_ILSVare"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_ILSVare via IDENTITY, INSERT distinct CleanCodes from stg.dim_ILSVare",
)
def map_ILSVare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table: Insert new ILS item numbers into map.ILSVare."""

    dfm = DfmManager(mssql_target_staging, context)

    run_ctx = dfm.start_dfm_run(
        "map",
        "ILSVare",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT COUNT(DISTINCT clean_code_ILSVare) FROM [stg].[dim_ILSVare] "
                    "WHERE clean_code_ILSVare IS NOT NULL AND clean_code_ILSVare <> ''"
                )
                stg_count = cursor.fetchone()[0]
                context.log.info(f"Staging has {stg_count} distinct ILSVare CleanCodes")

                cursor.execute(
                    """
                    INSERT INTO [map].[ILSVare] (
                        clean_code_ILSVare,
                        dw_timestamp_insert,
                        dw_timestamp_update
                    )
                    SELECT DISTINCT
                        source.clean_code_ILSVare,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    FROM [stg].[dim_ILSVare] source
                    WHERE source.clean_code_ILSVare IS NOT NULL
                      AND source.clean_code_ILSVare <> ''
                      AND NOT EXISTS (
                          SELECT 1
                          FROM [map].[ILSVare] m
                          WHERE m.clean_code_ILSVare = source.clean_code_ILSVare
                      );
                """
                )

                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[ILSVare]")
                total_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"map.ILSVare: {new_count} inserted, {total_count} total")

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total": total_count,
            "table": "map.ILSVare",
            "pattern": "INSERT (insert-only)",
            "mode": config.mode,
        }
    )
