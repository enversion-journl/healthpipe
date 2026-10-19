# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: ILSBruger

Assign dw_ek_ILSBruger to ILS user initials via IDENTITY.
Reads the CleanCodes from stg.dim_ILSBruger and INSERTs the ones map.ILSBruger
does not hold yet (insert-only).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
    INSERT INTO [map].[ILSBruger] (
        clean_code_ILSBruger,
        dw_timestamp_insert,
        dw_timestamp_update
    )
    SELECT
        source.clean_code_ILSBruger,
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    FROM [stg].[dim_ILSBruger] source
    WHERE source.clean_code_ILSBruger IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM [map].[ILSBruger] m
          WHERE m.clean_code_ILSBruger = source.clean_code_ILSBruger
      );
"""


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_dim_ILSBruger"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_ILSBruger via IDENTITY, INSERT new CleanCodes from stg.dim_ILSBruger",
)
def map_ILSBruger(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table: Insert new ILS users into map.ILSBruger."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("map", "ILSBruger", dagster_run_id=context.run_id, mode=config.mode)

    new_count = 0
    total_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(INSERT_SQL)
                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[ILSBruger]")
                total_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"map.ILSBruger: {new_count} inserted, {total_count} total")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total": total_count,
            "table": "map.ILSBruger",
            "pattern": "INSERT (insert-only)",
            "mode": config.mode,
        }
    )
