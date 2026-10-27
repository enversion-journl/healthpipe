# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: AftaleVarePris

Assign dw_ek_AftaleVarePris via IDENTITY using clean_code_AftaleVarePris as natural key.
Grain: aftalelinje — CleanCode-format AFTALE_{lnr}_{aftale}_VARE_{vnr} — stabil paa tvaers af prisversioner.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "stg_fct_AftaleVarePris_ils"])],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_AftaleVarePris via IDENTITY using clean_code_AftaleVarePris as natural key",
)
def map_AftaleVarePris(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new clean_code_AftaleVarePris values into map.AftaleVarePris."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map",
        "AftaleVarePris",
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
                    "Starting AftaleVarePris map update from stg.fct_AftaleVarePris_ils..."
                )

                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_AftaleVarePris_ils]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"stg.fct_AftaleVarePris_ils has {stg_count} rows")

                cursor.execute(
                    """
                INSERT INTO [map].[AftaleVarePris] (
                    clean_code_AftaleVarePris,
                    dw_timestamp_insert
                )
                SELECT DISTINCT
                    s.clean_code_AftaleVarePris,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[fct_AftaleVarePris_ils] s
                WHERE s.clean_code_AftaleVarePris IS NOT NULL
                  AND s.clean_code_AftaleVarePris <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [map].[AftaleVarePris] m
                      WHERE m.clean_code_AftaleVarePris = s.clean_code_AftaleVarePris
                  );
            """
                )

                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[AftaleVarePris]")
                total_count = cursor.fetchone()[0]

                context.log.info(
                    f"Map update complete: {new_count} new, {total_count} total AftaleVarePris entries"
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
            "table": "map.AftaleVarePris",
            "natural_key": "clean_code_AftaleVarePris",
            "source_table": "stg.fct_AftaleVarePris_ils",
            "mode": config.mode,
        }
    )
