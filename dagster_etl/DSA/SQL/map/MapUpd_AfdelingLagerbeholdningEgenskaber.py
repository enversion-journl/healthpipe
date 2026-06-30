# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map: AfdelingLagerbeholdningEgenskaber
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    name="map_AfdelingLagerbeholdningEgenskaber",
    group_name="dimensions_lagerbeholdning",
    compute_kind="mssql",
    deps=[dg.AssetKey(["erp", "stg_dim_AfdelingLagerbeholdningEgenskaber_ils"])],
    tags={"domain": "erp", "layer": "maps", "entity": "AfdelingLagerbeholdningEgenskaber"},
    description="Map INSERT for AfdelingLagerbeholdningEgenskaber.",
)
def map_AfdelingLagerbeholdningEgenskaber(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    if config.mode != "delta":
        context.log.warning(
            f"Map asset running with mode={config.mode!r} — map tables always process "
            f"the entire upstream staging table regardless of mode."
        )

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map",
        "AfdelingLagerbeholdningEgenskaber",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    INSERT INTO [map].[AfdelingLagerbeholdningEgenskaber] (
                        CleanCode_AfdelingLagerbeholdningEgenskaber,
                        DW_Timestamp_Insert, DW_Timestamp_Update
                    )
                    SELECT
                        source.CleanCode_AfdelingLagerbeholdningEgenskaber,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    FROM [stg].[dim_AfdelingLagerbeholdningEgenskaber_ils] source
                    WHERE NOT EXISTS (
                        SELECT 1 FROM [map].[AfdelingLagerbeholdningEgenskaber] m
                        WHERE m.CleanCode_AfdelingLagerbeholdningEgenskaber
                            = source.CleanCode_AfdelingLagerbeholdningEgenskaber
                    );
                """
                )
                new_count = cursor.rowcount
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [map].[AfdelingLagerbeholdningEgenskaber]")
                total_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total": total_count,
            "table": "map.AfdelingLagerbeholdningEgenskaber",
        }
    )
