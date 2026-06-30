# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: LagerbeholdningEgenskaber

Business logic identical to stg_dim_LagerbeholdningEgenskaber_ils.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    key=dg.AssetKey(["erp", "stg_dim_LagerbeholdningEgenskaber_ils"]),
    group_name="dimensions_lagerbeholdning",
    compute_kind="mssql",
    deps=[dg.AssetKey(["erp", "pre_stg_Lagerbeholdning_ils"])],
    tags={"domain": "erp", "layer": "stg_dim", "entity": "LagerbeholdningEgenskaber"},
    description="Distinct attribute combinations for dim.LagerbeholdningEgenskaber.",
)
def stg_dim_LagerbeholdningEgenskaber_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_LagerbeholdningEgenskaber_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_LagerbeholdningEgenskaber_ils];")

                cursor.execute(
                    """
                    SELECT
                        NULLIF(LTRIM(RTRIM(lgrlok)), '') AS Lgrlok,
                        NULLIF(LTRIM(RTRIM(udlevenh)), '') AS Udlevenh,
                        sw_erstat AS Sw_erstat,
                        lgrbeh_prio AS Lgrbeh_prio,
                        MAX(CASE WHEN LTRIM(RTRIM(lnr)) <> '' THEN UPPER(LTRIM(RTRIM(lnr))) END) AS Lnr,
                        CONCAT_WS('_',
                            ISNULL(NULLIF(LTRIM(RTRIM(lgrlok)), ''), ''),
                            ISNULL(NULLIF(LTRIM(RTRIM(udlevenh)), ''), ''),
                            ISNULL(CAST(sw_erstat AS NVARCHAR(1)), ''),
                            ISNULL(CAST(lgrbeh_prio AS NVARCHAR(10)), '')
                        ) AS CleanCode_LagerbeholdningEgenskaber
                    INTO [stg].[dim_LagerbeholdningEgenskaber_ils]
                    FROM [stg].[pre_Lagerbeholdning_ils]
                    GROUP BY
                        NULLIF(LTRIM(RTRIM(lgrlok)), ''),
                        NULLIF(LTRIM(RTRIM(udlevenh)), ''),
                        sw_erstat,
                        lgrbeh_prio;
                """
                )

                row_count = cursor.rowcount

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_dim_lagerbeh_egenskaber_CC                    ON [stg].[dim_LagerbeholdningEgenskaber_ils](
                        CleanCode_LagerbeholdningEgenskaber
                    );
                """
                )

                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"stg.dim_LagerbeholdningEgenskaber_ils: {row_count} rows")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.dim_LagerbeholdningEgenskaber_ils",
            "row_count": row_count,
        }
    )
