# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pre-Staging: lgrbeh from arc.ils_lgrbeh
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

from etl.DSA.SQL.common.dato_dimension import dato_key


@dg.asset(
    key=dg.AssetKey(["erp", "pre_stg_Lagerbeholdning_ils"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    deps=[dg.AssetKey(["arc_ils_lgrbeh"])],
    tags={"domain": "erp", "layer": "pre_stg", "source": "ils", "entity": "lgrbeh"},
    description="Pre-staging for ILS warehouse stock from arc.ils_lgrbeh.",
)
def pre_stg_Lagerbeholdning_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)

    dim_cp = dfm.get_checkpoint("dim", "LagerbeholdningEgenskaber")
    fct_cp = dfm.get_checkpoint("fct", "Lagerbeholdning")
    checkpoints = [cp for cp in [dim_cp, fct_cp] if cp is not None]
    min_checkpoint = min(checkpoints) if checkpoints else None

    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_fct_Lagerbeholdning_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        where_clause = ""
        if min_checkpoint is not None:
            where_clause = f"WHERE src.dw_timestamp_update > '{min_checkpoint}'"
            context.log.info(f"Checkpoint filter: dw_timestamp_update > {min_checkpoint}")
        else:
            context.log.info("No checkpoint found — loading all rows from archive")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_fct_Lagerbeholdning_ils];")

                cursor.execute(
                    f"""
                    WITH ranked AS (
                        SELECT
                            src.*,
                            CASE
                                WHEN src.sgh IS NOT NULL AND src.lgr IS NOT NULL
                                THEN 'SGH_' + LTRIM(RTRIM(REPLACE(CAST(src.sgh AS NVARCHAR(50)), ' ', '')))
                                     + '_LGR_'
                                     + LTRIM(RTRIM(REPLACE(CAST(src.lgr AS NVARCHAR(50)), ' ', '')))
                                WHEN src.sgh IS NOT NULL
                                THEN 'SGH_' + LTRIM(RTRIM(REPLACE(CAST(src.sgh AS NVARCHAR(50)), ' ', '')))
                                WHEN src.lgr IS NOT NULL
                                THEN 'LGR_' + LTRIM(RTRIM(REPLACE(CAST(src.lgr AS NVARCHAR(50)), ' ', '')))
                                ELSE NULL
                            END AS clean_code_Lager,
                            src.dw_timestamp_update AS snapshot_date_time,
                            {dato_key('src.dw_timestamp_update')} AS snapshot_datekey,
                            DENSE_RANK() OVER (
                                PARTITION BY CAST(src.dw_timestamp_update AS DATE)
                                ORDER BY src.dw_timestamp_update DESC
                            ) AS dr
                        FROM [arc].[ils_lgrbeh] src
                        {where_clause}
                    )
                    SELECT *
                    INTO [stg].[pre_fct_Lagerbeholdning_ils]
                    FROM ranked
                    WHERE dr = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.pre_fct_Lagerbeholdning_ils with {row_count} rows")

                if row_count > 0:
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_stg_pre_Lagerbeh_Lager
                        ON [stg].[pre_fct_Lagerbeholdning_ils](clean_code_Lager);
                    """
                    )

                conn.commit()
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.pre_fct_Lagerbeholdning_ils",
            "row_count": row_count,
            "source_table": "arc.ils_lgrbeh",
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "mode": config.mode,
        }
    )
