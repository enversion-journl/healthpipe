# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pre-Staging: afdlgrbeh from arc.afdlgrbeh_ils

Business logic identical to pre_stg_afdlgrbeh_ils.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    key=dg.AssetKey(["erp", "pre_stg_AfdelingLagerbeholdning_ils"]),
    group_name="pre_staging_lagerbeholdning",
    compute_kind="mssql",
    deps=[dg.AssetKey(["arc_afdlgrbeh_ils"])],
    tags={"domain": "erp", "layer": "pre_stg", "source": "ils", "entity": "afdlgrbeh"},
    description="Pre-staging for ILS department stock from arc.afdlgrbeh_ils.",
)
def pre_stg_AfdelingLagerbeholdning_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)

    dim_cp = dfm.get_checkpoint("dim", "AfdelingLagerbeholdningEgenskaber")
    fct_cp = dfm.get_checkpoint("fct", "AfdelingLagerbeholdning")
    checkpoints = [cp for cp in [dim_cp, fct_cp] if cp is not None]
    min_checkpoint = min(checkpoints) if checkpoints else None

    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_AfdelingLagerbeholdning_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        where_clause = ""
        if min_checkpoint is not None:
            where_clause = f"WHERE src.DW_Timestamp_Update > '{min_checkpoint}'"
            context.log.info(f"Checkpoint filter: DW_Timestamp_Update > {min_checkpoint}")
        else:
            context.log.info("No checkpoint found — loading all rows from archive")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_AfdelingLagerbeholdning_ils];")

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
                            END AS Lager_CleanCode,
                            CASE
                                WHEN src.sgh IS NOT NULL AND src.rnr IS NOT NULL
                                THEN 'RNR_' + LTRIM(RTRIM(REPLACE(CAST(src.sgh AS NVARCHAR(50)), ' ', '')))
                                     + '_'
                                     + LTRIM(RTRIM(REPLACE(CAST(src.rnr AS NVARCHAR(50)), ' ', '')))
                                WHEN src.rnr IS NOT NULL
                                THEN 'RNR__' + LTRIM(RTRIM(REPLACE(CAST(src.rnr AS NVARCHAR(50)), ' ', '')))
                                ELSE NULL
                            END AS Rekvirent_CleanCode,
                            src.DW_Timestamp_Update AS Snapshot_DateTime,
                            dbo.fn_DateTime_To_DateKey(CAST(src.DW_Timestamp_Update AS DATE)) AS Snapshot_Datekey,
                            DENSE_RANK() OVER (
                                PARTITION BY CAST(src.DW_Timestamp_Update AS DATE)
                                ORDER BY src.DW_Timestamp_Update DESC
                            ) AS dr
                        FROM [arc].[afdlgrbeh_ils] src
                        {where_clause}
                    )
                    SELECT *
                    INTO [stg].[pre_AfdelingLagerbeholdning_ils]
                    FROM ranked
                    WHERE dr = 1;
                """
                )

                row_count = cursor.rowcount

                if row_count > 0:
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_stg_pre_AfdLagerbeh_Lager
                        ON [stg].[pre_AfdelingLagerbeholdning_ils](Lager_CleanCode);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_stg_pre_AfdLagerbeh_Rekvirent
                        ON [stg].[pre_AfdelingLagerbeholdning_ils](Rekvirent_CleanCode);
                    """
                    )

                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"stg.pre_AfdelingLagerbeholdning_ils: {row_count} rows")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.pre_AfdelingLagerbeholdning_ils",
            "row_count": row_count,
            "source_table": "arc.afdlgrbeh_ils",
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "mode": config.mode,
        }
    )
