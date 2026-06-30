# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: fct_Lagerbeholdning

Business logic identical to stg_fct_lagerbeholdning_ils.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    key=dg.AssetKey(["erp", "stg_fct_Lagerbeholdning_ils"]),
    group_name="facts_lagerbeholdning",
    compute_kind="mssql",
    deps=[dg.AssetKey(["erp", "pre_stg_Lagerbeholdning_ils"])],
    tags={"domain": "erp", "layer": "stg_fct", "entity": "lagerbeholdning"},
    description="Fact staging for warehouse lagerbeholdning from pre-staging.",
)
def stg_fct_Lagerbeholdning_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "fct_Lagerbeholdning_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS [stg].[fct_Lagerbeholdning_ils];")

                cursor.execute(
                    """
                    SELECT
                        src.sgh,
                        src.lgr,
                        src.vnr,
                        NULLIF(LTRIM(RTRIM(src.lgrlok)), '') AS lgrlok,
                        SUM(src.antal) AS antal,
                        MAX(CASE WHEN LTRIM(RTRIM(src.lnr)) <> '' THEN UPPER(LTRIM(RTRIM(src.lnr))) END) AS lnr,
                        MAX(NULLIF(LTRIM(RTRIM(src.udlevenh)), '')) AS udlevenh,
                        MAX(CAST(src.sw_erstat AS INT)) AS sw_erstat,
                        MAX(src.lgrbeh_prio) AS lgrbeh_prio,
                        COALESCE(
                            MAX(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.udlobdto))), -1
                        ) AS DW_EK_Dato_Udloeb,
                        MAX(src.Lager_CleanCode) AS Lager_CleanCode,
                        CONCAT('ILSVARE_', UPPER(LTRIM(RTRIM(src.vnr)))) AS IlsVare_CleanCode,
                        CONCAT_WS('_',
                            ISNULL(NULLIF(LTRIM(RTRIM(src.lgrlok)), ''), ''),
                            ISNULL(MAX(NULLIF(LTRIM(RTRIM(src.udlevenh)), '')), ''),
                            ISNULL(CAST(MAX(CAST(src.sw_erstat AS INT)) AS NVARCHAR(1)), ''),
                            ISNULL(CAST(MAX(src.lgrbeh_prio) AS NVARCHAR(10)), '')
                        ) AS Egenskab_CleanCode,
                        src.Snapshot_Datekey AS DW_EK_Dato_Snapshot,
                        src.Snapshot_DateTime AS Datotid_Snapshot,
                        CONCAT_WS('_',
                            CAST(src.Snapshot_Datekey AS NVARCHAR(8)),
                            ISNULL(UPPER(LTRIM(RTRIM(src.sgh))), ''),
                            ISNULL(UPPER(LTRIM(RTRIM(src.lgr))), ''),
                            ISNULL(UPPER(LTRIM(RTRIM(src.vnr))), ''),
                            ISNULL(NULLIF(LTRIM(RTRIM(src.lgrlok)), ''), '')
                        ) AS CleanCode_Lagerbeholdning
                    INTO [stg].[fct_Lagerbeholdning_ils]
                    FROM [stg].[pre_Lagerbeholdning_ils] src
                    GROUP BY
                        src.Snapshot_DateTime, src.Snapshot_Datekey,
                        src.sgh, src.lgr, src.vnr,
                        NULLIF(LTRIM(RTRIM(src.lgrlok)), '');
                """
                )

                row_count = cursor.rowcount
                snapshot_count = 0

                if row_count > 0:
                    cursor.execute(
                        """
                        SELECT COUNT(DISTINCT DW_EK_Dato_Snapshot)
                        FROM [stg].[fct_Lagerbeholdning_ils]
                    """
                    )
                    snapshot_count = cursor.fetchone()[0]

                    cursor.execute(
                        """
                        CREATE CLUSTERED INDEX IX_fct_Lagerbeh_Snapshot
                        ON [stg].[fct_Lagerbeholdning_ils](DW_EK_Dato_Snapshot);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_Lagerbeh_CC
                        ON [stg].[fct_Lagerbeholdning_ils](CleanCode_Lagerbeholdning);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_Lagerbeh_Lager
                        ON [stg].[fct_Lagerbeholdning_ils](Lager_CleanCode);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_Lagerbeh_IlsVare
                        ON [stg].[fct_Lagerbeholdning_ils](IlsVare_CleanCode);
                    """
                    )

                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"Staged {row_count} warehouse stock rows ({snapshot_count} snapshots)")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.fct_Lagerbeholdning_ils",
            "row_count": row_count,
            "snapshot_dates": snapshot_count,
        }
    )
