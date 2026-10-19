# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: fct_Lagerbeholdning

Business logic identical to stg_fct_Lagerbeholdning_ils.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.dato_dimension import dato_key, dato_tid


@dg.asset(
    key=dg.AssetKey(["erp", "stg_fct_Lagerbeholdning_ils"]),
    group_name="erp_staging",
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
                    f"""
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
                        {dato_key('MAX(src.udlobdto)')} AS dw_ek_Dato_udloeb,
                        {dato_tid('MAX(src.udlobdto)')} AS dato_tid_udloeb,
                        MAX(src.clean_code_Lager) AS clean_code_Lager,
                        CONCAT('ILSVARE_', UPPER(LTRIM(RTRIM(src.vnr)))) AS clean_code_ILSVare,
                        CONCAT_WS('_',
                            ISNULL(NULLIF(LTRIM(RTRIM(src.lgrlok)), ''), ''),
                            ISNULL(MAX(NULLIF(LTRIM(RTRIM(src.udlevenh)), '')), ''),
                            ISNULL(CAST(MAX(CAST(src.sw_erstat AS INT)) AS NVARCHAR(1)), ''),
                            ISNULL(CAST(MAX(src.lgrbeh_prio) AS NVARCHAR(10)), '')
                        ) AS clean_code_LagerbeholdningEgenskaber,
                        src.snapshot_datekey AS dw_ek_Dato_snapshot,
                        src.snapshot_date_time AS dato_tid_snapshot,
                        CONCAT_WS('_',
                            CAST(src.snapshot_datekey AS NVARCHAR(8)),
                            ISNULL(UPPER(LTRIM(RTRIM(src.sgh))), ''),
                            ISNULL(UPPER(LTRIM(RTRIM(src.lgr))), ''),
                            ISNULL(UPPER(LTRIM(RTRIM(src.vnr))), ''),
                            ISNULL(NULLIF(LTRIM(RTRIM(src.lgrlok)), ''), '')
                        ) AS clean_code_Lagerbeholdning
                    INTO [stg].[fct_Lagerbeholdning_ils]
                    FROM [stg].[pre_fct_Lagerbeholdning_ils] src
                    GROUP BY
                        src.snapshot_date_time, src.snapshot_datekey,
                        src.sgh, src.lgr, src.vnr,
                        NULLIF(LTRIM(RTRIM(src.lgrlok)), '');
                """
                )

                row_count = cursor.rowcount
                snapshot_count = 0

                if row_count > 0:
                    cursor.execute(
                        """
                        SELECT COUNT(DISTINCT dw_ek_Dato_snapshot)
                        FROM [stg].[fct_Lagerbeholdning_ils]
                    """
                    )
                    snapshot_count = cursor.fetchone()[0]

                    cursor.execute(
                        """
                        CREATE CLUSTERED INDEX IX_fct_Lagerbeh_Snapshot
                        ON [stg].[fct_Lagerbeholdning_ils](dw_ek_Dato_snapshot);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_Lagerbeh_CC
                        ON [stg].[fct_Lagerbeholdning_ils](clean_code_Lagerbeholdning);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_Lagerbeh_Lager
                        ON [stg].[fct_Lagerbeholdning_ils](clean_code_Lager);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_Lagerbeh_IlsVare
                        ON [stg].[fct_Lagerbeholdning_ils](clean_code_ILSVare);
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
