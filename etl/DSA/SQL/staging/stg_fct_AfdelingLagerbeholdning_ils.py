# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: fct_AfdelingLagerbeholdning

Business logic identical to stg_fct_AfdelingLagerbeholdning_ils.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.dato_dimension import dato_key, dato_tid


@dg.asset(
    key=dg.AssetKey(["erp", "stg_fct_AfdelingLagerbeholdning_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[dg.AssetKey(["erp", "pre_stg_AfdelingLagerbeholdning_ils"])],
    tags={"domain": "erp", "layer": "stg_fct", "entity": "afdeling_lagerbeholdning"},
    description="Fact staging for department lagerbeholdning from pre-staging.",
)
def stg_fct_AfdelingLagerbeholdning_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "fct_AfdelingLagerbeholdning_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS [stg].[fct_AfdelingLagerbeholdning_ils];")

                cursor.execute(
                    f"""
                    SELECT
                        src.sgh, src.lgr, src.vnr, src.brik,
                        UPPER(LTRIM(RTRIM(NULLIF(src.rnr, '')))) AS rnr,
                        SUM(src.antal) AS antal,
                        MAX(CASE WHEN LTRIM(RTRIM(src.lnr)) <> '' THEN UPPER(LTRIM(RTRIM(src.lnr))) END) AS lnr,
                        {dato_key('MAX(src.udlobdto)')} AS dw_ek_Dato_udloeb,
                        {dato_tid('MAX(src.udlobdto)')} AS dato_tid_udloeb,
                        MAX(src.clean_code_Lager) AS clean_code_Lager,
                        MAX(src.clean_code_Rekvirent) AS clean_code_Rekvirent,
                        CONCAT('ILSVARE_', UPPER(LTRIM(RTRIM(src.vnr)))) AS clean_code_ILSVare,
                        ISNULL(CAST(src.brik AS NVARCHAR(20)), '') AS clean_code_AfdelingLagerbeholdningEgenskaber,
                        src.snapshot_datekey AS dw_ek_Dato_snapshot,
                        src.snapshot_date_time AS dato_tid_snapshot,
                        CONCAT_WS('_',
                            CAST(src.snapshot_datekey AS NVARCHAR(8)),
                            ISNULL(UPPER(LTRIM(RTRIM(src.sgh))), ''),
                            ISNULL(UPPER(LTRIM(RTRIM(src.lgr))), ''),
                            ISNULL(UPPER(LTRIM(RTRIM(src.vnr))), ''),
                            ISNULL(CAST(src.brik AS NVARCHAR(20)), ''),
                            ISNULL(UPPER(LTRIM(RTRIM(NULLIF(src.rnr, '')))), '')
                        ) AS clean_code_AfdelingLagerbeholdning
                    INTO [stg].[fct_AfdelingLagerbeholdning_ils]
                    FROM [stg].[pre_fct_AfdelingLagerbeholdning_ils] src
                    GROUP BY
                        src.snapshot_date_time, src.snapshot_datekey,
                        src.sgh, src.lgr, src.vnr, src.brik,
                        UPPER(LTRIM(RTRIM(NULLIF(src.rnr, ''))));
                """
                )

                row_count = cursor.rowcount
                snapshot_count = 0

                if row_count > 0:
                    cursor.execute(
                        """
                        SELECT COUNT(DISTINCT dw_ek_Dato_snapshot)
                        FROM [stg].[fct_AfdelingLagerbeholdning_ils]
                    """
                    )
                    snapshot_count = cursor.fetchone()[0]

                    cursor.execute(
                        """
                        CREATE CLUSTERED INDEX IX_fct_AfdLagerbeh_Snapshot
                        ON [stg].[fct_AfdelingLagerbeholdning_ils](dw_ek_Dato_snapshot);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_AfdLagerbeh_CC
                        ON [stg].[fct_AfdelingLagerbeholdning_ils](clean_code_AfdelingLagerbeholdning);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_AfdLagerbeh_Lager
                        ON [stg].[fct_AfdelingLagerbeholdning_ils](clean_code_Lager);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_fct_AfdLagerbeh_IlsVare
                        ON [stg].[fct_AfdelingLagerbeholdning_ils](clean_code_ILSVare);
                    """
                    )

                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"Staged {row_count} department stock rows ({snapshot_count} snapshots)")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.fct_AfdelingLagerbeholdning_ils",
            "row_count": row_count,
            "snapshot_dates": snapshot_count,
        }
    )
