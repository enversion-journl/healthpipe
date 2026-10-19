# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pre-Staging: afdlgrbeh from arc.ils_afdlgrbeh
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.dato_dimension import dato_key
from DSA.SQL.common.ils_clean_code_shared import (
    sql_clean_code_lager,
    sql_clean_code_rekvirent,
)

# Raw keys, as the maps hold them. Module-level so the contract is testable
# without running the asset.
LAGER_CLEAN_CODE_SQL = sql_clean_code_lager("src.sgh", "src.lgr")
REKVIRENT_CLEAN_CODE_SQL = sql_clean_code_rekvirent("src.sgh", "src.rnr")


@dg.asset(
    key=dg.AssetKey(["erp", "pre_stg_AfdelingLagerbeholdning_ils"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    deps=[dg.AssetKey(["arc_ils_afdlgrbeh"])],
    tags={"domain": "erp", "layer": "pre_stg", "source": "ils", "entity": "afdlgrbeh"},
    description="Pre-staging for ILS department stock from arc.ils_afdlgrbeh.",
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
        "pre_fct_AfdelingLagerbeholdning_ils",
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
                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_fct_AfdelingLagerbeholdning_ils];")

                cursor.execute(
                    f"""
                    WITH ranked AS (
                        SELECT
                            src.*,
                            {LAGER_CLEAN_CODE_SQL} AS clean_code_Lager,
                            {REKVIRENT_CLEAN_CODE_SQL} AS clean_code_Rekvirent,
                            src.dw_timestamp_update AS snapshot_date_time,
                            {dato_key('src.dw_timestamp_update')} AS snapshot_datekey,
                            DENSE_RANK() OVER (
                                PARTITION BY CAST(src.dw_timestamp_update AS DATE)
                                ORDER BY src.dw_timestamp_update DESC
                            ) AS dr
                        FROM [arc].[ils_afdlgrbeh] src
                        {where_clause}
                    )
                    SELECT *
                    INTO [stg].[pre_fct_AfdelingLagerbeholdning_ils]
                    FROM ranked
                    WHERE dr = 1;
                """
                )

                row_count = cursor.rowcount

                if row_count > 0:
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_stg_pre_AfdLagerbeh_Lager
                        ON [stg].[pre_fct_AfdelingLagerbeholdning_ils](clean_code_Lager);
                    """
                    )
                    cursor.execute(
                        """
                        CREATE NONCLUSTERED INDEX IX_stg_pre_AfdLagerbeh_Rekvirent
                        ON [stg].[pre_fct_AfdelingLagerbeholdning_ils](clean_code_Rekvirent);
                    """
                    )

                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"stg.pre_fct_AfdelingLagerbeholdning_ils: {row_count} rows")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.pre_fct_AfdelingLagerbeholdning_ils",
            "row_count": row_count,
            "source_table": "arc.ils_afdlgrbeh",
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "mode": config.mode,
        }
    )
