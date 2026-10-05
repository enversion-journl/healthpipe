# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension: LagerbeholdningEgenskaber (Type 1 SCD)

Business logic identical to dim_LagerbeholdningEgenskaber.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    name="dim_LagerbeholdningEgenskaber",
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["erp", "stg_dim_LagerbeholdningEgenskaber_ils"]),
        dg.AssetKey(["map_LagerbeholdningEgenskaber"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "entity": "LagerbeholdningEgenskaber"},
    description="MERGE dim.LagerbeholdningEgenskaber (Type 1 SCD).",
)
def dim_LagerbeholdningEgenskaber(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "LagerbeholdningEgenskaber",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT
                        m.dw_ek_LagerbeholdningEgenskaber,
                        stg.clean_code_LagerbeholdningEgenskaber,
                        stg.lnr, stg.lgrlok, stg.udlevenh, stg.sw_erstat, stg.lgrbeh_prio
                    INTO #LagerbehEgen_Resolved                    FROM [stg].[dim_LagerbeholdningEgenskaber_ils] stg
                    INNER JOIN [map].[LagerbeholdningEgenskaber] m
                        ON m.clean_code_LagerbeholdningEgenskaber
                         = stg.clean_code_LagerbeholdningEgenskaber;
                """
                )
                resolved_count = cursor.rowcount

                cursor.execute(
                    """
                    MERGE INTO [dim].[LagerbeholdningEgenskaber] AS tgt
                    USING #LagerbehEgen_Resolved AS src
                        ON tgt.dw_ek_LagerbeholdningEgenskaber = src.dw_ek_LagerbeholdningEgenskaber
                    WHEN MATCHED THEN UPDATE SET
                        tgt.clean_code_LagerbeholdningEgenskaber = src.clean_code_LagerbeholdningEgenskaber,
                        tgt.lnr = CASE WHEN src.lnr IS NOT NULL AND src.lnr <> '' THEN src.lnr ELSE tgt.lnr END,
                        tgt.lgrlok = src.lgrlok,
                        tgt.udlevenh = src.udlevenh,
                        tgt.sw_erstat = src.sw_erstat,
                        tgt.lgrbeh_prio = src.lgrbeh_prio,
                        tgt.dw_timestamp_update = SYSDATETIME()
                    WHEN NOT MATCHED THEN INSERT (
                        dw_ek_LagerbeholdningEgenskaber,
                        clean_code_LagerbeholdningEgenskaber,
                        lnr, lgrlok, udlevenh, sw_erstat, lgrbeh_prio,
                        dw_timestamp_insert, dw_timestamp_update
                    ) VALUES (
                        src.dw_ek_LagerbeholdningEgenskaber,
                        src.clean_code_LagerbeholdningEgenskaber,
                        src.lnr, src.lgrlok, src.udlevenh, src.sw_erstat, src.lgrbeh_prio,
                        SYSDATETIME(), SYSDATETIME()
                    );
                """
                )
                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #LagerbehEgen_Resolved;")
                cursor.execute("UPDATE STATISTICS [dim].[LagerbeholdningEgenskaber];")
                cursor.execute("SELECT COUNT(*) FROM [dim].[LagerbeholdningEgenskaber]")
                total = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"dim.LagerbeholdningEgenskaber: {merge_count} merged, {total} total")

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "LagerbeholdningEgenskaber", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "LagerbeholdningEgenskaber")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=merge_count
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "dim.LagerbeholdningEgenskaber",
            "merged_rows": merge_count,
            "total_rows": total,
            "resolved_from_map": resolved_count,
        }
    )
