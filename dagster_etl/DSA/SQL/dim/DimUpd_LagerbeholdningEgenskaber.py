# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension: LagerbeholdningEgenskaber (Type 1 SCD)

Business logic identical to DimUpd_LagerbeholdningEgenskaber.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    name="dim_LagerbeholdningEgenskaber",
    group_name="dimensions_lagerbeholdning",
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
                        m.DW_EK_LagerbeholdningEgenskaber,
                        stg.CleanCode_LagerbeholdningEgenskaber,
                        stg.Lnr, stg.Lgrlok, stg.Udlevenh, stg.Sw_erstat, stg.Lgrbeh_prio
                    INTO #LagerbehEgen_Resolved                    FROM [stg].[dim_LagerbeholdningEgenskaber_ils] stg
                    INNER JOIN [map].[LagerbeholdningEgenskaber] m
                        ON m.CleanCode_LagerbeholdningEgenskaber
                         = stg.CleanCode_LagerbeholdningEgenskaber;
                """
                )
                resolved_count = cursor.rowcount

                cursor.execute(
                    """
                    MERGE INTO [dim].[LagerbeholdningEgenskaber] AS tgt
                    USING #LagerbehEgen_Resolved AS src
                        ON tgt.DW_EK_LagerbeholdningEgenskaber = src.DW_EK_LagerbeholdningEgenskaber
                    WHEN MATCHED THEN UPDATE SET
                        tgt.CleanCode_LagerbeholdningEgenskaber = src.CleanCode_LagerbeholdningEgenskaber,
                        tgt.Lnr = CASE WHEN src.Lnr IS NOT NULL AND src.Lnr <> '' THEN src.Lnr ELSE tgt.Lnr END,
                        tgt.Lgrlok = src.Lgrlok,
                        tgt.Udlevenh = src.Udlevenh,
                        tgt.Sw_erstat = src.Sw_erstat,
                        tgt.Lgrbeh_prio = src.Lgrbeh_prio,
                        tgt.DW_Timestamp_Update = SYSDATETIME()
                    WHEN NOT MATCHED THEN INSERT (
                        DW_EK_LagerbeholdningEgenskaber,
                        CleanCode_LagerbeholdningEgenskaber,
                        Lnr, Lgrlok, Udlevenh, Sw_erstat, Lgrbeh_prio,
                        DW_Timestamp_Insert, DW_Timestamp_Update
                    ) VALUES (
                        src.DW_EK_LagerbeholdningEgenskaber,
                        src.CleanCode_LagerbeholdningEgenskaber,
                        src.Lnr, src.Lgrlok, src.Udlevenh, src.Sw_erstat, src.Lgrbeh_prio,
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
