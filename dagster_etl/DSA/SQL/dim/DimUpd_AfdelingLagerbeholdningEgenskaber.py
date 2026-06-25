# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension: AfdelingLagerbeholdningEgenskaber (Type 1 SCD)
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    name="dim_AfdelingLagerbeholdningEgenskaber",
    group_name="dimensions_lagerbeholdning",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["erp", "stg_dim_AfdelingLagerbeholdningEgenskaber_ils"]),
        dg.AssetKey(["map_AfdelingLagerbeholdningEgenskaber"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "entity": "AfdelingLagerbeholdningEgenskaber"},
    description="MERGE dim.AfdelingLagerbeholdningEgenskaber (Type 1 SCD).",
)
def dim_AfdelingLagerbeholdningEgenskaber(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
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
                    SELECT
                        m.DW_EK_AfdelingLagerbeholdningEgenskaber,
                        stg.CleanCode_AfdelingLagerbeholdningEgenskaber,
                        stg.Lnr, stg.Brik
                    INTO #AfdLagerbehEgen_Resolved                    FROM [stg].[dim_AfdelingLagerbeholdningEgenskaber_ils] stg
                    INNER JOIN [map].[AfdelingLagerbeholdningEgenskaber] m
                        ON m.CleanCode_AfdelingLagerbeholdningEgenskaber
                         = stg.CleanCode_AfdelingLagerbeholdningEgenskaber;
                """
                )
                resolved_count = cursor.rowcount

                cursor.execute(
                    """
                    MERGE INTO [dim].[AfdelingLagerbeholdningEgenskaber] AS tgt
                    USING #AfdLagerbehEgen_Resolved AS src
                        ON tgt.DW_EK_AfdelingLagerbeholdningEgenskaber = src.DW_EK_AfdelingLagerbeholdningEgenskaber
                    WHEN MATCHED THEN UPDATE SET
                        tgt.CleanCode_AfdelingLagerbeholdningEgenskaber = src.CleanCode_AfdelingLagerbeholdningEgenskaber,
                        tgt.Lnr = CASE WHEN src.Lnr IS NOT NULL AND src.Lnr <> '' THEN src.Lnr ELSE tgt.Lnr END,
                        tgt.Brik = src.Brik,
                        tgt.DW_Timestamp_Update = SYSDATETIME()
                    WHEN NOT MATCHED THEN INSERT (
                        DW_EK_AfdelingLagerbeholdningEgenskaber,
                        CleanCode_AfdelingLagerbeholdningEgenskaber,
                        Lnr, Brik,
                        DW_Timestamp_Insert, DW_Timestamp_Update
                    ) VALUES (
                        src.DW_EK_AfdelingLagerbeholdningEgenskaber,
                        src.CleanCode_AfdelingLagerbeholdningEgenskaber,
                        src.Lnr, src.Brik,
                        SYSDATETIME(), SYSDATETIME()
                    );
                """
                )
                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #AfdLagerbehEgen_Resolved;")
                cursor.execute("UPDATE STATISTICS [dim].[AfdelingLagerbeholdningEgenskaber];")
                cursor.execute("SELECT COUNT(*) FROM [dim].[AfdelingLagerbeholdningEgenskaber]")
                total = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(
            f"dim.AfdelingLagerbeholdningEgenskaber: {merge_count} merged, {total} total"
        )

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "AfdelingLagerbeholdningEgenskaber", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "AfdelingLagerbeholdningEgenskaber")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=merge_count
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "dim.AfdelingLagerbeholdningEgenskaber",
            "merged_rows": merge_count,
            "total_rows": total,
            "resolved_from_map": resolved_count,
        }
    )
