# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Bridge Asset: bridge.DRGProcedureGruppe (group -> procedure + unit membership).

Append-only, like the diagnose group bridge. The producing unit is part of the
group's defining set and is carried as dw_ek_Organisation_producerende (already
resolved in the group staging). Resolves the procedure code via map.DRGProcedure
and the group via map.DRGProcedureGruppe, and fails fast on anything unresolved.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset

UNRESOLVED_SQL = """
SELECT COUNT(*)
FROM [stg].[DRGProcedureGruppeMedlem] s
LEFT JOIN [map].[DRGProcedureGruppe] mg ON mg.[clean_code_DRGProcedureGruppe] = s.[clean_code_DRGProcedureGruppe]
LEFT JOIN [map].[DRGProcedure] mp ON mp.[clean_code_DRGProcedure] = s.[clean_code_DRGProcedure]
WHERE mg.[dw_ek_DRGProcedureGruppe] IS NULL OR mp.[dw_ek_DRGProcedure] IS NULL;
"""

INSERT_SQL = """
INSERT INTO [bridge].[DRGProcedureGruppe] (
    [dw_ek_DRGProcedureGruppe], [dw_ek_DRGProcedure], [dw_ek_Organisation_producerende],
    [dw_is_deleted], [dw_timestamp_insert], [dw_timestamp_update]
)
SELECT DISTINCT
    mg.[dw_ek_DRGProcedureGruppe], mp.[dw_ek_DRGProcedure], s.[dw_ek_Organisation_producerende], 0,
    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[DRGProcedureGruppeMedlem] s
INNER JOIN [map].[DRGProcedureGruppe] mg ON mg.[clean_code_DRGProcedureGruppe] = s.[clean_code_DRGProcedureGruppe]
INNER JOIN [map].[DRGProcedure] mp ON mp.[clean_code_DRGProcedure] = s.[clean_code_DRGProcedure]
WHERE NOT EXISTS (
    SELECT 1 FROM [bridge].[DRGProcedureGruppe] b
    WHERE b.[dw_ek_DRGProcedureGruppe] = mg.[dw_ek_DRGProcedureGruppe]
      AND b.[dw_ek_DRGProcedure] = mp.[dw_ek_DRGProcedure]
      AND b.[dw_ek_Organisation_producerende] = s.[dw_ek_Organisation_producerende]
);
"""


@asset(
    group_name="drg_dimension",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["drg", "stg_DRGProcedureGruppe"]),
        AssetKey(["map_DRGProcedureGruppe"]),
        AssetKey(["map_DRGProcedure"]),
    ],
    tags={"domain": "drg", "layer": "bridge", "module": "datawarehouse"},
    description=(
        "bridge.DRGProcedureGruppe — append-only group->procedure membership "
        "with producing unit; immutable per group set, resolved via the maps."
    ),
)
def bridge_DRGProcedureGruppe(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "bridge", "DRGProcedureGruppe", dagster_run_id=context.run_id, mode=config.mode
    )
    new_rows = 0
    total_rows = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNRESOLVED_SQL)
                unresolved = cursor.fetchone()[0]
                if unresolved > 0:
                    raise ValueError(
                        f"bridge_DRGProcedureGruppe: {unresolved} staged membership rows unresolved "
                        "against map.DRGProcedureGruppe/map.DRGProcedure. Run the maps first."
                    )
                cursor.execute(INSERT_SQL)
                new_rows = cursor.rowcount
                cursor.execute("UPDATE STATISTICS [bridge].[DRGProcedureGruppe];")
                conn.commit()
                cursor.execute(
                    "SELECT COUNT(*) FROM [bridge].[DRGProcedureGruppe] WHERE dw_ek_DRGProcedureGruppe <> -1"
                )
                total_rows = cursor.fetchone()[0]
                context.log.info(
                    f"bridge.DRGProcedureGruppe: {new_rows} new membership rows, {total_rows} total"
                )
            finally:
                cursor.close()
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("bridge", "DRGProcedureGruppe", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("bridge", "DRGProcedureGruppe")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=new_rows
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
    return MaterializeResult(
        metadata={
            "table": "bridge.DRGProcedureGruppe",
            "new_rows": new_rows,
            "total_rows": total_rows,
            "grain": "one row per (dw_ek_DRGProcedureGruppe, dw_ek_DRGProcedure, dw_ek_Organisation_producerende)",
            "scd_type": "append-only (immutable membership per group)",
            "checkpoint_after": logged_checkpoint,
            "mode": config.mode,
        }
    )
