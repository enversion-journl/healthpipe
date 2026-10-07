# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Bridge Asset: bridge.DRGDiagnoseGruppe (group -> diagnose membership).

Append-only: a group is identified by the hash of its member set, so its
membership is immutable — only new (group, code) rows are ever inserted. When a
contact's set changes it points to a different group (handled on the fact); a
group's rows here never change or soft-delete. Resolves the staged membership
via map.DRGDiagnoseGruppe + map.DRGDiagnose and fails fast on anything
unresolved (the maps must run first).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset

UNRESOLVED_SQL = """
SELECT COUNT(*)
FROM [stg].[DRGDiagnoseGruppeMedlem] s
LEFT JOIN [map].[DRGDiagnoseGruppe] mg ON mg.[clean_code_DRGDiagnoseGruppe] = s.[clean_code_DRGDiagnoseGruppe]
LEFT JOIN [map].[DRGDiagnose] md ON md.[clean_code_DRGDiagnose] = s.[clean_code_DRGDiagnose]
WHERE mg.[dw_ek_DRGDiagnoseGruppe] IS NULL OR md.[dw_ek_DRGDiagnose] IS NULL;
"""

INSERT_SQL = """
INSERT INTO [bridge].[DRGDiagnoseGruppe] (
    [dw_ek_DRGDiagnoseGruppe], [dw_ek_DRGDiagnose], [dw_is_deleted], [dw_timestamp_insert], [dw_timestamp_update]
)
SELECT DISTINCT
    mg.[dw_ek_DRGDiagnoseGruppe], md.[dw_ek_DRGDiagnose], 0,
    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[DRGDiagnoseGruppeMedlem] s
INNER JOIN [map].[DRGDiagnoseGruppe] mg ON mg.[clean_code_DRGDiagnoseGruppe] = s.[clean_code_DRGDiagnoseGruppe]
INNER JOIN [map].[DRGDiagnose] md ON md.[clean_code_DRGDiagnose] = s.[clean_code_DRGDiagnose]
WHERE NOT EXISTS (
    SELECT 1 FROM [bridge].[DRGDiagnoseGruppe] b
    WHERE b.[dw_ek_DRGDiagnoseGruppe] = mg.[dw_ek_DRGDiagnoseGruppe]
      AND b.[dw_ek_DRGDiagnose] = md.[dw_ek_DRGDiagnose]
);
"""


@asset(
    group_name="drg_dimension",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["drg", "stg_DRGDiagnoseGruppe"]),
        AssetKey(["map_DRGDiagnoseGruppe"]),
        AssetKey(["map_DRGDiagnose"]),
    ],
    tags={"domain": "drg", "layer": "bridge", "module": "datawarehouse"},
    description=(
        "bridge.DRGDiagnoseGruppe — append-only group->diagnose membership; "
        "immutable per group set, resolved via the maps."
    ),
)
def bridge_DRGDiagnoseGruppe(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "bridge", "DRGDiagnoseGruppe", dagster_run_id=context.run_id, mode=config.mode
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
                        f"bridge_DRGDiagnoseGruppe: {unresolved} staged membership rows unresolved "
                        "against map.DRGDiagnoseGruppe/map.DRGDiagnose. Run the maps first."
                    )
                cursor.execute(INSERT_SQL)
                new_rows = cursor.rowcount
                cursor.execute("UPDATE STATISTICS [bridge].[DRGDiagnoseGruppe];")
                conn.commit()
                cursor.execute(
                    "SELECT COUNT(*) FROM [bridge].[DRGDiagnoseGruppe] WHERE dw_ek_DRGDiagnoseGruppe <> -1"
                )
                total_rows = cursor.fetchone()[0]
                context.log.info(
                    f"bridge.DRGDiagnoseGruppe: {new_rows} new membership rows, {total_rows} total"
                )
            finally:
                cursor.close()
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("bridge", "DRGDiagnoseGruppe", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("bridge", "DRGDiagnoseGruppe")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=new_rows
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
    return MaterializeResult(
        metadata={
            "table": "bridge.DRGDiagnoseGruppe",
            "new_rows": new_rows,
            "total_rows": total_rows,
            "grain": "one row per (dw_ek_DRGDiagnoseGruppe, dw_ek_DRGDiagnose)",
            "scd_type": "append-only (immutable membership per group)",
            "checkpoint_after": logged_checkpoint,
            "mode": config.mode,
        }
    )
