# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Staging for the procedure group layer.

Derives from the (already delta-scoped) per-registration staging, resolving the
producing unit to dw_ek_Organisation (part of the group's defining set):
  - stg.DRGKontaktProcedureGruppe: one group clean_code per contact.
  - stg.DRGProcedureGruppeMedlem: the distinct group -> (procedure, unit) rows.

No timestamps, no CPR.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset
from DSA.SQL.staging.drg_bridge_sql import (
    PROCEDURE_GRUPPE_MEDLEM_STG,
    PROCEDURE_GRUPPE_STG,
    build_procedure_gruppe_stg_sql,
)


@asset(
    key=AssetKey(["drg", "stg_DRGProcedureGruppe"]),
    group_name="drg_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["drg", "stg_bridge_DRGKontaktProcedure"]), AssetKey(["dim_Organisation"])],
    tags={"domain": "drg", "layer": "stg", "source": "drg", "entity": "drgproceduregruppe"},
    description=(
        "Staging for the procedure group layer — per-contact group clean_code "
        "(hash over the distinct procedure+unit set) and the group->member rows."
    ),
)
def stg_DRGProcedureGruppe(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "DRGProcedureGruppe", dagster_run_id=context.run_id, mode=config.mode
    )
    grupper = 0
    medlemmer = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_procedure_gruppe_stg_sql())
                conn.commit()
                cursor.execute(f"SELECT COUNT(*) FROM {PROCEDURE_GRUPPE_STG}")
                grupper = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM {PROCEDURE_GRUPPE_MEDLEM_STG}")
                medlemmer = cursor.fetchone()[0]
                context.log.info(
                    f"stg procedure-gruppe: {grupper} kontakt-grupper, {medlemmer} medlemsrækker"
                )
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=grupper)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
    return MaterializeResult(
        metadata={
            "kontakt_grupper": grupper,
            "medlemsraekker": medlemmer,
            "tables": f"{PROCEDURE_GRUPPE_STG} + {PROCEDURE_GRUPPE_MEDLEM_STG}",
        }
    )
