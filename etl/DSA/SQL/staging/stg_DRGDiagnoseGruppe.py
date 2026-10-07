# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Staging for the diagnose group layer.

Derives from the (already delta-scoped) per-registration staging:
  - stg.DRGKontaktDiagnoseGruppe: one group clean_code per contact, hashed over
    its DISTINCT set of diagnose members (order-independent).
  - stg.DRGDiagnoseGruppeMedlem: the distinct group -> member rows.

The group interns a contact's whole diagnosis set as one key, so fct.DRGKontakt
references it with a single FK and the membership is stored once per set. No CPR.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset
from DSA.SQL.staging.drg_bridge_sql import (
    DIAGNOSE_GRUPPE_MEDLEM_STG,
    DIAGNOSE_GRUPPE_STG,
    build_diagnose_gruppe_stg_sql,
)


@asset(
    key=AssetKey(["drg", "stg_DRGDiagnoseGruppe"]),
    group_name="drg_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["drg", "stg_bridge_DRGKontaktDiagnose"])],
    tags={"domain": "drg", "layer": "stg", "source": "drg", "entity": "drgdiagnosegruppe"},
    description=(
        "Staging for the diagnose group layer — per-contact group clean_code "
        "(hash over the distinct diagnose set) and the group->member rows."
    ),
)
def stg_DRGDiagnoseGruppe(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "DRGDiagnoseGruppe", dagster_run_id=context.run_id, mode=config.mode
    )
    grupper = 0
    medlemmer = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_diagnose_gruppe_stg_sql())
                conn.commit()
                cursor.execute(f"SELECT COUNT(*) FROM {DIAGNOSE_GRUPPE_STG}")
                grupper = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM {DIAGNOSE_GRUPPE_MEDLEM_STG}")
                medlemmer = cursor.fetchone()[0]
                context.log.info(
                    f"stg diagnose-gruppe: {grupper} kontakt-grupper, {medlemmer} medlemsrækker"
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
            "tables": f"{DIAGNOSE_GRUPPE_STG} + {DIAGNOSE_GRUPPE_MEDLEM_STG}",
        }
    )
