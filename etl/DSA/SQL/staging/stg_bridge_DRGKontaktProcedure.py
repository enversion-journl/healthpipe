# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Staging for the procedure bridge: one row per procedure registration.

Delta-scoped to contacts whose procedure or procedure-tillaeg rows changed
since the checkpoint (both delivery types); checkpoint is the MIN over the
downstream owners (dim.DRGProcedure + bridge.DRGProcedureGruppe +
fct.DRGKontakt) — the fact is included so a failed/retried fct cannot be
left with the -1 empty-group sentinel on a contact that has procedures.

Carries clean_code_DRGKontakt, clean_code_DRGProcedure (the combination
key), the code attributes for the dimension and the raw producing-unit SOR
(resolved to dw_ek_Organisation in the bridge). No timestamps, no CPR.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset
from DSA.SQL.staging.drg_bridge_sql import (
    BEGINNING_OF_TIME,
    PROCEDURE_STG,
    build_procedure_scope_sql,
    build_procedure_stg_sql,
)

_ARC_DEPS = [
    "arc_drg_procedure",
    "arc_drg_procedure_tillaeg",
    "arc_drgpsyk_procedure",
    "arc_drgpsyk_procedure_tillaeg",
]


@asset(
    key=AssetKey(["drg", "stg_bridge_DRGKontaktProcedure"]),
    group_name="drg_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey([d]) for d in _ARC_DEPS],
    tags={"domain": "drg", "layer": "stg", "source": "drg", "entity": "drgprocedure"},
    description=(
        "Staging for the procedure bridge — one row per procedure registration "
        "(somatik + psyk) with contact and combination clean_codes, the raw "
        "producing-unit SOR, delta-scoped, tillaegskoder denormalised."
    ),
)
def stg_bridge_DRGKontaktProcedure(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    # MIN over the downstream owners; a missing consumer (None) counts as
    # BEGINNING_OF_TIME, so a lagging owner forces a full-archive scope until it
    # catches up. full_load is the full archive by definition. fct.DRGKontakt is
    # a downstream owner too: if the fact fails after the bridge succeeded, its
    # checkpoint lags the bridge's, and without this a retry would re-stage the
    # contact from pre_stg while its procedure mapping is already behind the
    # bridge window — the fact's group-FK COALESCE would then write the -1
    # empty-group sentinel to a contact that actually has procedures. Including
    # the fact keeps the mapping in scope until the fact has consumed it.
    downstream = (
        dfm.get_checkpoint("dim", "DRGProcedure"),
        dfm.get_checkpoint("bridge", "DRGProcedureGruppe"),
        dfm.get_checkpoint("fct", "DRGKontakt"),
    )
    checkpoint = min(cp or BEGINNING_OF_TIME for cp in downstream)
    if config.mode == "full_load":
        checkpoint = BEGINNING_OF_TIME

    run_ctx = dfm.start_dfm_run(
        "stg", "bridge_DRGKontaktProcedure", dagster_run_id=context.run_id, mode=config.mode
    )
    context.log.info(
        "Full load from arc"
        if checkpoint == BEGINNING_OF_TIME
        else f"Delta scope: contacts touched after {checkpoint}"
    )
    row_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_procedure_scope_sql(checkpoint))
                cursor.execute(build_procedure_stg_sql())
                conn.commit()
                cursor.execute(f"SELECT COUNT(*) FROM {PROCEDURE_STG}")
                row_count = cursor.fetchone()[0]
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "checkpoint": "full load" if checkpoint == BEGINNING_OF_TIME else str(checkpoint),
            "table": "stg.bridge_DRGKontaktProcedure",
        }
    )
