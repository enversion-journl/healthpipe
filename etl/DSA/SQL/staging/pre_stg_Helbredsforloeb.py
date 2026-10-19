# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pre-staging asset for udredningsret helbredsforloeb — the scope table.

stg.pre_Helbredsforloeb holds ONE row per helbredsforloeb_noegle in this
run's scope (spec 3.1/3.2, docs/source_systems/udredningsret/
udredningsret_model.md). Every other udredningsret staging table restricts
itself to the helbredsforloeb present here, so the delta window is decided
exactly once: helbredsforloeb with at least one arc row (live or
soft-deleted) touched after the checkpoint, plus the borger-recovery branch
of decision U9 (live fact rows still pointing at dw_ek_Borger = -1). The
checkpoint is the MIN over the downstream dim/fct owners, mirroring
pre_stg_DRGKontakt — except that an owner without a checkpoint forces a full
load (decision U11); a full load has no checkpoint.

A helbredsforloeb whose markers are ALL soft-deleted stays in scope and is
staged with dw_is_deleted = 1 and zero forloeb — that is how the forloeb
fact learns that its rows are gone (spec 6).

GDPR: CPR is resolved to dw_ek_Borger inside the SQL (via #borger_current_cpr /
#borger_previous_cpr against dim.Borger, tidligere_cpr as fallback) while the scoped
arc rows are copied into a session temp table, and never enters the staging
table. Unknown CPRs get -1 (created in FHIR by fhir_patient_udredningsret);
blank gets -2. Only counts are reported.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    RetryPolicy,
    asset,
)
from DSA.SQL.staging.udredningsret_kolonner import (
    DOWNSTREAM_CHECKPOINT_EJERE,
    PRE_HF_TABLE,
)
from DSA.SQL.staging.udredningsret_staging_sql import (
    build_borger_coverage_sql,
    build_pre_helbredsforloeb_sql,
    create_borger_cpr_lookup_tables_sql,
)


@asset(
    key=AssetKey(["udredningsret", "pre_stg_Helbredsforloeb"]),
    group_name="udredningsret_pre_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["arc_udredningsret_mont"]), AssetKey(["dim_Borger"])],
    tags={
        "domain": "udredningsret",
        "layer": "pre_stg",
        "source": "udredningsret",
        "entity": "helbredsforloeb",
    },
    description=(
        "Scope table for the udredningsret staging chain: one row per "
        "helbredsforloeb in the delta window (arc rows touched after the MIN "
        "downstream checkpoint, plus borger recovery) with "
        "clean_code_Helbredsforloeb, CPR resolved to dw_ek_Borger (tidligere_cpr "
        "fallback, no raw CPR staged), first live marker and counters."
    ),
)
def pre_stg_Helbredsforloeb(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)

    # The delta window is the MIN over ALL downstream checkpoint owners, so an
    # owner that fell behind (failed run) gets its rows re-staged until it
    # catches up — otherwise its members would never be presented again and
    # a fact would point at a dimension without a row.
    downstream = tuple(
        dfm.get_checkpoint(schema, table) for schema, table in DOWNSTREAM_CHECKPOINT_EJERE
    )
    # Decision U11 (deviates from pre_stg_DRGKontakt, which skips None): an owner
    # WITHOUT a checkpoint has never completed a run (or failed its first one).
    # Skipping it would let the other owners' checkpoints shrink the delta
    # window to "nothing changed", so the lagging owner never sees the rows it
    # missed and its whole-table control total fails on every run until someone
    # runs full_load by hand. One full load after a partial failure is the
    # cheaper failure mode.
    checkpoint = None if any(cp is None for cp in downstream) else min(downstream)
    if config.mode == "full_load":
        checkpoint = None

    run_ctx = dfm.start_dfm_run(
        "pre_stg", "pre_Helbredsforloeb", dagster_run_id=context.run_id, mode=config.mode
    )
    if checkpoint is not None:
        context.log.info(f"Delta scope: helbredsforloeb with arc rows touched after {checkpoint}")
    else:
        context.log.info("No checkpoint — full load from arc")

    row_count = 0
    uden_levende = 0
    coverage: dict[str, int] = {}
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(create_borger_cpr_lookup_tables_sql())
                cursor.execute(build_pre_helbredsforloeb_sql(checkpoint))
                conn.commit()

                cursor.execute(
                    f"SELECT COUNT(*), SUM(CASE WHEN [dw_is_deleted] = 1 THEN 1 ELSE 0 END) "
                    f"FROM {PRE_HF_TABLE}"
                )
                row_count, uden_levende = (int(v or 0) for v in cursor.fetchone())

                cursor.execute(build_borger_coverage_sql())
                for opslag, antal in cursor.fetchall():
                    coverage[str(opslag)] = antal
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    unresolved = coverage.get("ukendt", 0)
    fallback = coverage.get("tidligere_cpr", 0)
    context.log.info(
        f"stg.pre_Helbredsforloeb: {row_count:,} rows ({uden_levende:,} without live "
        f"markers); borger lookup: {fallback:,} via tidligere_cpr, {unresolved:,} unresolved (-1)"
    )
    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "checkpoint": str(checkpoint) if checkpoint else "full load",
            "helbredsforloeb_uden_levende_markoerer": uden_levende,
            "borger_opslag": coverage,
            "borger_uoploeste": unresolved,
            "borger_via_tidligere_cpr": fallback,
        }
    )
