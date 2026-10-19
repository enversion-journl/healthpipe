# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.ILSAendringslog — ILS ændringslog (dbo.changelog) as-is.

One table for every tabelnavn, so new tables added to the source log flow
through without code changes. Point-in-time lookups (a vare's basisenhed or
delaftale at a given time) and dimension joins belong in views on top.

Delta: MERGE of arc rows changed since the ref DFM checkpoint (~9M rows, so no
full rebuild per run). full_load / first run reads the whole archive.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.ref.ils_aendringslog_sql import (
    REF_TABLE,
    build_invalid_klok_count_sql,
    build_merge_sql,
)


def _execute(cursor, sql: str, params: tuple | None) -> None:
    """Run sql with the delta checkpoint bound, or unbound for full scope."""
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)


@dg.asset(
    name="ref_ILSAendringslog",
    group_name="erp_ref",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["arc_ils_changelog"]),
        dg.AssetKey(["staging_infra_ref"]),
    ],
    tags={"domain": "erp", "layer": "ref", "source": "ils", "entity": "ILSAendringslog"},
    description=(
        "ref.ILSAendringslog — ILS dbo.changelog as-is: one row per change event, "
        "raw noegle without FK resolution; own DFM checkpoint on arc."
    ),
)
def ref_ILSAendringslog(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ref",
        "ILSAendringslog",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    checkpoint = run_ctx.checkpoint
    full_scope = config.mode == "full_load" or checkpoint is None
    params = None if full_scope else (checkpoint,)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _execute(cursor, build_merge_sql(full_scope=full_scope), params)
                merge_affected = cursor.rowcount
                _execute(cursor, build_invalid_klok_count_sql(full_scope=full_scope), params)
                invalid_klok = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT_BIG(*) FROM {REF_TABLE}")
                row_count = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        # The read start, held back to any archive MERGE still in flight — not
        # MAX(arc.dw_timestamp_update), which can pass rows not yet committed.
        new_checkpoint = None
        if config.should_advance_checkpoint:
            new_checkpoint = dfm.safe_reader_checkpoint(run_ctx.started_at)
            if new_checkpoint:
                dfm.set_checkpoint(
                    "ref",
                    "ILSAendringslog",
                    value=new_checkpoint,
                    mode=config.mode,
                )

        if invalid_klok:
            context.log.warning(
                f"{REF_TABLE}: {invalid_klok} rows in scope have klok outside 0..86399 "
                "and got tidspunkt NULL — check arc.ils_changelog.klok."
            )
        context.log.info(
            f"{REF_TABLE}: {merge_affected} rows touched by MERGE, {row_count} total "
            f"(scope={'full' if full_scope else 'delta'}, "
            f"checkpoint {checkpoint} → {new_checkpoint})"
        )
        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=merge_affected,
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "merge_affected": merge_affected,
            "row_count": row_count,
            "invalid_klok_rows": invalid_klok,
            "target_table": "ref.ILSAendringslog",
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "full_scope": full_scope,
            "mode": config.mode,
        }
    )
