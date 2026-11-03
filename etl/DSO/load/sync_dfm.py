# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO sync integration with DfmManager (scope='dso').

Checkpoint semantics (Strategy B, pinned window):
  1. Before the MERGE, sample max_now = MAX(COALESCE(wm, insert)) on staging.
  2. Delta source filter: COALESCE(...) > checkpoint AND COALESCE(...) <= max_now.
  3. On success with rows > 0, set_checkpoint(max_now) — never re-sample MAX after
     the MERGE (avoids advancing past rows that landed mid-sync and were not read).

Exclusive ``>`` on the lower bound is unchanged (see dfm_design.md §8).
"""

from __future__ import annotations

from contextlib import AbstractContextManager

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSO.load.sync_configs import SyncTableConfig
from DSO.load.sync_utils import (
    _find_insert_timestamp_col,
    _find_update_timestamp_col,
)

DSO_DFM_SCOPE = "dso"


def resolve_dfm_mode(
    config: SyncTableConfig,
    run_mode_config: RunModeConfig | None = None,
) -> str:
    """Map SyncTableConfig.mode (+ optional RunModeConfig) to a DFM run log mode."""
    if run_mode_config is not None and run_mode_config.mode in ("delta", "full_load"):
        return run_mode_config.mode
    if config.mode == "watermark":
        return "delta"
    return "full_load"


def watermark_column_names(
    config: SyncTableConfig,
    columns: list[str],
) -> tuple[str, str]:
    """Return (watermark_col, insert_fallback_col) for delta filtering."""
    update_ts_col = _find_update_timestamp_col(columns)
    insert_ts_col = _find_insert_timestamp_col(columns)
    watermark_col = config.watermark_column or update_ts_col or "dw_timestamp_update"
    insert_fallback = insert_ts_col or "dw_timestamp_insert"
    return watermark_col, insert_fallback


def build_source_max_checkpoint_sql(
    config: SyncTableConfig,
    columns: list[str],
) -> str:
    """SQL to sample MAX(COALESCE(wm, insert)) from the staging source table."""
    watermark_col, insert_fallback = watermark_column_names(config, columns)
    schema = config.staging_schema
    table = config.staging_table
    return (
        f"SELECT CONVERT(NVARCHAR(50), "
        f"MAX(COALESCE([{watermark_col}], [{insert_fallback}])), 121) "
        f"FROM [{schema}].[{table}]"
    )


def query_source_checkpoint(
    mssql_staging: MSSQLResource,
    config: SyncTableConfig,
    columns: list[str],
) -> str | None:
    """Sample MAX(source) on staging *before* the MERGE. Returns None if empty."""
    sql = build_source_max_checkpoint_sql(config, columns)
    with mssql_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is None or row[0] is None:
                return None
            return str(row[0])
        finally:
            cursor.close()


def finalize_dfm_run(
    dfm: DfmManager,
    run_ctx,
    config: SyncTableConfig,
    columns: list[str],
    mssql_staging: MSSQLResource,
    *,
    rows_affected: int,
    dfm_mode: str,
    checkpoint_upper_bound: str | None = None,
) -> str | None:
    """Complete a DFM run; advance checkpoint only for watermark syncs with rows > 0.

    ``checkpoint_upper_bound`` must be the pre-MERGE MAX sample (pinned window).
    Do not re-query MAX after the MERGE — that can advance past unread rows.
    """
    del columns, mssql_staging  # retained for call-site compatibility
    checkpoint_before = run_ctx.checkpoint
    if config.mode != "watermark":
        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=checkpoint_before,
            rows_affected=rows_affected,
        )
        return checkpoint_before

    if rows_affected == 0:
        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=checkpoint_before,
            rows_affected=0,
        )
        return checkpoint_before

    new_checkpoint = checkpoint_upper_bound
    if new_checkpoint is None:
        # Keep DFMRunLog aligned with DFMState (set_checkpoint was skipped).
        dfm._context.log.warning(
            f"Watermark sync {config.schema}.{config.table} affected "
            f"{rows_affected} row(s) but source MAX(COALESCE(...)) is NULL; "
            f"leaving checkpoint at {checkpoint_before!r}."
        )
    else:
        dfm.set_checkpoint(
            config.schema,
            config.table,
            value=new_checkpoint,
            mode=dfm_mode,
            scope=DSO_DFM_SCOPE,
        )
    checkpoint_after = new_checkpoint or checkpoint_before
    dfm.complete_dfm_run(
        run_ctx.log_id,
        checkpoint_after=checkpoint_after,
        rows_affected=rows_affected,
    )
    return checkpoint_after


class DsoSyncDfmSession:
    """Manages DFM run lifecycle for one DSO sync asset execution."""

    def __init__(
        self,
        mssql_staging: MSSQLResource,
        context: dg.AssetExecutionContext,
    ) -> None:
        self.dfm = DfmManager(mssql_staging, context)
        self.context = context
        self.mssql_staging = mssql_staging
        self._conn_ctx: AbstractContextManager[DfmManager] | None = None

    def __enter__(self) -> DsoSyncDfmSession:
        self._conn_ctx = self.dfm.connection()
        self._conn_ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn_ctx is not None:
            self._conn_ctx.__exit__(exc_type, exc_val, exc_tb)

    def start_run(
        self,
        config: SyncTableConfig,
        dfm_mode: str,
    ):
        return self.dfm.start_dfm_run(
            config.schema,
            config.table,
            dagster_run_id=self.context.run_id,
            mode=dfm_mode,
            scope=DSO_DFM_SCOPE,
        )

    def fail_run(self, log_id: int, error_message: str) -> None:
        self.dfm.fail_dfm_run(log_id, error_message=error_message)
