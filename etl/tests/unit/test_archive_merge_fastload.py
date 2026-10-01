# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Tests for the empty-target fast load path in the archive layer.

When an arc table is empty (first load), execute_archive_merge bypasses the
MERGE (with its OUTPUT $action table-variable tracking and per-row change
detection) in favour of a single ``INSERT INTO arc WITH (TABLOCK) SELECT ...
FROM ext``, which reads the ext heap exactly once (O(n)). ext is not consumed —
like the MERGE path, it keeps its rows until the next extract drops and
recreates it. Nonclustered indexes on arc are disabled for the load and rebuilt
once at the end — both a performance measure and a precondition for minimal
logging of the TABLOCK insert. See _build_empty_target_fastload_sql.
"""

import dataclasses
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from core.archive_merge import (
    _build_empty_target_fastload_sql,
    execute_archive_merge,
)
from core.dfm_config import ArchiveMergeConfig, RunModeConfig

pytestmark = pytest.mark.unit

# ===========================================================================
# Fixtures
# ===========================================================================

FASTLOAD_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="mysupply_statistic_invoice_header",
    target_schema="arc",
    target_table="mysupply_statistic_invoice_header",
    merge_keys=("id",),
    non_key_columns=("faktura_nr", "FakturaDato"),
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    flow_start_key=("dfm", "mysupply_statistic_invoice_header_flow_start"),
    enable_bulk_insert_on_first_load=True,
)

# Same shape but the opt-in flag is left at its default (False): an empty target
# must still take the safe MERGE path, because the ext table is not guaranteed
# to satisfy the arc primary key.
NO_FASTLOAD_CONFIG = dataclasses.replace(FASTLOAD_CONFIG, enable_bulk_insert_on_first_load=False)


def _make_context():
    ctx = MagicMock()
    ctx.run_id = "test-run-fastload"
    ctx.log = MagicMock()
    return ctx


class _FakeCursor:
    """Fake cursor that drives probe / fast-load / merge behaviour off the SQL text."""

    def __init__(self, *, empty, insert_rowcount=0, action_row=None, index_names=()):
        self._empty = empty
        self._insert_rowcount = insert_rowcount
        self._action_row = action_row
        self._index_names = list(index_names)
        self._last = None
        self.rowcount = -1
        self.executed: list[str] = []

    def execute(self, sql, *args, **kwargs):
        self.executed.append(sql)
        head = sql.strip().upper()
        if head.startswith("SELECT TOP (1) 1"):
            self._last = "probe"
        elif head.startswith("SELECT I.NAME FROM SYS.INDEXES"):
            self._last = "index_names"
        elif head.startswith("INSERT INTO"):
            self._last = "fastload_insert"
            self.rowcount = self._insert_rowcount
        elif head.startswith("MERGE") or head.startswith("DECLARE"):
            self._last = "merge"
        else:
            self._last = "other"

    def fetchone(self):
        if self._last == "probe":
            return None if self._empty else (1,)
        if self._last == "merge":
            return self._action_row
        return None

    def fetchall(self):
        if self._last == "index_names":
            return [(name,) for name in self._index_names]
        return []

    def nextset(self):
        return True

    def close(self):
        pass


def _make_mssql(cursor):
    mssql = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.commit = MagicMock()
    cursor._conn = conn

    @contextmanager
    def _get_conn():
        yield conn

    mssql.get_connection = _get_conn
    return mssql, conn


def _fastload_inserts(cursor):
    return [s for s in cursor.executed if s.strip().upper().startswith("INSERT INTO")]


def _merges(cursor):
    return [
        s
        for s in cursor.executed
        if s.strip().upper().startswith("MERGE") or "MERGE [ARC]" in s.upper()
    ]


def _index_ddls(cursor):
    return [s for s in cursor.executed if s.strip().upper().startswith("ALTER INDEX")]


# ===========================================================================
# SQL builder
# ===========================================================================


class TestFastloadSqlBuilder:
    def test_emits_single_insert_select_with_tablock(self):
        sql = _build_empty_target_fastload_sql(FASTLOAD_CONFIG)

        assert "INSERT INTO [arc].[mysupply_statistic_invoice_header] WITH (TABLOCK)" in sql
        assert "FROM [ext].[mysupply_statistic_invoice_header]" in sql
        # Single-pass copy: no batching, no self-consuming DELETE.
        assert "DELETE" not in sql.upper()
        assert "TOP (" not in sql.upper()

    def test_selects_all_key_and_non_key_columns(self):
        sql = _build_empty_target_fastload_sql(FASTLOAD_CONFIG)

        for col in ("id", "faktura_nr", "FakturaDato"):
            assert sql.count(f"[{col}]") == 2

    def test_stamps_both_dw_timestamps(self):
        sql = _build_empty_target_fastload_sql(FASTLOAD_CONFIG)

        assert "[dw_timestamp_insert], [dw_timestamp_update]" in sql
        assert sql.count("CAST(SYSDATETIME() AS datetime2(0))") == 2

    def test_no_merge_or_change_detection(self):
        sql = _build_empty_target_fastload_sql(FASTLOAD_CONFIG)

        assert "MERGE" not in sql.upper()
        assert "$action" not in sql
        assert "NULLIF" not in sql


# ===========================================================================
# execute_archive_merge integration
# ===========================================================================


class TestEmptyTargetFastPath:
    @patch("core.archive_merge.DfmManager")
    def test_empty_target_uses_single_insert_select(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 00:00:00"
        MockDfmManager.return_value = mock_dfm

        cursor = _FakeCursor(empty=True, insert_rowcount=3)
        mssql, conn = _make_mssql(cursor)
        ctx = _make_context()

        result = execute_archive_merge(ctx, mssql, FASTLOAD_CONFIG, RunModeConfig(mode="delta"))

        # One INSERT...SELECT, no MERGE — and ext is left untouched (no
        # TRUNCATE/DELETE; it keeps its rows exactly like the MERGE path).
        assert len(_fastload_inserts(cursor)) == 1
        assert _merges(cursor) == []
        assert not any(
            s.strip().upper().startswith(("TRUNCATE", "DELETE")) for s in cursor.executed
        )
        assert result.rows_inserted == 3
        assert result.rows_updated == 0
        assert result.rows_soft_deleted == 0
        assert result.row_count == 3
        assert conn.commit.call_count >= 1

    @patch("core.archive_merge.DfmManager")
    def test_fast_load_disables_then_rebuilds_nonclustered_indexes(self, MockDfmManager):
        """Nonclustered indexes are disabled before the load and rebuilt after,
        so per-row B-tree maintenance is replaced by one sorted rebuild (also a
        precondition for minimal logging of the TABLOCK insert)."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 00:00:00"
        MockDfmManager.return_value = mock_dfm

        cursor = _FakeCursor(
            empty=True,
            insert_rowcount=3,
            index_names=("IX_ts", "IX_header_id"),
        )
        mssql, _ = _make_mssql(cursor)
        ctx = _make_context()

        execute_archive_merge(ctx, mssql, FASTLOAD_CONFIG, RunModeConfig(mode="delta"))

        ddls = _index_ddls(cursor)
        disables = [s for s in ddls if "DISABLE" in s.upper()]
        rebuilds = [s for s in ddls if "REBUILD" in s.upper()]
        assert len(disables) == 2
        assert len(rebuilds) == 2

        # Both indexes are disabled before the INSERT and rebuilt after it.
        exec_upper = [s.strip().upper() for s in cursor.executed]
        insert_idx = next(i for i, s in enumerate(exec_upper) if s.startswith("INSERT INTO"))
        disable_idx = [i for i, s in enumerate(exec_upper) if "DISABLE" in s]
        rebuild_idx = [i for i, s in enumerate(exec_upper) if "REBUILD" in s]
        assert all(i < insert_idx for i in disable_idx)
        assert all(i > insert_idx for i in rebuild_idx)

    @patch("core.archive_merge.DfmManager")
    def test_empty_target_still_advances_checkpoint(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 00:00:00"
        MockDfmManager.return_value = mock_dfm

        cursor = _FakeCursor(empty=True, insert_rowcount=3)
        mssql, _ = _make_mssql(cursor)
        ctx = _make_context()

        result = execute_archive_merge(ctx, mssql, FASTLOAD_CONFIG, RunModeConfig(mode="delta"))

        assert result.new_checkpoint == "2025-06-01 00:00:00"
        mock_dfm.set_checkpoint.assert_called_once_with(
            "arc",
            "mysupply_statistic_invoice_header",
            value="2025-06-01 00:00:00",
            mode="delta",
        )

    @patch("core.archive_merge.DfmManager")
    def test_nonempty_target_uses_merge(self, MockDfmManager):
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 00:00:00"
        MockDfmManager.return_value = mock_dfm

        cursor = _FakeCursor(empty=False, action_row=(5, 3, 0))
        mssql, _ = _make_mssql(cursor)
        ctx = _make_context()

        result = execute_archive_merge(ctx, mssql, FASTLOAD_CONFIG, RunModeConfig(mode="delta"))

        assert _fastload_inserts(cursor) == []
        assert len(_merges(cursor)) == 1
        assert result.rows_inserted == 5
        assert result.rows_updated == 3

    @patch("core.archive_merge.DfmManager")
    def test_empty_target_without_flag_still_uses_merge(self, MockDfmManager):
        """The fast path is opt-in: without enable_bulk_insert_on_first_load,
        even an empty target takes the safe MERGE path (ext may violate the
        arc primary key)."""
        mock_dfm = MagicMock()
        mock_dfm.start_dfm_run.return_value = MagicMock(log_id=1, checkpoint=None)
        mock_dfm.get_checkpoint.return_value = "2025-06-01 00:00:00"
        MockDfmManager.return_value = mock_dfm

        cursor = _FakeCursor(empty=True, action_row=(9, 0, 0))
        mssql, _ = _make_mssql(cursor)
        ctx = _make_context()

        result = execute_archive_merge(ctx, mssql, NO_FASTLOAD_CONFIG, RunModeConfig(mode="delta"))

        assert _fastload_inserts(cursor) == []
        assert len(_merges(cursor)) == 1
        assert result.rows_inserted == 9
