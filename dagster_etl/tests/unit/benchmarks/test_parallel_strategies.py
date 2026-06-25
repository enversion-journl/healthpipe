# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Window planning, SQL building, and ordering-key autodetect."""

import pytest
from DSI.connectors.mssql.parallel_extract import (
    KeysetRange,
    _sql_literal,
    build_keyset_select,
    detect_ordering_key,
    plan_keyset_windows,
)


@pytest.mark.unit
def test_build_keyset_select_half_open_inner_unbounded_and_capped_last():
    sql_mid = build_keyset_select(["A"], "dbo", "t", "Id", KeysetRange(10, 20, False))
    assert "[Id] >= 10 AND [Id] < 20" in sql_mid
    # Last range with no upper bound => full-load tail (no upper predicate).
    sql_last = build_keyset_select(["A"], "dbo", "t", "Id", KeysetRange(20, None, True))
    assert "[Id] >= 20" in sql_last
    assert "<" not in sql_last.split("WHERE")[1]
    # Last range WITH an upper bound => row_limit cap, inclusive (<=).
    sql_cap = build_keyset_select(["A"], "dbo", "t", "Id", KeysetRange(20, 30, True))
    assert "[Id] >= 20 AND [Id] <= 30" in sql_cap


@pytest.mark.unit
def test_sql_literal_quotes_strings_passes_ints():
    assert _sql_literal(5) == "5"
    assert _sql_literal("a'b") == "'a''b'"


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self._desc = None

    def execute(self, sql, *params):
        self._sql = sql

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)


@pytest.mark.unit
def test_detect_ordering_key_prefers_clustered_unique():
    # rows: (column_name, is_unique)
    conn = _FakeConn([("Id", True)])
    key, unique = detect_ordering_key(conn, "dbo", "statistic_invoice_header", override=None)
    assert key == "Id"
    assert unique is True


@pytest.mark.unit
def test_detect_ordering_key_honors_override():
    conn = _FakeConn([])
    key, unique = detect_ordering_key(conn, "dbo", "t", override="FakturaDato")
    assert key == "FakturaDato"
    assert unique is False


class _SeqCursor:
    """Returns different rows per query, keyed by a marker substring in the SQL."""

    def __init__(self, by_marker):
        self._by_marker = by_marker
        self._rows = []

    def execute(self, sql, *a):
        self._rows = next((rows for marker, rows in self._by_marker if marker in sql), [])

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _SeqConn:
    def __init__(self, by_marker):
        self._by_marker = by_marker

    def cursor(self):
        return _SeqCursor(self._by_marker)


@pytest.mark.unit
def test_detect_ordering_key_falls_back_to_primary_key():
    # No clustered index -> use the PK leading column.
    conn = _SeqConn([("is_primary_key = 1", [("PkCol", True)])])
    key, unique = detect_ordering_key(conn, "dbo", "t", override=None)
    assert key == "PkCol"
    assert unique is True


@pytest.mark.unit
def test_detect_ordering_key_falls_back_to_first_column():
    # No clustered index, no PK -> first column by ordinal (not unique).
    conn = _SeqConn([("INFORMATION_SCHEMA", [("FirstCol",)])])
    key, unique = detect_ordering_key(conn, "dbo", "t", override=None)
    assert key == "FirstCol"
    assert unique is False


@pytest.mark.unit
def test_detect_ordering_key_raises_when_no_columns():
    with pytest.raises(ValueError):
        detect_ordering_key(_SeqConn([]), "dbo", "t", override=None)


@pytest.mark.unit
def test_plan_keyset_windows_full_load_uses_minmax_range_split():
    # Full load (row_limit=None) now plans via a cheap MIN/MAX integer range
    # split (no NTILE full-table sort). MIN=1, MAX=10, workers=3 => width=ceil(10/3)=4.
    conn = _FakeConn([(1, 10)])
    ranges = plan_keyset_windows(conn, "dbo", "t", "Id", workers=3, row_limit=None)
    assert ranges == [
        KeysetRange(1, 5, False, is_first=True),  # first window also sweeps NULL keys
        KeysetRange(5, 9, False),
        KeysetRange(9, None, True),  # last window unbounded => covers all rows
    ]


@pytest.mark.unit
def test_plan_keyset_windows_full_load_single_worker_is_unbounded():
    conn = _FakeConn([(1, 1000)])
    ranges = plan_keyset_windows(conn, "dbo", "t", "Id", workers=1, row_limit=None)
    assert ranges == [KeysetRange(1, None, True, is_first=True)]


@pytest.mark.unit
def test_plan_keyset_windows_caps_last_range_when_row_limit_set():
    # With a row_limit, the last range must be capped at the subset max key
    # (here 39) — otherwise the unbounded tail would read the entire table.
    conn = _FakeConn([(1, 10, 19), (2, 20, 29), (3, 30, 39)])
    ranges = plan_keyset_windows(conn, "dbo", "t", "Id", workers=3, row_limit=1_000_000)
    assert ranges == [
        KeysetRange(10, 20, False, is_first=True),
        KeysetRange(20, 30, False),
        KeysetRange(30, 39, True),
    ]


@pytest.mark.unit
def test_plan_keyset_windows_empty_result_is_single_unbounded_range():
    conn = _FakeConn([])
    ranges = plan_keyset_windows(conn, "dbo", "t", "Id", workers=4, row_limit=2_000_000)
    assert ranges == [KeysetRange(None, None, True, is_first=True)]


@pytest.mark.unit
def test_build_keyset_select_first_window_sweeps_null_keys():
    # NULL keys satisfy no >=/< bound, so without this they'd be extracted by NO
    # window (silent data loss). The first window must also capture IS NULL.
    sql = build_keyset_select(["A"], "dbo", "t", "Id", KeysetRange(1, 100, False, is_first=True))
    assert "([Id] >= 1 AND [Id] < 100 OR [Id] IS NULL)" in sql


@pytest.mark.unit
def test_build_keyset_select_non_first_window_excludes_null_keys():
    sql = build_keyset_select(["A"], "dbo", "t", "Id", KeysetRange(100, 200, False))
    assert "IS NULL" not in sql


@pytest.mark.unit
def test_plan_keyset_windows_marks_only_first_range():
    conn = _FakeConn([(1, 100)])
    ranges = plan_keyset_windows(conn, "dbo", "t", "Id", workers=4, row_limit=None)
    assert ranges[0].is_first is True
    assert all(not r.is_first for r in ranges[1:])
