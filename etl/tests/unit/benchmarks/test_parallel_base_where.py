# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Windowed SELECTs and planners compose with an optional base WHERE predicate."""

import pytest
from DSI.connectors.mssql.parallel_extract import (
    KeysetRange,
    build_keyset_select,
    plan_keyset_windows,
)

_BASE = "[FK] IN (SELECT id FROM x)"


@pytest.mark.unit
def test_keyset_select_combines_base_and_window():
    sql = build_keyset_select(["A"], "dbo", "t", "Id", KeysetRange(10, 20, False), base_where=_BASE)
    assert f"WHERE ({_BASE}) AND [Id] >= 10 AND [Id] < 20" in sql


@pytest.mark.unit
def test_keyset_select_base_only_on_unbounded_last():
    sql = build_keyset_select(
        ["A"], "dbo", "t", "Id", KeysetRange(20, None, True), base_where=_BASE
    )
    assert f"WHERE ({_BASE}) AND [Id] >= 20" in sql
    assert "<" not in sql.split("WHERE")[1]


class _Cur:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, *a):
        self.sql = sql

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _Conn:
    def __init__(self, rows):
        self._rows = rows
        self.last = None

    def cursor(self):
        self.last = _Cur(self._rows)
        return self.last


@pytest.mark.unit
def test_plan_keyset_windows_full_load_uses_minmax_no_sort():
    # Full load (integer key) plans via MIN/MAX range split — no sort, no NTILE,
    # no TOP, and therefore no illegal inner ORDER BY (the bug this guards).
    conn = _Conn([(1, 1000)])
    plan_keyset_windows(conn, "dbo", "t", "Id", workers=4, row_limit=None)
    sql = conn.last.sql
    assert "MIN([Id])" in sql and "MAX([Id])" in sql
    assert "ORDER BY" not in sql
    assert "NTILE" not in sql and "TOP" not in sql


@pytest.mark.unit
def test_plan_keyset_windows_full_load_nonint_key_falls_back_to_ntile():
    # Non-integer key => range split returns None => fall back to NTILE. Even
    # then the full-load NTILE must omit the illegal inner ORDER BY (no TOP).
    # Row shape serves both queries: MIN/MAX reads [0] (non-int -> fallback);
    # the NTILE result is parsed as (bucket, lo, hi).
    conn = _Conn([("a", 1, 5)])
    plan_keyset_windows(conn, "dbo", "t", "code", workers=4, row_limit=None)
    sql = conn.last.sql  # last execute = the NTILE query
    assert "NTILE" in sql and "TOP" not in sql
    assert sql.count("ORDER BY") == 2  # OVER + outer; no illegal inner ORDER BY


@pytest.mark.unit
def test_plan_keyset_windows_with_row_limit_keeps_top_and_inner_order_by():
    conn = _Conn([])
    plan_keyset_windows(conn, "dbo", "t", "Id", workers=4, row_limit=1000)
    sql = conn.last.sql
    assert "TOP (1000)" in sql
    assert sql.count("ORDER BY") == 3  # inner (for TOP) + NTILE OVER + outer
