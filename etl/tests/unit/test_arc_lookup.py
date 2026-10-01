# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the chunked MSSQL archive IN-clause lookup helper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from DSA.FHIR.utils.ifdb.arc_lookup import (
    MSSQL_IN_CHUNK_LIMIT,
    chunked_in_fetch,
    chunked_in_fetch_grouped,
)


class _FakeCursor:
    """Records ``execute`` calls and serves stubbed result sets per call."""

    def __init__(self, result_sets: list[list[tuple]], description: list[tuple]):
        self._result_sets = list(result_sets)
        self.description = description
        self.executes: list[tuple[str, list]] = []

    def execute(self, query: str, params=None):  # noqa: ANN001
        self.executes.append((query, list(params) if params is not None else []))
        self._current = self._result_sets.pop(0) if self._result_sets else []

    def fetchall(self):
        return self._current

    def close(self):
        pass


class _FakeMssqlResource:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def get_connection(self):
        cursor = self._cursor
        conn = MagicMock()
        conn.cursor.return_value = cursor

        class _Ctx:
            def __enter__(self_inner):
                return conn

            def __exit__(self_inner, *exc):
                return False

        return _Ctx()


def _make_resource(result_sets, description):
    cursor = _FakeCursor(result_sets, description)
    return _FakeMssqlResource(cursor), cursor


def test_empty_ids_returns_empty_without_cursor():
    res, cursor = _make_resource([], [])
    out = chunked_in_fetch(
        [],
        table="arc.ifdb_x",
        columns=["A", "B"],
        where_column="A",
        mssql=res,
    )
    assert out == []
    assert cursor.executes == []


def test_all_none_ids_returns_empty():
    res, cursor = _make_resource([], [])
    out = chunked_in_fetch(
        [None, None],
        table="arc.ifdb_x",
        columns=["A"],
        where_column="A",
        mssql=res,
    )
    assert out == []
    assert cursor.executes == []


def test_single_chunk_brackets_identifiers_and_binds_params():
    res, cursor = _make_resource(
        [[(1, "a"), (2, "b")]],
        [("ID", None), ("VAL", None)],
    )
    out = chunked_in_fetch(
        ["1", "2"],
        table="arc.ifdb_x",
        columns=["ID", "VAL"],
        where_column="ID",
        mssql=res,
    )
    assert out == [{"ID": 1, "VAL": "a"}, {"ID": 2, "VAL": "b"}]
    assert len(cursor.executes) == 1
    query, params = cursor.executes[0]
    # De baerende egenskaber, ikke den eksakte streng: identifiers er
    # bracket-quotede, og vaerdier bindes positionelt (aldrig interpoleret).
    assert "[arc].[ifdb_x]" in query and "[ID]" in query and "[VAL]" in query
    assert "IN (?, ?)" in query
    assert params == ["1", "2"]
    assert "'1'" not in query and "'2'" not in query


def test_filters_deleted_rows_by_default():
    res, cursor = _make_resource([[]], [("ID", None)])
    chunked_in_fetch(
        ["1"],
        table="arc.ifdb_x",
        columns=["ID"],
        where_column="ID",
        mssql=res,
    )
    assert "[dw_is_deleted] = 0" in cursor.executes[0][0]


def test_include_deleted_drops_soft_delete_filter():
    res, cursor = _make_resource([[]], [("ID", None)])
    chunked_in_fetch(
        ["1"],
        table="arc.ifdb_x",
        columns=["ID"],
        where_column="ID",
        mssql=res,
        include_deleted=True,
    )
    assert "dw_is_deleted" not in cursor.executes[0][0]


def test_dedupes_and_sorts_ids_before_chunking():
    res, cursor = _make_resource([[]], [("ID", None)])
    chunked_in_fetch(
        ["b", "a", "b", None, "a"],
        table="arc.t",
        columns=["ID"],
        where_column="ID",
        mssql=res,
    )
    _, params = cursor.executes[0]
    assert params == ["a", "b"]


def test_multi_chunk_splits_at_chunk_size():
    n = MSSQL_IN_CHUNK_LIMIT + 5
    ids = [str(i) for i in range(n)]
    res, cursor = _make_resource(
        [
            [(int(i),) for i in sorted(ids)[:MSSQL_IN_CHUNK_LIMIT]],
            [(int(i),) for i in sorted(ids)[MSSQL_IN_CHUNK_LIMIT:]],
        ],
        [("ID", None)],
    )
    out = chunked_in_fetch(
        ids,
        table="arc.t",
        columns=["ID"],
        where_column="ID",
        mssql=res,
    )
    assert len(out) == n
    assert len(cursor.executes) == 2
    expected = sorted(ids)
    assert cursor.executes[0][1] == expected[:MSSQL_IN_CHUNK_LIMIT]
    assert cursor.executes[1][1] == expected[MSSQL_IN_CHUNK_LIMIT:]


def test_grouped_groups_by_key_fn_and_sorts_groups():
    rows = [
        ("700", "SC02", 2),
        ("700", "SC01", 1),
        ("701", "SC09", 9),
    ]
    res, cursor = _make_resource(
        [rows],
        [("PARENT_OBJECT_ID", None), ("SUPPL_CODE", None), ("IDX", None)],
    )
    out = chunked_in_fetch_grouped(
        ["700", "701"],
        table="arc.ifdb_suppl",
        columns=["PARENT_OBJECT_ID", "SUPPL_CODE", "IDX"],
        where_column="PARENT_OBJECT_ID",
        mssql=res,
        key_fn=lambda r: r["PARENT_OBJECT_ID"],
        sort_key=lambda r: int(r["IDX"]),
    )
    assert set(out.keys()) == {"700", "701"}
    assert [r["SUPPL_CODE"] for r in out["700"]] == ["SC01", "SC02"]
    assert [r["SUPPL_CODE"] for r in out["701"]] == ["SC09"]


def test_grouped_drops_rows_with_none_key():
    rows = [(None, "X"), ("A", "Y")]
    res, cursor = _make_resource([rows], [("PID", None), ("V", None)])
    out = chunked_in_fetch_grouped(
        ["A"],
        table="arc.t",
        columns=["PID", "V"],
        where_column="PID",
        mssql=res,
        key_fn=lambda r: r["PID"],
    )
    assert list(out.keys()) == ["A"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"table": "1bad", "columns": ["A"], "where_column": "A"},
        {"table": "arc.t; DROP TABLE X", "columns": ["A"], "where_column": "A"},
        {"table": "arc.schema.too.deep", "columns": ["A"], "where_column": "A"},
        {"table": "arc.t", "columns": ["A"], "where_column": "A B"},
        {"table": "arc.t", "columns": ["A--"], "where_column": "A"},
        {"table": "arc.t", "columns": [], "where_column": "A"},
    ],
)
def test_rejects_bad_identifiers(kwargs):
    res, _ = _make_resource([], [])
    with pytest.raises(ValueError):
        chunked_in_fetch(["1"], mssql=res, **kwargs)


@pytest.mark.parametrize("chunk_size", [0, MSSQL_IN_CHUNK_LIMIT + 1])
def test_rejects_out_of_range_chunk_size(chunk_size):
    res, _ = _make_resource([], [])
    with pytest.raises(ValueError):
        chunked_in_fetch(
            ["1"],
            table="arc.t",
            columns=["A"],
            where_column="A",
            mssql=res,
            chunk_size=chunk_size,
        )
