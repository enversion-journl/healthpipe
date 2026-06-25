# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the chunked Oracle IN-clause lookup helper."""

from __future__ import annotations

from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.FHIR.utils.ifdb.oracle_lookup import (
    ORACLE_IN_CHUNK_LIMIT,
    chunked_in_fetch,
    chunked_in_fetch_grouped,
)


def _make_context() -> dg.AssetExecutionContext:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    return ctx


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


class _FakeOracleResource:
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
    return _FakeOracleResource(cursor), cursor


def test_empty_ids_returns_empty_without_cursor():
    res, cursor = _make_resource([], [])
    out = chunked_in_fetch(
        [],
        table="IFDB.X",
        columns=["A", "B"],
        where_column="A",
        ifdb_oracle=res,
        context=_make_context(),
    )
    assert out == []
    assert cursor.executes == []


def test_all_none_ids_returns_empty():
    res, cursor = _make_resource([], [])
    out = chunked_in_fetch(
        [None, None],
        table="IFDB.X",
        columns=["A"],
        where_column="A",
        ifdb_oracle=res,
        context=_make_context(),
    )
    assert out == []
    assert cursor.executes == []


def test_single_chunk_returns_rows_in_order():
    res, cursor = _make_resource(
        [[(1, "a"), (2, "b")]],
        [("ID", None), ("VAL", None)],
    )
    out = chunked_in_fetch(
        ["1", "2"],
        table="IFDB.X",
        columns=["ID", "VAL"],
        where_column="ID",
        ifdb_oracle=res,
        context=_make_context(),
    )
    assert out == [{"ID": 1, "VAL": "a"}, {"ID": 2, "VAL": "b"}]
    assert len(cursor.executes) == 1
    query, params = cursor.executes[0]
    assert "SELECT ID, VAL FROM IFDB.X WHERE ID IN (:1, :2)" in query
    assert params == ["1", "2"]


def test_dedupes_and_sorts_ids_before_chunking():
    res, cursor = _make_resource([[]], [("ID", None)])
    chunked_in_fetch(
        ["b", "a", "b", None, "a"],
        table="T",
        columns=["ID"],
        where_column="ID",
        ifdb_oracle=res,
        context=_make_context(),
    )
    _, params = cursor.executes[0]
    assert params == ["a", "b"]


def test_multi_chunk_splits_at_chunk_size():
    n = ORACLE_IN_CHUNK_LIMIT + 5
    ids = [str(i) for i in range(n)]
    res, cursor = _make_resource(
        [
            [(int(i),) for i in ids[:ORACLE_IN_CHUNK_LIMIT]],
            [(int(i),) for i in ids[ORACLE_IN_CHUNK_LIMIT:]],
        ],
        [("ID", None)],
    )
    out = chunked_in_fetch(
        ids,
        table="T",
        columns=["ID"],
        where_column="ID",
        ifdb_oracle=res,
        context=_make_context(),
    )
    assert len(out) == n
    assert len(cursor.executes) == 2
    # Inputs are deduped+sorted lexicographically before chunking
    expected = sorted(ids)
    assert cursor.executes[0][1] == expected[:ORACLE_IN_CHUNK_LIMIT]
    assert cursor.executes[1][1] == expected[ORACLE_IN_CHUNK_LIMIT:]


def test_grouped_groups_by_key_fn():
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
        table="IFDB.SUPPL",
        columns=["PARENT_OBJECT_ID", "SUPPL_CODE", "IDX"],
        where_column="PARENT_OBJECT_ID",
        ifdb_oracle=res,
        context=_make_context(),
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
        table="T",
        columns=["PID", "V"],
        where_column="PID",
        ifdb_oracle=res,
        context=_make_context(),
        key_fn=lambda r: r["PID"],
    )
    assert list(out.keys()) == ["A"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"table": "1bad", "columns": ["A"], "where_column": "A"},
        {"table": "T; DROP TABLE X", "columns": ["A"], "where_column": "A"},
        {"table": "T", "columns": ["A"], "where_column": "A B"},
        {"table": "T", "columns": ["A--"], "where_column": "A"},
        {"table": "T", "columns": [], "where_column": "A"},
    ],
)
def test_rejects_bad_identifiers(kwargs):
    res, _ = _make_resource([], [])
    with pytest.raises(ValueError):
        chunked_in_fetch(["1"], ifdb_oracle=res, context=_make_context(), **kwargs)


def test_rejects_chunk_size_over_oracle_limit():
    res, _ = _make_resource([], [])
    with pytest.raises(ValueError):
        chunked_in_fetch(
            ["1"],
            table="T",
            columns=["A"],
            where_column="A",
            ifdb_oracle=res,
            context=_make_context(),
            chunk_size=ORACLE_IN_CHUNK_LIMIT + 1,
        )


def test_rejects_zero_chunk_size():
    res, _ = _make_resource([], [])
    with pytest.raises(ValueError):
        chunked_in_fetch(
            ["1"],
            table="T",
            columns=["A"],
            where_column="A",
            ifdb_oracle=res,
            context=_make_context(),
            chunk_size=0,
        )


def test_schema_qualified_identifiers_accepted():
    res, cursor = _make_resource([[]], [("A", None)])
    chunked_in_fetch(
        ["1"],
        table="IFDB.PATIENT_OPLYSNINGER_COLUMNA",
        columns=["A"],
        where_column="A",
        ifdb_oracle=res,
        context=_make_context(),
    )
    assert "FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA" in cursor.executes[0][0]
