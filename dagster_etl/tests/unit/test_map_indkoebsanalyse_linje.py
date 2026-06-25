# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for map_IndkoebsanalyseLinje asset."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.map.MapUpd_IndkoebsanalyseLinje  # noqa: F401

_module = sys.modules["DSA.SQL.map.MapUpd_IndkoebsanalyseLinje"]
INSERT_SQL = _module.INSERT_SQL
map_IndkoebsanalyseLinje = _module.map_IndkoebsanalyseLinje.op.compute_fn.decorated_fn


def _mock_resource(rowcount=3):
    cur = MagicMock()
    cur.rowcount = rowcount
    cur.fetchone.side_effect = [(10,), (rowcount,)]
    conn = MagicMock()
    conn.cursor.return_value = cur
    res = MagicMock()

    @contextmanager
    def _g():
        yield conn

    res.get_connection = _g
    return res, conn, cur


class TestInsertSql:
    def test_inserts_into_map(self):
        assert "INSERT INTO [map].[IndkoebsanalyseLinje]" in INSERT_SQL

    def test_reads_stg(self):
        assert "[stg].[fct_indkoebsanalyse_linje_kmd]" in INSERT_SQL

    def test_where_not_exists_guard(self):
        assert "NOT EXISTS" in INSERT_SQL

    def test_no_merge_keyword(self):
        assert "MERGE" not in INSERT_SQL.upper()


class TestExecution:
    def test_returns_metadata(self):
        res, _, _ = _mock_resource(rowcount=2)
        ctx = MagicMock()
        ctx.log = MagicMock()
        result = map_IndkoebsanalyseLinje(ctx, res)
        assert result.metadata["table"] == "map.IndkoebsanalyseLinje"
        assert result.metadata["new_rows"] == 2
