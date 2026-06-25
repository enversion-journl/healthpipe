# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for map_UNSPSC asset (sticky source-flag map maintenance)."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.map.MapUpd_UNSPSC  # noqa: F401

_module = sys.modules["DSA.SQL.map.MapUpd_UNSPSC"]
INSERT_SQL = _module.INSERT_SQL
UPDATE_FLAG_ILS_SQL = _module.UPDATE_FLAG_ILS_SQL
UPDATE_FLAG_KMD_SQL = _module.UPDATE_FLAG_KMD_SQL
map_UNSPSC = _module.map_UNSPSC.op.compute_fn.decorated_fn


def _mock_resource(rowcount=3):
    cur = MagicMock()
    cur.rowcount = rowcount
    cur.fetchone.return_value = (rowcount,)
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
        assert "INSERT INTO [map].[UNSPSC]" in INSERT_SQL

    def test_uses_union_all_not_union(self):
        # Per ETL guide: UNION ALL avoids redundant sort
        assert "UNION ALL" in INSERT_SQL
        # No bare 'UNION' (would be a sort+dedup) — only UNION ALL
        without_union_all = INSERT_SQL.replace("UNION ALL", "")
        assert "UNION " not in without_union_all and "UNION\n" not in without_union_all

    def test_where_not_exists_guard(self):
        assert "WHERE NOT EXISTS" in INSERT_SQL

    def test_no_merge_keyword(self):
        # ETL guide: never MERGE for map insert
        assert "MERGE" not in INSERT_SQL.upper()

    def test_groups_by_cleancode(self):
        assert "GROUP BY" in INSERT_SQL


class TestFlagFlipUpdates:
    def test_ils_update_only_flips_zero_to_one(self):
        assert "DW_Source_ILS = 1" in UPDATE_FLAG_ILS_SQL
        assert "DW_Source_ILS = 0" in UPDATE_FLAG_ILS_SQL  # the WHERE filter
        assert "[stg].[dim_unspsc_ils]" in UPDATE_FLAG_ILS_SQL

    def test_kmd_update_only_flips_zero_to_one(self):
        assert "DW_Source_KMD = 1" in UPDATE_FLAG_KMD_SQL
        assert "DW_Source_KMD = 0" in UPDATE_FLAG_KMD_SQL
        assert "[stg].[dim_unspsc_kmd]" in UPDATE_FLAG_KMD_SQL


class TestExecution:
    def test_runs_three_statements(self, monkeypatch):
        res, _, cur = _mock_resource()
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"

        run_ctx = MagicMock()
        dfm = MagicMock()
        dfm.start_dfm_run.return_value = run_ctx
        monkeypatch.setattr(_module, "DfmManager", lambda *a, **k: dfm)

        from core.dfm_config import RunModeConfig

        config = RunModeConfig(mode="delta")
        result = map_UNSPSC(ctx, res, config)
        # 1 INSERT + 2 UPDATEs + final SELECT COUNT — at least 4 executes
        assert cur.execute.call_count >= 4
        assert result.metadata["target_table"] == "map.UNSPSC"


class TestDocstring:
    def test_documents_map_purpose(self):
        doc = (_module.__doc__ or "").lower()
        assert "map" in doc
        assert "unspsc" in doc
