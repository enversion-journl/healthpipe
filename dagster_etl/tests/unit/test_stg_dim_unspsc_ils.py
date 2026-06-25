# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for stg_dim_unspsc_ils asset."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.staging.stg_dim_unspsc_ils  # noqa: F401

_module = sys.modules["DSA.SQL.staging.stg_dim_unspsc_ils"]
BUILD_SQL = _module.BUILD_SQL
stg_dim_unspsc_ils = _module.stg_dim_unspsc_ils.op.compute_fn.decorated_fn


def _mock_resource(rowcount=10):
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


class TestStgIlsSql:
    def test_drops_then_selects_into(self):
        assert "DROP TABLE IF EXISTS [stg].[dim_unspsc_ils]" in BUILD_SQL
        assert "INTO [stg].[dim_unspsc_ils]" in BUILD_SQL

    def test_unions_three_columns(self):
        assert "[arc].[unspsc_ils]" in BUILD_SQL
        assert "[unspc]" in BUILD_SQL
        assert "[unspsc2]" in BUILD_SQL
        assert BUILD_SQL.count("UNION ALL") >= 2

    def test_unspscint_excluded(self):
        assert "[unspscint]" not in BUILD_SQL

    def test_strips_dot_zero_on_unspsc2(self):
        assert "LIKE '%.0'" in BUILD_SQL
        assert "LEN([unspsc2]) - 2" in BUILD_SQL

    def test_filters_by_dim_unspsc_checkpoint(self):
        assert "@wm IS NULL" in BUILD_SQL
        assert "[DW_Timestamp_Update] > CAST(@wm AS DATETIME2)" in BUILD_SQL

    def test_distinct(self):
        assert "SELECT DISTINCT CleanCode_UNSPSC" in BUILD_SQL

    def test_clustered_index_created(self):
        assert "CREATE CLUSTERED INDEX" in BUILD_SQL
        assert "IX_stg_dim_unspsc_ils_cc" in BUILD_SQL


class TestExecution:
    def test_uses_dim_unspsc_checkpoint(self, monkeypatch):
        res, conn, cur = _mock_resource(rowcount=42)
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"

        run_ctx = MagicMock()
        run_ctx.checkpoint = "2026-05-04T00:00:00"
        dfm = MagicMock()
        dfm.start_dfm_run.return_value = run_ctx
        monkeypatch.setattr(_module, "DfmManager", lambda *a, **k: dfm)

        from core.dfm_config import RunModeConfig

        config = RunModeConfig(mode="delta")
        result = stg_dim_unspsc_ils(ctx, res, config)
        dfm.start_dfm_run.assert_called_once()
        assert result.metadata["target_table"] == "stg.dim_unspsc_ils"
