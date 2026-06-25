# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for stg_dim_unspsc_kmd asset."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.staging.stg_dim_unspsc_kmd  # noqa: F401

_module = sys.modules["DSA.SQL.staging.stg_dim_unspsc_kmd"]
BUILD_SQL = _module.BUILD_SQL
stg_dim_unspsc_kmd = _module.stg_dim_unspsc_kmd.op.compute_fn.decorated_fn


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


class TestSql:
    def test_targets_stg(self):
        assert "INTO [stg].[dim_unspsc_kmd]" in BUILD_SQL

    def test_reads_kmd_arc(self):
        assert "[arc].[unspsc_kmd_uddata]" in BUILD_SQL

    def test_filters_by_dw_iscurrent(self):
        assert "[DW_IsCurrent] = 1" in BUILD_SQL

    def test_filters_by_dim_checkpoint(self):
        assert "@wm IS NULL" in BUILD_SQL
        assert "[DW_Timestamp_Update] > CAST(@wm AS DATETIME2)" in BUILD_SQL

    def test_selects_unspscclassid_distinct(self):
        assert "SELECT DISTINCT [UNSPSCClassID]" in BUILD_SQL

    def test_clustered_index(self):
        assert "CREATE CLUSTERED INDEX" in BUILD_SQL
        assert "IX_stg_dim_unspsc_kmd_cc" in BUILD_SQL


class TestExecution:
    def test_uses_dim_checkpoint(self, monkeypatch):
        res, _, _ = _mock_resource(rowcount=7)
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"

        run_ctx = MagicMock()
        run_ctx.checkpoint = None
        dfm = MagicMock()
        dfm.start_dfm_run.return_value = run_ctx
        monkeypatch.setattr(_module, "DfmManager", lambda *a, **k: dfm)

        from core.dfm_config import RunModeConfig

        config = RunModeConfig(mode="delta")
        result = stg_dim_unspsc_kmd(ctx, res, config)
        dfm.start_dfm_run.assert_called_once()
        assert result.metadata["target_table"] == "stg.dim_unspsc_kmd"
