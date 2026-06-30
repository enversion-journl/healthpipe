# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for stg_fct_indkoebsanalyse_linje_kmd asset."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.staging.stg_fct_indkoebsanalyse_linje_kmd  # noqa: F401

_module = sys.modules["DSA.SQL.staging.stg_fct_indkoebsanalyse_linje_kmd"]
BUILD_SQL = _module.BUILD_SQL
stg_fct_indkoebsanalyse_linje_kmd = (
    _module.stg_fct_indkoebsanalyse_linje_kmd.op.compute_fn.decorated_fn
)


def _mock_resource(rowcount=10, missing_unspsc=0):
    cur = MagicMock()
    cur.rowcount = rowcount
    cur.fetchone.side_effect = [(rowcount,), (missing_unspsc,)]
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
        assert "INTO [stg].[fct_indkoebsanalyse_linje_kmd]" in BUILD_SQL

    def test_reads_spend_analysis_arc(self):
        assert "[arc].[spend_analysis_lines_kmd_uddata]" in BUILD_SQL

    def test_unspsc_join_chain(self):
        assert "[arc].[unspsc_kmd_uddata]" in BUILD_SQL
        assert "[DW_IsCurrent] = 1" in BUILD_SQL
        assert "[map].[UNSPSC]" in BUILD_SQL
        assert "ku.[DW_SK_UNSPSC] = sal.[DW_SK_UNSPSC]" in BUILD_SQL
        assert "um.[CleanCode_UNSPSC] = ku.[UNSPSCClassID]" in BUILD_SQL

    def test_cleancode_pattern(self):
        assert "KMD_SAL_" in BUILD_SQL

    def test_filters_by_fact_watermark(self):
        assert "@wm IS NULL" in BUILD_SQL
        assert "[DW_Timestamp_Update] >= CAST(@wm AS DATETIME2)" in BUILD_SQL

    def test_coalesces_unspsc_to_unknown(self):
        assert "COALESCE(um.[DW_EK_UNSPSC], -1)" in BUILD_SQL

    def test_clustered_index(self):
        assert "CREATE CLUSTERED INDEX" in BUILD_SQL
        assert "IX_stg_fct_indkoebsanalyse_linje_kmd_cc" in BUILD_SQL


class TestExecution:
    def test_uses_fact_watermark(self, monkeypatch):
        res, _, _ = _mock_resource(rowcount=5, missing_unspsc=1)
        ctx = MagicMock()
        ctx.log = MagicMock()
        wm = MagicMock()
        wm.get_watermark.return_value = "2026-01-01"
        monkeypatch.setattr(_module, "EtlWatermarkManager", lambda *a, **k: wm)
        result = stg_fct_indkoebsanalyse_linje_kmd(ctx, res)
        wm.get_watermark.assert_called_with("fact", "fct", "IndkoebsanalyseLinje")
        assert result.metadata["target_table"] == "stg.fct_indkoebsanalyse_linje_kmd"
        assert result.metadata["missing_unspsc_fk"] == 1
