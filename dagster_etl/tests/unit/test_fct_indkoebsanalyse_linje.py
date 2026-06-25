# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for fct_indkoebsanalyse_linje asset."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.fct.FctUpd_indkoebsanalyse_linje  # noqa: F401
import pytest

_module = sys.modules["DSA.SQL.fct.FctUpd_indkoebsanalyse_linje"]
RESOLVE_SQL = _module.RESOLVE_SQL
MERGE_SQL = _module.MERGE_SQL
UNMAPPED_STG_SQL = _module.UNMAPPED_STG_SQL
DIAG_SQL = _module.DIAG_SQL
fct_indkoebsanalyse_linje = _module.fct_indkoebsanalyse_linje.op.compute_fn.decorated_fn


def _mock_resource(stg_count=5, unmapped=False, resolved=5, merged=5, diag=(10, 2)):
    cur = MagicMock()
    cur.rowcount = resolved
    fetch_side_effect = [(stg_count,)]
    if unmapped:
        fetch_side_effect.append((1,))
    else:
        fetch_side_effect.append(None)
    fetch_side_effect.append(diag)
    cur.fetchone.side_effect = fetch_side_effect
    conn = MagicMock()
    conn.cursor.return_value = cur
    res = MagicMock()

    @contextmanager
    def _g():
        yield conn

    res.get_connection = lambda **kwargs: _g()
    return res, conn, cur, MagicMock()


class TestSql:
    def test_resolve_joins_map(self):
        assert "#IndkoebsanalyseLinje_Resolved" in RESOLVE_SQL
        assert "INNER JOIN [map].[IndkoebsanalyseLinje]" in RESOLVE_SQL

    def test_merge_targets_fct(self):
        assert "MERGE [fct].[IndkoebsanalyseLinje]" in MERGE_SQL
        assert "WHEN MATCHED THEN UPDATE" in MERGE_SQL
        assert "WHEN NOT MATCHED BY TARGET THEN INSERT" in MERGE_SQL
        assert "[DW_EK_UNSPSC]" in MERGE_SQL

    def test_resolve_creates_clustered_index_on_temp(self):
        assert "RESOLVE_INDEX_SQL" in dir(_module) or hasattr(_module, "RESOLVE_INDEX_SQL")
        assert "CREATE UNIQUE CLUSTERED INDEX IX_tmp_IndkoebsanalyseLinje_Resolved" in (
            _module.RESOLVE_INDEX_SQL
        )

    def test_fail_fast_unmapped_query(self):
        assert "SELECT TOP 1 1" in UNMAPPED_STG_SQL
        assert "LEFT JOIN [map].[IndkoebsanalyseLinje]" in UNMAPPED_STG_SQL
        assert "DW_EK_IndkoebsanalyseLinje IS NULL" in UNMAPPED_STG_SQL

    def test_diag_counts_missing_unspsc(self):
        assert "missing_unspsc" in DIAG_SQL.lower() or "DW_EK_UNSPSC = -1" in DIAG_SQL


class TestExecution:
    def test_skips_on_empty_staging(self, monkeypatch):
        res, _, cur, wm = _mock_resource(stg_count=0)
        cur.fetchone.side_effect = [(0,)]
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"
        wm.get_watermark.return_value = None
        wm.log_start_run.return_value = 99
        monkeypatch.setattr(_module, "EtlWatermarkManager", lambda *a, **k: wm)
        result = fct_indkoebsanalyse_linje(ctx, res)
        wm.advance_watermark.assert_called_with("fact", "fct", "IndkoebsanalyseLinje")
        wm.log_complete_run.assert_called_once()
        assert result.metadata["row_count"] == 0

    def test_fail_fast_on_unmapped(self, monkeypatch):
        res, _, cur, wm = _mock_resource(stg_count=3, unmapped=True)
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"
        wm.get_watermark.return_value = None
        wm.log_start_run.return_value = 99
        monkeypatch.setattr(_module, "EtlWatermarkManager", lambda *a, **k: wm)
        with pytest.raises(ValueError, match="at least one staging row has no match"):
            fct_indkoebsanalyse_linje(ctx, res)
        wm.log_fail_run.assert_called_once_with(99)

    def test_watermark_on_success(self, monkeypatch):
        res, _, _, wm = _mock_resource()
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"
        wm.get_watermark.return_value = None
        wm.log_start_run.return_value = 42
        monkeypatch.setattr(_module, "EtlWatermarkManager", lambda *a, **k: wm)
        fct_indkoebsanalyse_linje(ctx, res)
        wm.advance_watermark.assert_called_with("fact", "fct", "IndkoebsanalyseLinje")
        wm.log_complete_run.assert_called_once()
