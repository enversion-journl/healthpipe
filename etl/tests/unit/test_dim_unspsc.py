# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim_UNSPSC asset."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.dim.dim_UNSPSC  # noqa: F401
import pytest

pytestmark = pytest.mark.unit

_module = sys.modules["DSA.SQL.dim.dim_UNSPSC"]
BUILD_SQL = _module.BUILD_SQL
MERGE_SQL = _module.MERGE_SQL
dim_UNSPSC = _module.dim_UNSPSC.op.compute_fn.decorated_fn


def _mock_resource(rowcount=10):
    cur = MagicMock()
    cur.rowcount = rowcount
    cur.fetchone.return_value = (rowcount,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    res = MagicMock()

    @contextmanager
    def _g(*args, **kwargs):  # accept prod kwargs (e.g. timeout=)
        yield conn

    res.get_connection = _g
    return res, conn, cur


class TestBuildSql:
    def test_delta_from_consolidated_stg(self):
        assert "[stg].[dim_UNSPSC]" in BUILD_SQL
        assert "dim_UNSPSC_ils" not in BUILD_SQL
        assert "dim_unspsc_kmd" not in BUILD_SQL

    def test_joins_map_for_dw_ek_and_flags(self):
        assert "JOIN [map].[UNSPSC]" in BUILD_SQL
        assert "m.dw_source_ils" in BUILD_SQL
        assert "m.dw_source_kmd" in BUILD_SQL

    def test_left_joins_full_arc_for_descriptions(self):
        assert "LEFT JOIN [arc].[ils_unspsc]" in BUILD_SQL
        assert "LEFT JOIN [arc].[kmd_uddata_unspsc]" in BUILD_SQL

    def test_kmd_join_filters_by_dw_iscurrent(self):
        assert "[DW_IsCurrent] = 1" in BUILD_SQL

    def test_builds_deltatomerge_temp(self):
        assert "INTO #DeltaToMerge" in BUILD_SQL
        assert "CREATE UNIQUE CLUSTERED INDEX" in BUILD_SQL


class TestMergeSql:
    def test_targets_dim(self):
        assert "MERGE INTO [dim].[UNSPSC]" in MERGE_SQL

    def test_merge_uses_deltatomerge(self):
        assert "USING #DeltaToMerge" in MERGE_SQL

    def test_when_matched_when_not_matched(self):
        assert "WHEN MATCHED AND EXISTS" in MERGE_SQL
        assert ") THEN UPDATE SET" in MERGE_SQL
        assert "WHEN NOT MATCHED BY TARGET THEN INSERT" in MERGE_SQL

    def test_plain_assignments_for_source_flags(self):
        # Source flags from map.UNSPSC are sticky — dim merge needs no CASE-WHEN logic.
        update_section = MERGE_SQL.split(") THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "tgt.dw_source_ils = src.dw_source_ils" in update_section
        assert "tgt.dw_source_kmd = src.dw_source_kmd" in update_section

    def test_when_matched_has_change_guard(self):
        # Re-runs that change nothing must be no-ops (no timestamp churn / sync churn).
        assert "WHEN MATCHED AND EXISTS" in MERGE_SQL
        assert "EXCEPT" in MERGE_SQL


class TestExecution:
    def test_advances_checkpoint_on_success(self, monkeypatch):
        res, _, _ = _mock_resource()
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"

        run_ctx = MagicMock()
        dfm = MagicMock()
        dfm.start_dfm_run.return_value = run_ctx
        dfm.get_checkpoint.return_value = "2026-05-04T00:00:00"
        monkeypatch.setattr(_module, "DfmManager", lambda *a, **k: dfm)

        from core.dfm_config import RunModeConfig

        config = RunModeConfig(mode="delta")
        result = dim_UNSPSC(ctx, res, config)
        dfm.set_checkpoint.assert_called_once()
        dfm.complete_dfm_run.assert_called_once()
        dfm.fail_dfm_run.assert_not_called()
        assert result.metadata["target_table"] == "dim.UNSPSC"

    def test_fail_dfm_run_on_exception(self, monkeypatch):
        res, _, cur = _mock_resource()
        cur.execute.side_effect = RuntimeError("boom")
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"

        run_ctx = MagicMock()
        dfm = MagicMock()
        dfm.start_dfm_run.return_value = run_ctx
        monkeypatch.setattr(_module, "DfmManager", lambda *a, **k: dfm)

        from core.dfm_config import RunModeConfig

        config = RunModeConfig(mode="delta")
        with pytest.raises(RuntimeError):
            dim_UNSPSC(ctx, res, config)
        dfm.fail_dfm_run.assert_called_once()
        dfm.set_checkpoint.assert_not_called()
