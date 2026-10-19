# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for map_UNSPSC asset (sticky source-flag MERGE from stg.dim_UNSPSC)."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.map.map_UNSPSC  # noqa: F401
import pytest

pytestmark = pytest.mark.unit

_module = sys.modules["DSA.SQL.map.map_UNSPSC"]
MERGE_SQL = _module.MERGE_SQL
map_UNSPSC = _module.map_UNSPSC.op.compute_fn.decorated_fn


def _mock_resource(rowcount=3):
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


class TestMergeSql:
    def test_merges_into_map_from_consolidated_stg(self):
        assert "MERGE INTO [map].[UNSPSC]" in MERGE_SQL
        assert "[stg].[dim_UNSPSC]" in MERGE_SQL

    def test_no_old_stg_tables(self):
        assert "dim_UNSPSC_ils" not in MERGE_SQL
        assert "dim_unspsc_kmd" not in MERGE_SQL

    def test_change_guard_on_update_arm(self):
        # UPDATE kører kun når mindst ét flag faktisk flipper 0->1.
        assert "WHEN MATCHED AND (" in MERGE_SQL
        assert "m.dw_source_kmd   = 0 AND s.dw_source_kmd   = 1" in MERGE_SQL
        assert "m.dw_source_ils   = 0 AND s.dw_source_ils   = 1" in MERGE_SQL

    def test_sticky_assignments(self):
        assert (
            "m.dw_source_kmd   = CASE WHEN m.dw_source_kmd   = 1 THEN 1 ELSE s.dw_source_kmd   END"
            in MERGE_SQL
        )
        assert (
            "m.dw_source_ils   = CASE WHEN m.dw_source_ils   = 1 THEN 1 ELSE s.dw_source_ils   END"
            in MERGE_SQL
        )

    def test_insert_arm(self):
        assert "WHEN NOT MATCHED BY TARGET THEN INSERT" in MERGE_SQL


class TestExecution:
    def test_runs_merge(self, monkeypatch):
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
        assert cur.execute.call_count >= 2  # MERGE + final COUNT
        assert result.metadata["target_table"] == "map.UNSPSC"


class TestDocstring:
    def test_documents_map_purpose(self):
        doc = (_module.__doc__ or "").lower()
        assert "map" in doc
        assert "unspsc" in doc
