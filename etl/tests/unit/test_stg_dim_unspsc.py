# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for consolidated stg_dim_UNSPSC asset (#28710)."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import DSA.SQL.staging.stg_dim_UNSPSC  # noqa: F401
import pytest

pytestmark = pytest.mark.unit

_module = sys.modules["DSA.SQL.staging.stg_dim_UNSPSC"]
BUILD_SQL = _module.BUILD_SQL
stg_dim_UNSPSC = _module.stg_dim_UNSPSC.op.compute_fn.decorated_fn


def _mock_resource(rowcount=7):
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
    def test_selects_into_consolidated_table(self):
        assert "INTO [stg].[dim_UNSPSC]" in BUILD_SQL
        assert "DROP TABLE IF EXISTS [stg].[dim_UNSPSC]" in BUILD_SQL

    def test_has_all_five_delta_legs(self):
        assert "[arc].[kmd_uddata_unspsc]" in BUILD_SQL
        assert "[arc].[ils_unspsc]" in BUILD_SQL
        assert "[arc].[ils_vare]" in BUILD_SQL
        assert "[arc].[unspsc_udbudskategori]" in BUILD_SQL
        # 3 ILS-ben: unspsc, unspc, unspsc2
        assert "[unspsc]" in BUILD_SQL
        assert "[unspc]" in BUILD_SQL
        assert "[unspsc2]" in BUILD_SQL

    def test_every_leg_filters_on_update_timestamp(self):
        # Ét delta-filter pr. arc-kilde-ben (KMD + 3 ILS + udbud = 5)
        assert BUILD_SQL.count("[dw_timestamp_update] > CAST(@wm AS DATETIME2)") == 5

    def test_kmd_leg_filters_iscurrent(self):
        assert "[DW_IsCurrent] = 1" in BUILD_SQL

    def test_ils2_strips_dot_zero(self):
        assert "LIKE '%.0'" in BUILD_SQL

    def test_groups_with_max_exists_flags(self):
        assert "GROUP BY" in BUILD_SQL
        assert "MAX(KMD)" in BUILD_SQL
        assert "MAX(ILS)" in BUILD_SQL
        assert "MAX(Udbud)" in BUILD_SQL
        assert "AS dw_source_kmd" in BUILD_SQL
        assert "AS dw_source_ils" in BUILD_SQL
        assert "AS dw_source_udbud" in BUILD_SQL

    def test_creates_clustered_index(self):
        assert "CREATE CLUSTERED INDEX" in BUILD_SQL


class TestAssetShape:
    def test_key_and_group(self):
        asset = _module.stg_dim_UNSPSC
        assert asset.key.path == ["erp", "stg_dim_UNSPSC"]
        assert "erp_staging" in asset.group_names_by_key.values()


class TestExecution:
    def test_reads_shared_dim_checkpoint(self, monkeypatch):
        res, _, cur = _mock_resource()
        ctx = MagicMock()
        ctx.log = MagicMock()
        ctx.run_id = "rid"

        run_ctx = MagicMock()
        run_ctx.checkpoint = "2026-05-04 00:00:00.000"
        dfm = MagicMock()
        dfm.start_dfm_run.return_value = run_ctx
        monkeypatch.setattr(_module, "DfmManager", lambda *a, **k: dfm)

        from core.dfm_config import RunModeConfig

        config = RunModeConfig(mode="delta")
        result = stg_dim_UNSPSC(ctx, res, config)
        _, kwargs = dfm.start_dfm_run.call_args
        assert kwargs["checkpoint_from"] == ("dim", "UNSPSC")
        assert result.metadata["target_table"] == "stg.dim_UNSPSC"
