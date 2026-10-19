# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim_Borger's scoped-target MERGE and lookback window.

The nightly measurement (2026-08-26) showed the previous full-table MERGE
spending 138 minutes on ~30k changed patients in a 2.0M-row dim: the NOT
MATCHED BY SOURCE branch forced a full outer scan with per-row correlated
IN probes. The rewrite scopes the MERGE target to
(sk IN source) OR (ek IN changed entity keys) — provably the same row set
per branch — and materializes the source/ek sets in indexed temp tables.

These tests assert the SQL statement sequence, the scoping constructs, the
unchanged NMBS bookkeeping, and the lookback_days checkpoint arithmetic.
"""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from core.dfm_config import RunModeConfig

pytestmark = pytest.mark.unit


class FakeLog:
    def __init__(self):
        self.infos: list[str] = []

    def info(self, msg):
        self.infos.append(str(msg))

    def warning(self, msg):
        pass


class FakeContext:
    def __init__(self):
        self.log = FakeLog()
        self.run_id = "testrun123"


class FakeDfm:
    def __init__(self, resource, context):
        self.checkpoints_set: list[tuple] = []
        self.completed: list[tuple] = []
        self.failed: list[tuple] = []
        FakeDfm.last = self

    def start_dfm_run(self, layer, table, dagster_run_id, mode):
        return SimpleNamespace(checkpoint="2026-08-25 09:22:47", log_id=7)

    def complete_dfm_run(self, log_id, checkpoint_after=None, rows_affected=None):
        self.completed.append((log_id, checkpoint_after, rows_affected))

    def fail_dfm_run(self, log_id, error_message=None):
        self.failed.append((log_id, error_message))

    def set_checkpoint(self, schema, table, value, mode):
        self.checkpoints_set.append((schema, table, value, mode))


class FakeCursor:
    """Routes fetchone() answers by inspecting the last executed SQL."""

    changed_patients = 30_000

    def __init__(self, executed: list[str]):
        self.executed = executed
        self._last = ""
        self.rowcount = 42

    def execute(self, sql, params=None):
        self._last = sql
        self.executed.append(sql)

    def fetchone(self):
        s = self._last
        if "COUNT(*)" in s and "changed_patients" in s:
            return (self.changed_patients,)
        if "COUNT(*)" in s and "merge_out" in s:
            return (0,)
        if "MAX(ts)" in s:
            return ("2026-08-26 04:37:04",)
        if "COUNT(*)" in s and "[dim].[Borger]" in s:
            return (1_999_685,)
        return (0,)

    def close(self):
        pass


class FakeConn:
    def __init__(self, executed):
        self._executed = executed

    def cursor(self):
        return FakeCursor(self._executed)

    def commit(self):
        pass


class FakeResource:
    def __init__(self, executed):
        self._executed = executed

    @contextmanager
    def get_connection(self, timeout: int = 0):
        yield FakeConn(self._executed)


def _run(monkeypatch, config: RunModeConfig) -> list[str]:
    import DSA.SQL.dim.borger as borger_module

    monkeypatch.setattr(borger_module, "DfmManager", FakeDfm)
    monkeypatch.setattr(borger_module, "check_soft_delete_threshold", lambda *a, **k: None)

    executed: list[str] = []
    asset_fn = borger_module.dim_Borger
    compute_fn = asset_fn.op.compute_fn
    fn = getattr(compute_fn, "decorated_fn", compute_fn)
    fn(FakeContext(), FakeResource(executed), config)
    return executed


def _find(executed: list[str], fragment: str) -> str:
    matches = [s for s in executed if fragment in s]
    assert matches, f"No executed statement contains {fragment!r}"
    return matches[0]


class TestScopedMerge:
    def test_merge_target_is_scoped_cte(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        merge_sql = _find(executed, "MERGE INTO ScopedTarget")
        # sk side covers every source row; ek side covers the NMBS rows.
        assert "dw_sk_Borger IN (" in merge_sql
        assert "src_rows_dim_borger" in merge_sql
        assert "dw_ek_Borger IN (" in merge_sql
        assert "changed_ek_dim_borger" in merge_sql
        # No full-table MERGE target remains.
        assert not any("MERGE INTO [dim].[Borger]" in s for s in executed)

    def test_source_rows_materialized_and_indexed(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        select_into = _find(executed, "SELECT * INTO ##src_rows_dim_borger_testrun123")
        assert "WITH CurrentAddress AS (" in select_into
        assert "SourceRows AS (" in select_into
        _find(executed, "CREATE CLUSTERED INDEX [CIX_src_rows]")
        _find(executed, "CREATE CLUSTERED INDEX [CIX_changed_ek]")
        _find(executed, "CREATE CLUSTERED INDEX [CIX_changed_patients]")

    def test_nmbs_branch_and_bookkeeping_unchanged(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        merge_sql = _find(executed, "WHEN NOT MATCHED BY SOURCE")
        assert "tgt.dw_sk_Borger > 0" in merge_sql
        assert "tgt.dw_is_deleted = 0" in merge_sql
        # OUTPUT bookkeeping (soft-delete threshold source) intact.
        assert "OUTPUT $action" in merge_sql
        assert "src.dw_sk_Borger" in merge_sql
        # Soft-delete/attribute-propagation counts still read from merge_out.
        assert any("src_sk IS NULL" in s for s in executed)
        assert any("src_sk IS NOT NULL" in s for s in executed)

    def test_clear_currents_uses_ek_temp(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        clear_sql = _find(executed, "SET tgt.dw_is_current = 0")
        assert "changed_ek_dim_borger" in clear_sql
        # Old nested-IN chain gone from the UPDATE.
        assert "[ref].[PatientToBorger]" not in clear_sql

    def test_all_temp_tables_dropped(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        drops = [s for s in executed if s.startswith("DROP TABLE IF EXISTS")]
        dropped = " ".join(drops)
        for base in (
            "merge_out_dim_borger",
            "changed_patients_dim_borger",
            "changed_ek_dim_borger",
            "src_rows_dim_borger",
        ):
            assert base in dropped


class TestPopulationsansvarAndProtection:
    def test_changed_mapping_reaches_dim_without_changed_patients(self, monkeypatch):
        monkeypatch.setattr(FakeCursor, "changed_patients", 0)
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        assert not any("MERGE INTO ScopedTarget" in s for s in executed)
        refresh_sql = _find(executed, "SET b.populationsansvar")
        assert "[ref].[KommuneToPopulationsansvar]" in refresh_sql

    def test_populationsansvar_set_after_merge(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        merge_at = executed.index(_find(executed, "MERGE INTO ScopedTarget"))
        refresh_at = executed.index(_find(executed, "SET b.populationsansvar"))
        assert refresh_at > merge_at

    def test_only_retired_versions_of_protected_citizens_are_masked(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        mask_sql = _find(executed, "SET fornavne = NULL")
        assert "tgt.dw_is_deleted = 1" in mask_sql
        assert "WHERE adressebeskyttelse = 1" in mask_sql


class TestLookbackWindow:
    def test_no_lookback_uses_plain_parameter(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta"))
        sql = _find(executed, "SELECT DISTINCT patient_id")
        assert "DATEADD" not in sql
        assert "dw_timestamp_update > ?" in sql

    def test_lookback_applies_dateadd(self, monkeypatch):
        executed = _run(monkeypatch, RunModeConfig(mode="delta", lookback_days=1))
        sql = _find(executed, "SELECT DISTINCT patient_id")
        assert "DATEADD(day, -1, CAST(? AS DATETIME2))" in sql

    def test_advance_checkpoint_false_blocks_set(self, monkeypatch):
        _run(monkeypatch, RunModeConfig(mode="delta", advance_checkpoint=False))
        assert FakeDfm.last.checkpoints_set == []

    def test_default_delta_advances_checkpoint(self, monkeypatch):
        _run(monkeypatch, RunModeConfig(mode="delta"))
        assert FakeDfm.last.checkpoints_set == [("dim", "Borger", "2026-08-26 04:37:04", "delta")]
