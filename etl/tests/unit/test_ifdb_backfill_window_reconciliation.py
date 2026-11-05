# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""A backfill window must prove its own month, not merely run without failing.

Without the reconciliation a green marker only means no step crashed. A
systematic loss would show only after the whole history had run — and then it
all runs again.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import dagster as dg
import pytest
from dqa.model import CheckOutcome
from DSA.SQL.dfm import ifdb_backfill_window_reconciliation as module


class _Config:
    def __init__(self, mode="period", date_from="2023-05-01", date_to="2023-05-31"):
        self.mode = mode
        self.date_from = date_from
        self.date_to = date_to


def _context():
    context = MagicMock()
    context.log = MagicMock()
    return context


def _outcome(missing: int, source: int = 1000) -> CheckOutcome:
    return CheckOutcome(
        passed=missing == 0,
        checked=source,
        violations=missing,
        extra={"missing_in_arc": missing, "source_count": source},
    )


class TestWindowSourceQuery:
    """The reconciliation must see exactly the rows the window's extract fetched."""

    def test_is_the_query_the_extract_builds(self) -> None:
        from core.dfm_config import RunModeConfig
        from DSI.connectors.factory import build_oracle_extract_query
        from DSI.oracle.ifdb_extract import AKTIVITETSRESULTAT_EXTRACT_CONFIG as cfg
        from DSI.oracle.ifdb_extract import build_oracle_where

        where = build_oracle_where(
            RunModeConfig(mode="period", date_from="2023-05-01", date_to="2023-05-31"), None, cfg
        )
        expected = build_oracle_extract_query(cfg, where)
        assert module.build_window_source_query(cfg, "2023-05-01", "2023-05-31") == expected

    def test_the_extract_itself_uses_the_same_builder(self) -> None:
        """Otherwise the two could drift and the test above would not notice."""
        import inspect

        from DSI.connectors import factory

        assert "build_oracle_extract_query(extract_config, where_clause)" in inspect.getsource(
            factory
        )

    def test_a_joined_extract_keeps_its_alias_inside_the_join(self) -> None:
        from DSI.oracle.ifdb_extract import HENVISNING_HISTORIK_EXTRACT_CONFIG as cfg

        query = module.build_window_source_query(cfg, "2023-05-01", "2023-05-31")
        assert query is not None
        assert "INNER JOIN IFDB.HENVISNING h" in query
        assert "h.OPDATERET_TID >= TO_DATE('2023-05-01'" in query
        # The last day counts in full, or every window loses its final day.
        assert "TO_DATE('2023-05-31', 'YYYY-MM-DD') + 1" in query

    def test_a_full_load_source_has_nothing_to_bound(self) -> None:
        from DSI.oracle import ifdb_extract

        full_load = next(
            c
            for c in vars(ifdb_extract).values()
            if getattr(c, "is_full_load", False) and hasattr(c, "source_table")
        )
        assert module.build_window_source_query(full_load, "2023-05-01", "2023-05-31") is None


class TestReconcileWindow:
    def test_skips_outside_a_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = []
        monkeypatch.setattr(module, "source_archive_reconciliation", lambda *a: calls.append(a))
        result = module.reconcile_window(_context(), _Config(mode="delta"))
        assert calls == []
        assert "skipped" in result.metadata

    def test_missing_keys_fail_the_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(module, "source_archive_reconciliation", lambda *a: _outcome(4711))
        with pytest.raises(dg.Failure) as raised:
            module.reconcile_window(_context(), _Config())
        assert "did not reach the archive" in raised.value.description
        assert any(v.value == 4711 for v in raised.value.metadata.values())

    def test_a_clean_window_lists_the_archives_it_could_not_check(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(module, "source_archive_reconciliation", lambda *a: _outcome(0))
        result = module.reconcile_window(_context(), _Config())
        assert result.metadata["archives_reconciled"].value > 0
        assert (
            list(module.ARCHIVES_WITHOUT_RECONCILIATION)
            == result.metadata["archives_without_reconciliation"].value
        )

    def test_window_params_bound_the_source_and_skip_the_extra_direction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A window's gaps belong to the backfill, not to the replay queue."""
        seen = []

        def _capture(context, subject, spec, resources):
            seen.append(spec.params)
            return _outcome(0)

        monkeypatch.setattr(module, "source_archive_reconciliation", _capture)
        module.reconcile_window(_context(), _Config())
        assert seen, "no archive was reconciled"
        for params in seen:
            assert params["missing_only"] is True
            assert "capture_replay" not in params
            assert "flow_start_bound_column" not in params
            assert "source_filter" not in params
            assert params["source_table"].startswith("(") and "2023-05-01" in params["source_table"]

    def test_an_archive_that_cannot_be_reconciled_leaves_the_window_unproven(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One failure must neither hide the other archives nor give a green window."""
        calls = []

        def _second_fails(context, subject, spec, resources):
            calls.append(subject.name)
            if len(calls) == 2:
                # An empty message, like TimeoutError(), must not crash the handler.
                raise TimeoutError()
            return _outcome(0)

        monkeypatch.setattr(module, "source_archive_reconciliation", _second_fails)
        with pytest.raises(dg.Failure) as raised:
            module.reconcile_window(_context(), _Config())
        assert "could not be reconciled" in raised.value.description
        assert any("TimeoutError" in str(v.value) for v in raised.value.metadata.values())
        assert len(calls) > 2, "the remaining archives must still be reconciled"


class TestPlacement:
    def test_the_list_of_unreconciled_archives_matches_the_definitions(self) -> None:
        import definitions
        from dqa.config.arc_subjects import _ARC

        graph = definitions.defs.get_repository_def().asset_graph
        reconciled = {spec[1].split(".")[-1] for spec in _ARC}
        archive_assets = {
            key.path[-1]
            for key in graph.get_all_asset_keys()
            if graph.get(key).group_name == "ifdb_archive" and key.path[-1].startswith("arc_")
        }

        def is_reconciled(asset: str) -> bool:
            # arc_<name>_ifdb holds arc.<name> or arc.ifdb_<name>, depending on source.
            name = asset[len("arc_") :]
            name = name[: -len("_ifdb")] if name.endswith("_ifdb") else name
            return name in reconciled or f"ifdb_{name}" in reconciled

        without = sorted(a for a in archive_assets if not is_reconciled(a))
        assert without == sorted(module.ARCHIVES_WITHOUT_RECONCILIATION)

    def test_no_job_runs_it_ahead_of_the_archive(self) -> None:
        """In a group job it could start before the merges it checks."""
        import definitions

        graph = definitions.defs.get_repository_def().asset_graph
        key = dg.AssetKey("ifdb_backfill_window_reconciliation")
        group = graph.get(key).group_name
        assert group != "ifdb_archive"
        for job in definitions.defs.get_repository_def().get_all_jobs():
            if job.name.startswith("__ASSET_JOB") or job.name.startswith("ifdb_fhir_fuldload"):
                continue
            selected = getattr(job, "asset_layer", None)
            keys = selected.executable_asset_keys if selected else set()
            assert key not in keys, f"{job.name} would run the reconciliation"
