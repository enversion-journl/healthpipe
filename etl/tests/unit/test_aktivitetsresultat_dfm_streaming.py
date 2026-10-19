# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for the DFM asset's streamed archive input.

The asset must spool the arc cursor to ``BatchedExtractRows``-format sidecars
and invoke ``_run_aktivitetsresultat_pipeline_chunked`` exactly ONCE per delta
slice with a re-iterable, ``iter_batches``-capable object. The previous shape —
one pipeline call per 500-row fetch batch — re-ran the orchestrator's
one-time setup per batch and re-posted model Questionnaires (the known SFI
timestamp-churn source), so these tests pin the single-call architecture.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from core.extract.batched_rows import BatchedExtractRows
from DSA.FHIR.Aktivitetsresultat import Aktivitetsresultat_IFDB_dfm as mod


class _FakeCursor:
    def __init__(self, rows: list[tuple], description: list[tuple]) -> None:
        self._rows = list(rows)
        self.description = description
        self.executed: list[str] = []
        self.fetchmany_sizes: list[int] = []
        self.closed = False

    def execute(self, query: str, *_params) -> None:
        self.executed.append(query)

    def fetchmany(self, size: int) -> list[tuple]:
        self.fetchmany_sizes.append(size)
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def fetchone(self) -> list[str]:
        return ["2026-08-11 06:00:00.0000000"]

    def close(self) -> None:
        self.closed = True


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_exc) -> bool:
        return False


class _FakeMSSQL:
    """Hands out one scripted cursor per ``get_connection()`` call."""

    def __init__(self, cursors: list[_FakeCursor]) -> None:
        self._cursors = list(cursors)

    def get_connection(self) -> _FakeConn:
        return _FakeConn(self._cursors.pop(0))


class _StubDfmManager:
    def __init__(self) -> None:
        self.checkpoints: list[tuple[str, str, str, str]] = []
        self.completed: list[dict] = []
        self.failed: list[str] = []
        self.reader_bounds: list[str | None] = []

    def start_dfm_run(self, _schema: str, _table: str, **_kwargs) -> SimpleNamespace:
        return SimpleNamespace(checkpoint=None, log_id=42, started_at="2026-08-11 05:59:00")

    def safe_reader_checkpoint(self, started_at: str | None) -> str | None:
        self.reader_bounds.append(started_at)
        return started_at

    def set_checkpoint(self, schema: str, table: str, *, value: str, mode: str) -> None:
        self.checkpoints.append((schema, table, value, mode))

    def complete_dfm_run(self, log_id: int, *, checkpoint_after, rows_affected: int) -> None:
        self.completed.append(
            {
                "log_id": log_id,
                "checkpoint_after": checkpoint_after,
                "rows_affected": rows_affected,
            }
        )

    def fail_dfm_run(self, log_id: int, *, error_message: str) -> None:
        self.failed.append(error_message)


def _fake_context() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="test-run",
        log=SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        ),
    )


def _asset_fn():
    """The undecorated compute function (plain generator, no Dagster ceremony)."""
    compute = mod.fhir_server_aktivitetsresultat_sfi_dfm.op.compute_fn
    return getattr(compute, "decorated_fn", compute)


_DESCRIPTION = [("OBJEKT_ID",), ("XML",)]


_BOUND = "2026-08-11 05:00:00"


def _slices(monkeypatch, *bounds: str) -> list[tuple[str | None, str | None]]:
    """Script the delta slice bounds; records the lower bound and ceiling of every lookup."""
    remaining = list(bounds)
    lookups: list[tuple[str | None, str | None]] = []

    def _next_bound(_conn, _table, lower, _max_rows, ceiling=None):
        lookups.append((lower, ceiling))
        return remaining.pop(0) if remaining else None

    monkeypatch.setattr(mod, "next_delta_slice_bound", _next_bound)
    return lookups


def _rows(n: int) -> list[tuple]:
    return [(str(i), f"<xml>{i}</xml>") for i in range(1, n + 1)]


@pytest.mark.unit
def test_spool_roundtrip_matches_batched_extract_rows(tmp_path, monkeypatch) -> None:
    """Format lock: the local spool writer must produce exactly what
    ``BatchedExtractRows`` reads — including re-iterability (the orchestrator
    does several full passes before its chunk loop) and chunk coalescing."""
    monkeypatch.setattr(mod, "_SPOOL_BATCH_SIZE", 2)
    cursor = _FakeCursor(_rows(5), _DESCRIPTION)

    row_count, batch_count = mod._spool_archive_to_sidecars(cursor, ["OBJEKT_ID", "XML"], tmp_path)

    assert (row_count, batch_count) == (5, 3)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["version"] == 1
    assert manifest["row_count"] == 5
    assert manifest["batch_count"] == 3
    assert manifest["batch_files"] == [
        "batch-000001.pickle",
        "batch-000002.pickle",
        "batch-000003.pickle",
    ]

    rows = BatchedExtractRows(tmp_path)
    assert len(rows) == 5
    first_pass = [row["OBJEKT_ID"] for row in rows]
    second_pass = [row["OBJEKT_ID"] for row in rows]
    assert first_pass == ["1", "2", "3", "4", "5"]
    assert second_pass == first_pass
    assert [len(batch) for batch in rows.iter_batches(3)] == [3, 2]


@pytest.mark.unit
def test_pipeline_invoked_once_with_streaming_container(monkeypatch) -> None:
    stub = _StubDfmManager()
    monkeypatch.setattr(mod, "DfmManager", lambda *_a, **_k: stub)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)

    calls: list[dict] = []

    def _fake_pipeline(*, extracted_data, **kwargs):
        calls.append(
            {
                "sidecar_dir": Path(extracted_data.sidecar_dir),
                "len": len(extracted_data),
                "has_iter_batches": callable(getattr(extracted_data, "iter_batches", None)),
                "first_pass": [row["OBJEKT_ID"] for row in extracted_data],
                "second_pass": [row["OBJEKT_ID"] for row in extracted_data],
                "coalesced": [len(batch) for batch in extracted_data.iter_batches(2)],
                "kwargs": kwargs,
            }
        )
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(len(extracted_data)),
                "failed_count": dg.MetadataValue.int(0),
            }
        )

    monkeypatch.setattr(mod, "_run_aktivitetsresultat_pipeline_chunked", _fake_pipeline)
    _slices(monkeypatch, _BOUND)

    spool_cursor = _FakeCursor(_rows(3), _DESCRIPTION)
    mssql = _FakeMSSQL([_FakeCursor([], _DESCRIPTION), spool_cursor, _FakeCursor([], _DESCRIPTION)])

    outputs = list(_asset_fn()(_fake_context(), mssql, object(), RunModeConfig(mode="delta")))

    # Exactly ONE pipeline call, fed a re-iterable iter_batches container.
    assert len(calls) == 1
    call = calls[0]
    assert call["len"] == 3
    assert call["has_iter_batches"] is True
    assert call["first_pass"] == ["1", "2", "3"]
    assert call["second_pass"] == call["first_pass"]
    assert call["coalesced"] == [2, 1]
    assert call["kwargs"]["ifdb_oracle"] is None

    # Spool dir is cleaned up after the run.
    assert not call["sidecar_dir"].exists()

    # Checkpoint advanced once, to the slice bound; rows_affected from the result.
    assert len(stub.checkpoints) == 1
    assert stub.checkpoints[0][:3] == ("fhir_server", "Aktivitetsresultat_SFI", _BOUND)
    assert f"<= '{_BOUND}'" in spool_cursor.executed[0]
    assert stub.completed[0]["rows_affected"] == 3
    assert stub.failed == []

    obs_meta = outputs[0].metadata
    assert obs_meta["posted_count"].value == 3
    assert obs_meta["fetched_count"].value == 3
    assert obs_meta["batch_count"].value == 1
    assert obs_meta["method"].value == "dfm_arc_streamed"


@pytest.mark.unit
def test_sfi_mode_unpacks_three_results(monkeypatch) -> None:
    stub = _StubDfmManager()
    monkeypatch.setattr(mod, "DfmManager", lambda *_a, **_k: stub)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: True)

    def _fake_pipeline(*, extracted_data, **kwargs):
        assert kwargs["emit_questionnaire_outputs"] is True

        def _result(count: int) -> dg.MaterializeResult:
            return dg.MaterializeResult(
                metadata={
                    "posted_count": dg.MetadataValue.int(count),
                    "failed_count": dg.MetadataValue.int(0),
                }
            )

        return (_result(len(extracted_data)), _result(2), _result(1))

    monkeypatch.setattr(mod, "_run_aktivitetsresultat_pipeline_chunked", _fake_pipeline)

    _slices(monkeypatch, _BOUND)
    mssql = _FakeMSSQL(
        [
            _FakeCursor([], _DESCRIPTION),
            _FakeCursor(_rows(4), _DESCRIPTION),
            _FakeCursor([], _DESCRIPTION),
        ]
    )
    outputs = list(_asset_fn()(_fake_context(), mssql, object(), RunModeConfig(mode="delta")))

    assert outputs[0].metadata["posted_count"].value == 4
    assert outputs[1].metadata["posted_count"].value == 2
    assert outputs[2].metadata["posted_count"].value == 1
    # rows_affected = obs + proc(0) + cond(0) + q + qr
    assert stub.completed[0]["rows_affected"] == 7


@pytest.mark.unit
def test_empty_run_skips_pipeline_and_checkpoint(monkeypatch) -> None:
    stub = _StubDfmManager()
    monkeypatch.setattr(mod, "DfmManager", lambda *_a, **_k: stub)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)

    def _fail_pipeline(**_kwargs):
        raise AssertionError("pipeline must not run for an empty archive window")

    monkeypatch.setattr(mod, "_run_aktivitetsresultat_pipeline_chunked", _fail_pipeline)

    # Only the slice lookup's connection: no rows left means no archive query.
    _slices(monkeypatch)
    mssql = _FakeMSSQL([_FakeCursor([], _DESCRIPTION)])
    outputs = list(_asset_fn()(_fake_context(), mssql, object(), RunModeConfig(mode="delta")))

    assert stub.checkpoints == []
    assert stub.completed[0]["rows_affected"] == 0
    assert outputs[0].metadata["posted_count"].value == 0
    assert outputs[0].metadata["fetched_count"].value == 0


@pytest.mark.unit
def test_pipeline_failure_cleans_spool_and_fails_run(monkeypatch) -> None:
    stub = _StubDfmManager()
    monkeypatch.setattr(mod, "DfmManager", lambda *_a, **_k: stub)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)

    seen_dirs: list[Path] = []

    def _fake_pipeline(*, extracted_data, **_kwargs):
        seen_dirs.append(Path(extracted_data.sidecar_dir))
        assert seen_dirs[0].exists()
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_run_aktivitetsresultat_pipeline_chunked", _fake_pipeline)

    _slices(monkeypatch, _BOUND)
    mssql = _FakeMSSQL([_FakeCursor([], _DESCRIPTION), _FakeCursor(_rows(2), _DESCRIPTION)])
    with pytest.raises(RuntimeError, match="boom"):
        list(_asset_fn()(_fake_context(), mssql, object(), RunModeConfig(mode="delta")))

    assert not seen_dirs[0].exists()
    assert stub.checkpoints == []
    assert stub.failed and "boom" in stub.failed[0]


@pytest.mark.unit
def test_delta_backlog_is_posted_slice_by_slice_with_a_checkpoint_each(monkeypatch) -> None:
    """A backlog is cut into slices: one pipeline call and one checkpoint per slice."""
    stub = _StubDfmManager()
    monkeypatch.setattr(mod, "DfmManager", lambda *_a, **_k: stub)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)
    first, second = "2026-09-27 22:23:57", "2026-09-28 01:08:59"
    lookups = _slices(monkeypatch, first, second)

    calls: list[int] = []

    def _fake_pipeline(*, extracted_data, **_kwargs):
        calls.append(len(extracted_data))
        return dg.MaterializeResult(metadata={"posted_count": dg.MetadataValue.int(len(calls))})

    monkeypatch.setattr(mod, "_run_aktivitetsresultat_pipeline_chunked", _fake_pipeline)
    slice_one = _FakeCursor(_rows(3), _DESCRIPTION)
    slice_two = _FakeCursor(_rows(2), _DESCRIPTION)
    mssql = _FakeMSSQL(
        [
            _FakeCursor([], _DESCRIPTION),
            slice_one,
            _FakeCursor([], _DESCRIPTION),
            slice_two,
            _FakeCursor([], _DESCRIPTION),
        ]
    )

    outputs = list(_asset_fn()(_fake_context(), mssql, object(), RunModeConfig(mode="delta")))

    assert calls == [3, 2]
    # Each slice starts where the previous one ended, not at the persisted checkpoint,
    # and none passes the safe read point taken at the run's start.
    read_start = "2026-08-11 05:59:00"
    assert lookups == [(None, read_start), (first, read_start), (second, read_start)]
    assert f"> '{first}'" in slice_two.executed[0]
    assert f"<= '{second}'" in slice_two.executed[0]
    assert [c[2] for c in stub.checkpoints] == [first, second]
    assert stub.completed[0]["checkpoint_after"] == second
    assert outputs[0].metadata["fetched_count"].value == 5
    assert outputs[0].metadata["posted_count"].value == 3


@pytest.mark.unit
def test_full_load_is_one_slice_and_keeps_the_read_start(monkeypatch) -> None:
    """Outside delta there is no slicing; the checkpoint logic is unchanged."""
    stub = _StubDfmManager()
    monkeypatch.setattr(mod, "DfmManager", lambda *_a, **_k: stub)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)

    def _no_slicing(*_args):
        raise AssertionError("a full load must not be sliced")

    monkeypatch.setattr(mod, "next_delta_slice_bound", _no_slicing)
    monkeypatch.setattr(
        mod,
        "_run_aktivitetsresultat_pipeline_chunked",
        lambda *, extracted_data, **_k: dg.MaterializeResult(
            metadata={"posted_count": dg.MetadataValue.int(len(extracted_data))}
        ),
    )
    spool_cursor = _FakeCursor(_rows(2), _DESCRIPTION)
    mssql = _FakeMSSQL([spool_cursor, _FakeCursor([], _DESCRIPTION)])
    list(_asset_fn()(_fake_context(), mssql, object(), RunModeConfig(mode="full_load")))

    assert "<=" not in spool_cursor.executed[0]
    assert stub.reader_bounds == ["2026-08-11 05:59:00"]
