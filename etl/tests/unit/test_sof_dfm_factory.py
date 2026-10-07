# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Checkpoint semantics of the DFM SOF factory.

The window a SOF asset reads is ``ts >= checkpoint``. The checkpoint the run
leaves behind must therefore be taken *before* the view is read: a stamp taken
after the read would let any resource written during the read fall between the
two windows and vanish from the flat table for good.
"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.utils.sql_on_fhir import sof_dfm_factory as mod


class _StubDfmManager:
    def __init__(self) -> None:
        self.checkpoints: list[tuple[str, str, str, str]] = []
        self.completed: list[dict] = []
        self.failed: list[str] = []

    def start_dfm_run(self, _schema: str, _table: str, **_kwargs) -> SimpleNamespace:
        return SimpleNamespace(checkpoint="2026-01-01T00:00:00+00:00", log_id=42)

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


def _compute_fn(asset):
    """The undecorated compute function (no Dagster ceremony)."""
    compute = asset.op.compute_fn
    return getattr(compute, "decorated_fn", compute)


def _build(monkeypatch, *, config_key: str = "patient_view", extract=None):
    stub = _StubDfmManager()
    monkeypatch.setattr(mod, "DfmManager", lambda *_a, **_k: stub)
    calls: list[dict] = []

    def _default_extract(*_args, **kwargs):
        calls.append({"kwargs": kwargs, "read_at": datetime.now(UTC)})
        return 7, 3

    monkeypatch.setattr(mod, "extract_and_merge_sof_view", extract or _default_extract)
    asset = mod.create_sof_dfm_asset(
        asset_name="sof_test_dfm",
        config_key=config_key,
        checkpoint_resource="Test",
        deps=["fhir_sof_infrastructure"],
    )
    return asset, stub, calls


@pytest.mark.unit
def test_checkpoint_is_stamped_before_the_view_is_read(monkeypatch) -> None:
    asset, stub, calls = _build(monkeypatch)

    _compute_fn(asset)(_fake_context(), None, None, RunModeConfig(mode="delta"))

    assert len(calls) == 1
    ((_, _, value, mode),) = stub.checkpoints
    assert mode == "delta"
    assert datetime.fromisoformat(value) <= calls[0]["read_at"]
    assert stub.completed == [{"log_id": 42, "checkpoint_after": value, "rows_affected": 3}]


@pytest.mark.unit
def test_period_mode_reads_the_requested_window_and_keeps_checkpoint(monkeypatch) -> None:
    asset, stub, calls = _build(monkeypatch)

    _compute_fn(asset)(
        _fake_context(),
        None,
        None,
        RunModeConfig(mode="period", date_from="2026-02-01", date_to="2026-02-02"),
    )

    kwargs = calls[0]["kwargs"]
    assert (kwargs["partition_start"], kwargs["partition_end"]) == ("2026-02-01", "2026-02-02")
    assert stub.checkpoints == []
    assert stub.completed[0]["checkpoint_after"] is None


@pytest.mark.unit
def test_full_load_reads_everything_and_advances_checkpoint(monkeypatch) -> None:
    asset, stub, calls = _build(monkeypatch)

    _compute_fn(asset)(_fake_context(), None, None, RunModeConfig(mode="full_load"))

    assert calls[0]["kwargs"]["partition_start"] is None
    assert len(stub.checkpoints) == 1


@pytest.mark.unit
def test_failed_read_fails_the_run_without_advancing_checkpoint(monkeypatch) -> None:
    def _boom(*_args, **_kwargs):
        raise RuntimeError("view exploded")

    asset, stub, _ = _build(monkeypatch, extract=_boom)

    with pytest.raises(RuntimeError, match="view exploded"):
        _compute_fn(asset)(_fake_context(), None, None, RunModeConfig(mode="delta"))

    assert stub.checkpoints == []
    assert stub.completed == []
    assert stub.failed == ["view exploded"]


@pytest.mark.unit
def test_view_config_columns_are_forwarded_to_the_extract(monkeypatch) -> None:
    asset, _, calls = _build(monkeypatch, config_key="patient_address_view")
    cfg = mod.SOF_RESOURCE_CONFIGS["patient_address_view"]

    _compute_fn(asset)(_fake_context(), None, None, RunModeConfig(mode="delta"))

    kwargs = calls[0]["kwargs"]
    assert kwargs["null_sentinel_columns"] == cfg.get("null_sentinel_columns")
    assert kwargs["normalized_datetime_columns"] == cfg["normalized_datetime_columns"]
