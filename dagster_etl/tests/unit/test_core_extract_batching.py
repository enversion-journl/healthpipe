# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import pytest
from core.extract import ExtractBatchingPolicy, iter_cursor_fetchmany, iter_sized_batches


class _FakeCursor:
    def __init__(self, batches):
        self._batches = list(batches)
        self.fetch_sizes: list[int] = []

    def fetchmany(self, size: int):
        self.fetch_sizes.append(size)
        if not self._batches:
            return []
        return self._batches.pop(0)


def test_policy_reads_source_env_before_fallback(monkeypatch):
    monkeypatch.setenv("IFDB_EXTRACT_BATCHED", "false")
    monkeypatch.setenv("IFDB_EXTRACT_BATCH_SIZE", "50")
    monkeypatch.setenv("IFDB_EXTRACT_AKTIVITETSRESULTAT_BATCHED", "true")
    monkeypatch.setenv("IFDB_EXTRACT_AKTIVITETSRESULTAT_BATCH_SIZE", "25")

    policy = ExtractBatchingPolicy.from_env(
        env_prefix="IFDB_EXTRACT_AKTIVITETSRESULTAT",
        default_enabled=False,
        default_fetch_size=100,
        fallback_env_prefix="IFDB_EXTRACT",
    )

    assert policy.enabled is True
    assert policy.fetch_size == 25


def test_policy_uses_fallback_env_when_source_env_missing(monkeypatch):
    monkeypatch.setenv("IFDB_EXTRACT_BATCHED", "true")
    monkeypatch.setenv("IFDB_EXTRACT_BATCH_SIZE", "75")

    policy = ExtractBatchingPolicy.from_env(
        env_prefix="IFDB_EXTRACT_MISSING",
        default_enabled=False,
        default_fetch_size=100,
        fallback_env_prefix="IFDB_EXTRACT",
    )

    assert policy.enabled is True
    assert policy.fetch_size == 75


def test_policy_preserves_explicit_sidecar_opt_in_through_env_resolution(
    monkeypatch,
):
    monkeypatch.setenv("IFDB_EXTRACT_BATCHED", "true")
    monkeypatch.setenv("IFDB_EXTRACT_BATCH_SIZE", "75")

    policy = ExtractBatchingPolicy(
        enabled=True,
        fetch_size=100,
        env_prefix="IFDB_EXTRACT_AKTIVITETSRESULTAT",
        write_batch_sidecars=True,
    ).resolved(fallback_env_prefix="IFDB_EXTRACT")

    assert policy.enabled is True
    assert policy.fetch_size == 75
    assert policy.write_batch_sidecars is True


def test_policy_does_not_enable_sidecars_from_global_env(monkeypatch):
    monkeypatch.setenv("IFDB_EXTRACT_WRITE_BATCH_SIDECARS", "true")

    policy = ExtractBatchingPolicy(
        enabled=True,
        fetch_size=100,
        env_prefix="IFDB_EXTRACT_RIS_YDELSE",
    ).resolved(fallback_env_prefix="IFDB_EXTRACT")

    assert policy.write_batch_sidecars is False


def test_policy_rejects_invalid_defaults():
    with pytest.raises(ValueError, match="fetch_size"):
        ExtractBatchingPolicy(enabled=True, fetch_size=0, env_prefix="TEST")

    with pytest.raises(ValueError, match="log_every_batches"):
        ExtractBatchingPolicy(
            enabled=True,
            fetch_size=1,
            env_prefix="TEST",
            log_every_batches=0,
        )


def test_iter_sized_batches_preserves_order():
    assert list(iter_sized_batches(range(5), 2)) == [[0, 1], [2, 3], [4]]


def test_iter_sized_batches_rejects_invalid_size():
    with pytest.raises(ValueError, match="batch_size"):
        list(iter_sized_batches([1], 0))


def test_iter_cursor_fetchmany_uses_requested_size():
    cursor = _FakeCursor([[(1,), (2,)], [(3,)]])

    assert list(iter_cursor_fetchmany(cursor, 2)) == [[(1,), (2,)], [(3,)]]
    assert cursor.fetch_sizes == [2, 2, 2]


def test_iter_cursor_fetchmany_rejects_invalid_size():
    with pytest.raises(ValueError, match="batch_size"):
        list(iter_cursor_fetchmany(_FakeCursor([]), 0))
