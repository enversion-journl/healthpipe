# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the sharded parallel write path in
:meth:`FHIRDatabaseResource.batch_upsert_resource_dicts`.

The single-connection fallback semantics live in
``test_fhir_database_batch_upsert_fallback.py``. This file covers the new
parallelism layer:

- worker resolution honours the row/batch/cap bounds
- sharded execution opens one connection per shard and routes the right
  rows to the right shard (correct txid offsets, no overlap)
- pipeline mode is engaged for the executemany batches
- worker-thread exceptions propagate
"""

from contextlib import contextmanager
from threading import Lock
from unittest.mock import MagicMock, patch

import pytest
from core.resources import (
    _DB_UPSERT_MAX_WORKERS,
    _DB_UPSERT_MIN_ROWS_PER_WORKER,
    FHIRDatabaseResource,
)


def _build_resource() -> FHIRDatabaseResource:
    return FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )


def _make_connection_factory(
    *,
    next_txid: int = 100,
    captured_connections: list[MagicMock] | None = None,
):
    """Return a ``get_connection`` patch target that hands out a fresh mock per call.

    The patch target replaces ``FHIRDatabaseResource.get_connection`` with a
    callable that returns a context manager wrapping a brand-new MagicMock
    connection on every invocation. Each connection records its own
    cursor.executemany / cursor.execute / pipeline / commit calls so the
    test can assert on per-shard behaviour.

    A shared lock guards ``next_txid`` so the parallel path's
    ``SELECT MAX(txid)`` returns the same starting value for the test even
    though shard workers race.
    """
    connections = captured_connections if captured_connections is not None else []
    txid_lock = Lock()
    txid_holder = {"value": next_txid}

    def _new_connection() -> MagicMock:
        connection = MagicMock(name="connection")

        cursor = MagicMock(name="cursor")
        cursor_cm = MagicMock(name="cursor_cm")
        cursor_cm.__enter__.return_value = cursor
        cursor_cm.__exit__.return_value = False
        connection.cursor.return_value = cursor_cm

        with txid_lock:
            cursor.fetchone.return_value = [txid_holder["value"]]

        pipeline_cm = MagicMock(name="pipeline_cm")
        pipeline_cm.__enter__.return_value = pipeline_cm
        pipeline_cm.__exit__.return_value = False
        connection.pipeline.return_value = pipeline_cm

        connection._test_cursor = cursor
        connection._test_pipeline = pipeline_cm
        connections.append(connection)
        return connection

    @contextmanager
    def _ctx_factory(self):
        conn = _new_connection()
        try:
            yield conn
        finally:
            pass

    return _ctx_factory, connections


def _sample_dict_resources(n: int) -> list[tuple[str, dict]]:
    return [
        (f"obs-{i}", {"resourceType": "Observation", "id": f"obs-{i}", "status": "final"})
        for i in range(n)
    ]


class TestResolveDictUpsertWorkers:
    def test_zero_rows_collapses_to_one_worker(self) -> None:
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=0, batch_size=2000, requested=None
            )
            == 1
        )

    def test_small_payload_stays_single_shard(self) -> None:
        # 1 batch worth of rows → cannot split, must be 1 shard
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=500, batch_size=2000, requested=None
            )
            == 1
        )

    def test_below_min_rows_per_worker_stays_single_shard(self) -> None:
        # Many batches but still under the per-worker rows floor → 1 shard
        n_rows = _DB_UPSERT_MIN_ROWS_PER_WORKER - 1
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=n_rows, batch_size=500, requested=None
            )
            == 1
        )

    def test_just_over_min_rows_collapses_to_one_shard(self) -> None:
        # Floor-division semantics: a payload only just past the MIN
        # threshold must NOT split into two shards (each shard would see
        # ~MIN/2 rows, defeating the per-shard fixed-cost amortisation).
        n_rows = _DB_UPSERT_MIN_ROWS_PER_WORKER + 1
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=n_rows, batch_size=500, requested=None
            )
            == 1
        )

    def test_exact_double_min_rows_authorises_two_shards(self) -> None:
        # Floor-division: 2x MIN is the smallest payload that authorises
        # 2 shards, and each shard then sees exactly MIN rows.
        n_rows = _DB_UPSERT_MIN_ROWS_PER_WORKER * 2
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=n_rows, batch_size=500, requested=None
            )
            == 2
        )

    def test_one_below_double_min_rows_stays_one_shard(self) -> None:
        # 2*MIN - 1 cannot satisfy "two shards each with >= MIN rows" →
        # falls back to 1 shard.
        n_rows = _DB_UPSERT_MIN_ROWS_PER_WORKER * 2 - 1
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=n_rows, batch_size=500, requested=None
            )
            == 1
        )

    def test_caps_at_configured_max(self) -> None:
        # Plenty of rows → cap kicks in
        n_rows = _DB_UPSERT_MIN_ROWS_PER_WORKER * (_DB_UPSERT_MAX_WORKERS + 5)
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=n_rows, batch_size=2000, requested=None
            )
            == _DB_UPSERT_MAX_WORKERS
        )

    def test_caps_at_n_batches(self) -> None:
        # Force the n_batches bound to dominate by using a huge batch_size
        n_rows = _DB_UPSERT_MIN_ROWS_PER_WORKER * _DB_UPSERT_MAX_WORKERS
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=n_rows,
                batch_size=n_rows,  # 1 batch only
                requested=None,
            )
            == 1
        )

    def test_explicit_request_overrides_default_cap(self) -> None:
        # Caller can ask for fewer than the env cap allows
        n_rows = _DB_UPSERT_MIN_ROWS_PER_WORKER * _DB_UPSERT_MAX_WORKERS * 4
        assert (
            FHIRDatabaseResource._resolve_dict_upsert_workers(
                n_rows=n_rows, batch_size=2000, requested=2
            )
            == 2
        )


class TestShardedDictUpsert:
    """End-to-end behaviour of the multi-connection write path."""

    # The production minimum-rows-per-worker bound (5 000) keeps small
    # payloads on the single-connection path. Drop it to 1 in these tests
    # so a few synthetic rows are enough to exercise the sharded code.
    MIN_ROWS_PER_WORKER_OVERRIDE = 1

    def _run_sharded(self, *, rows, workers: int, batch_size: int):
        resource = _build_resource()
        ctx_factory, connections = _make_connection_factory()

        with (
            patch(
                "core.resources._DB_UPSERT_MIN_ROWS_PER_WORKER",
                self.MIN_ROWS_PER_WORKER_OVERRIDE,
            ),
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(FHIRDatabaseResource, "get_connection", new=ctx_factory),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
        ):
            result = resource.batch_upsert_resource_dicts(
                resource_dicts=rows,
                resource_type="Observation",
                batch_size=batch_size,
                invalidate_cache=False,
                max_workers=workers,
            )
        return result, connections

    def test_opens_one_connection_per_shard_plus_one_for_txid_lookup(self) -> None:
        rows = _sample_dict_resources(8)  # 4 shards × 2 rows each
        (successes, failures, failed_ids), connections = self._run_sharded(
            rows=rows, workers=4, batch_size=2
        )

        assert successes == 8
        assert failures == 0
        assert failed_ids == []
        # 1 connection for SELECT MAX(txid) reservation + 4 shard connections
        assert len(connections) == 5

    def test_each_shard_commits_its_own_connection(self) -> None:
        rows = _sample_dict_resources(6)  # 3 shards × 2 rows
        _, connections = self._run_sharded(rows=rows, workers=3, batch_size=2)

        # The first connection is the txid-reservation one (no commit expected).
        # All shard connections must commit exactly once.
        commit_counts = [c.commit.call_count for c in connections]
        # Order isn't deterministic, but exactly one connection should have 0
        # commits (the txid-reservation one) and all others should have 1.
        assert sorted(commit_counts) == [0, 1, 1, 1]

    def test_pipeline_mode_is_used_for_each_executemany(self) -> None:
        rows = _sample_dict_resources(4)
        _, connections = self._run_sharded(rows=rows, workers=2, batch_size=2)

        # Skip the txid-reservation connection (it never enters pipeline mode).
        shard_connections = [c for c in connections if c.commit.call_count == 1]
        assert len(shard_connections) == 2
        for conn in shard_connections:
            # One executemany per batch → one pipeline-block per batch.
            executemany_calls = conn._test_cursor.executemany.call_count
            pipeline_calls = conn.pipeline.call_count
            assert executemany_calls == pipeline_calls
            assert executemany_calls >= 1

    def test_txid_offsets_are_contiguous_and_non_overlapping(self) -> None:
        # 6 rows split into 3 shards of 2 → expect txids 100..105 across
        # shards (100 from the mocked SELECT MAX(txid)).
        rows = _sample_dict_resources(6)
        _, connections = self._run_sharded(rows=rows, workers=3, batch_size=2)

        observed_txids: list[int] = []
        for conn in connections:
            cur = conn._test_cursor
            for call in cur.executemany.call_args_list:
                params = call.args[1]
                observed_txids.extend(p[1] for p in params)

        assert sorted(observed_txids) == [100, 101, 102, 103, 104, 105]

    def test_worker_exception_propagates_to_caller(self) -> None:
        resource = _build_resource()
        ctx_factory, connections = _make_connection_factory()

        # Make every shard connection's executemany raise a *non-database*
        # error (so the fallback path doesn't swallow it). The first
        # connection handed out is the txid-reservation one, so we let
        # that one succeed and make subsequent ones blow up.
        original_factory = ctx_factory

        call_index = {"n": 0}

        @contextmanager
        def _erroring_factory(self):
            with original_factory(self) as conn:
                call_index["n"] += 1
                if call_index["n"] > 1:  # first call is txid lookup
                    conn._test_cursor.executemany.side_effect = ValueError("non-database failure")
                yield conn

        with (
            patch(
                "core.resources._DB_UPSERT_MIN_ROWS_PER_WORKER",
                self.MIN_ROWS_PER_WORKER_OVERRIDE,
            ),
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(FHIRDatabaseResource, "get_connection", new=_erroring_factory),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            pytest.raises(ValueError, match="non-database failure"),
        ):
            resource.batch_upsert_resource_dicts(
                resource_dicts=_sample_dict_resources(4),
                resource_type="Observation",
                batch_size=2,
                invalidate_cache=False,
                max_workers=2,
            )
