# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for core.parallel.chunked_pipeline.

These tests pin the orchestrator's contract so the migration of the five
unchunked CPO assets can rely on it. They use a fake FHIRDatabaseResource
that records suspend/cleanup calls; the helper itself is pure orchestration
so no real database is touched.

What is being pinned here:

- Empty rows do not invoke setup/transform/post and do not touch caches.
- A non-empty partition that fits in one chunk still goes through the
  setup-once + suspend + transform + post + finally-cleanup path.
- Multi-chunk partitions split correctly, run setup exactly once, and
  aggregate posted_count/failed_count from MaterializeResult metadata.
- Post functions returning ``int`` are also supported (fall back to
  posted=int, failed=0).
- Exceptions in setup/transform/post still trigger the finally-cleanup so
  the next partition starts with clean caches.
- ``ChunkedPipelineStage.from_env`` reads ``{PREFIX}_CHUNK_SIZE`` and falls
  back to the default for missing/empty/malformed values.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import dagster as dg
import pytest

# pytest.ini already adds the parent dir; this is a belt-and-braces guard
# in case the test is invoked outside the standard runner.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.parallel.chunked_pipeline import (  # noqa: E402
    ChunkedPipelineStage,
    run_chunked_transform_post,
)


class _FakeFHIRDatabase:
    """Minimal stand-in for FHIRDatabaseResource.

    Tracks suspend + finalize calls so tests can assert ordering and counts
    without spinning up a real resource.
    """

    def __init__(self) -> None:
        self.suspend_calls = 0
        self.finalize_calls: list[tuple[frozenset[str], str | None]] = []

    def suspend_identifier_caches(self) -> None:
        self.suspend_calls += 1

    def finalize_identifier_caches(
        self,
        resource_types: set[str],
        *,
        reason: str | None = None,
    ) -> None:
        self.finalize_calls.append((frozenset(resource_types), reason))


def _make_context(partition_key: str = "2026-01-01") -> MagicMock:
    """Build a minimal context that has the attributes the helper reads."""
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.has_partition_key = True
    ctx.partition_key = partition_key
    return ctx


def _materialize_result(posted: int, failed: int = 0, **extra) -> dg.MaterializeResult:
    metadata: dict[str, dg.MetadataValue] = {
        "posted_count": dg.MetadataValue.int(posted),
        "failed_count": dg.MetadataValue.int(failed),
    }
    for key, value in extra.items():
        if isinstance(value, str):
            metadata[key] = dg.MetadataValue.text(value)
        elif isinstance(value, int):
            metadata[key] = dg.MetadataValue.int(value)
    return dg.MaterializeResult(metadata=metadata)


@pytest.fixture
def fake_db() -> _FakeFHIRDatabase:
    return _FakeFHIRDatabase()


@pytest.fixture
def stage() -> ChunkedPipelineStage:
    return ChunkedPipelineStage(
        stage_id="test.stage",
        chunk_size=3,
        cleanup_resource_types=frozenset({"Condition"}),
    )


# ---------------------------------------------------------------------------
# Empty partition
# ---------------------------------------------------------------------------


def test_empty_rows_returns_zero_result_without_calling_anything(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    setup_calls: list[list[int]] = []
    transform_calls: list[tuple[list[int], object]] = []
    post_calls: list[tuple[list[int], list[int], object]] = []

    def setup(rows: list[int]) -> str:
        setup_calls.append(list(rows))
        return "setup"

    def transform(chunk: list[int], setup_value: str) -> list[int]:
        transform_calls.append((list(chunk), setup_value))
        return list(chunk)

    def post(resources: list[int], chunk: list[int], setup_value: str):
        post_calls.append((list(resources), list(chunk), setup_value))
        return 0

    result = run_chunked_transform_post(
        stage,
        rows=[],
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata["posted_count"].value == 0
    assert result.metadata["chunk_count"].value == 0
    assert result.metadata["extracted_count"].value == 0
    assert setup_calls == []
    assert transform_calls == []
    assert post_calls == []
    assert fake_db.suspend_calls == 0
    assert fake_db.finalize_calls == []


# ---------------------------------------------------------------------------
# Single-chunk partition
# ---------------------------------------------------------------------------


def test_under_chunk_size_runs_one_pass(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    rows = [1, 2]  # chunk_size=3, so single chunk

    setup_calls = 0
    transform_calls = 0
    post_calls = 0

    def setup(_rows: list[int]) -> str:
        nonlocal setup_calls
        setup_calls += 1
        return "shared"

    def transform(chunk: list[int], setup_value: str) -> list[int]:
        nonlocal transform_calls
        transform_calls += 1
        assert setup_value == "shared"
        return [r * 10 for r in chunk]

    def post(resources: list[int], chunk: list[int], setup_value: str):
        nonlocal post_calls
        post_calls += 1
        assert setup_value == "shared"
        assert resources == [10, 20]
        assert chunk == [1, 2]
        return _materialize_result(posted=2)

    result = run_chunked_transform_post(
        stage,
        rows=rows,
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )

    assert setup_calls == 1
    assert transform_calls == 1
    assert post_calls == 1
    assert fake_db.suspend_calls == 1
    assert result.metadata["posted_count"].value == 2
    assert result.metadata["chunk_count"].value == 1
    assert result.metadata["chunk_size"].value == 3
    assert result.metadata["extracted_count"].value == 2
    assert len(fake_db.finalize_calls) == 1
    types, reason = fake_db.finalize_calls[0]
    assert types == frozenset({"Condition"})
    assert reason is not None and "test.stage" in reason


# ---------------------------------------------------------------------------
# Multi-chunk partition
# ---------------------------------------------------------------------------


def test_over_chunk_size_splits_correctly(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    rows = list(range(7))  # 3 chunks: [0,1,2], [3,4,5], [6]
    seen_chunks: list[list[int]] = []

    def setup(_rows: list[int]) -> None:
        return None

    def transform(chunk: list[int], _setup: object) -> list[int]:
        seen_chunks.append(list(chunk))
        return list(chunk)

    def post(resources: list[int], _chunk: list[int], _setup: object):
        return _materialize_result(posted=len(resources), failed=0)

    result = run_chunked_transform_post(
        stage,
        rows=rows,
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )

    assert seen_chunks == [[0, 1, 2], [3, 4, 5], [6]]
    assert fake_db.suspend_calls == 3
    assert result.metadata["chunk_count"].value == 3
    assert result.metadata["posted_count"].value == 7
    assert result.metadata["extracted_count"].value == 7


def test_setup_fn_called_exactly_once_for_multi_chunk(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    setup_count = 0

    def setup(_rows: list[int]) -> int:
        nonlocal setup_count
        setup_count += 1
        return 42

    def transform(chunk: list[int], setup_value: int) -> list[int]:
        assert setup_value == 42
        return list(chunk)

    def post(resources: list[int], _chunk: list[int], _setup: int):
        return _materialize_result(posted=len(resources))

    run_chunked_transform_post(
        stage,
        rows=list(range(10)),  # 4 chunks of 3 + 1 trailing
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )
    assert setup_count == 1


def test_setup_fn_none_passes_none_to_transform_and_post(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    seen_setup_in_transform: list[object] = []
    seen_setup_in_post: list[object] = []

    def transform(chunk: list[int], setup_value: object) -> list[int]:
        seen_setup_in_transform.append(setup_value)
        return list(chunk)

    def post(resources: list[int], _chunk: list[int], setup_value: object):
        seen_setup_in_post.append(setup_value)
        return _materialize_result(posted=len(resources))

    run_chunked_transform_post(
        stage,
        rows=[1, 2, 3, 4],
        setup_fn=None,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )
    assert seen_setup_in_transform == [None, None]
    assert seen_setup_in_post == [None, None]


def test_suspend_identifier_caches_called_per_chunk(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    rows = list(range(9))  # exactly 3 chunks

    run_chunked_transform_post(
        stage,
        rows=rows,
        setup_fn=lambda _r: None,
        transform_fn=lambda chunk, _s: list(chunk),
        post_fn=lambda r, _c, _s: _materialize_result(posted=len(r)),
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )
    assert fake_db.suspend_calls == 3


def test_safe_clear_runs_in_finally_on_success(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    run_chunked_transform_post(
        stage,
        rows=[1, 2, 3],
        setup_fn=lambda _r: None,
        transform_fn=lambda chunk, _s: list(chunk),
        post_fn=lambda r, _c, _s: _materialize_result(posted=len(r)),
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )
    assert len(fake_db.finalize_calls) == 1
    types, _ = fake_db.finalize_calls[0]
    assert types == frozenset({"Condition"})


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


def test_exception_in_chunk_n_propagates_and_runs_cleanup(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    rows = list(range(7))  # 3 chunks
    chunks_seen = 0

    def transform(chunk: list[int], _setup: object) -> list[int]:
        nonlocal chunks_seen
        chunks_seen += 1
        if chunks_seen == 2:
            raise RuntimeError("boom in chunk 2")
        return list(chunk)

    with pytest.raises(RuntimeError, match="boom in chunk 2"):
        run_chunked_transform_post(
            stage,
            rows=rows,
            setup_fn=lambda _r: None,
            transform_fn=transform,
            post_fn=lambda r, _c, _s: _materialize_result(posted=len(r)),
            fhir_database=fake_db,  # type: ignore[arg-type]
            context=_make_context(),
        )

    assert len(fake_db.finalize_calls) == 1
    # First chunk's suspend + the failing chunk's suspend = 2.
    assert fake_db.suspend_calls == 2


def test_exception_in_setup_runs_cleanup(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    """If setup raises before any chunk is processed, cleanup still runs.

    No chunk work has started, so suspend_calls stays at 0.
    """

    def bad_setup(_rows: list[int]) -> None:
        raise RuntimeError("setup failed")

    with pytest.raises(RuntimeError, match="setup failed"):
        run_chunked_transform_post(
            stage,
            rows=[1, 2, 3],
            setup_fn=bad_setup,
            transform_fn=lambda chunk, _s: list(chunk),
            post_fn=lambda r, _c, _s: _materialize_result(posted=len(r)),
            fhir_database=fake_db,  # type: ignore[arg-type]
            context=_make_context(),
        )

    assert fake_db.suspend_calls == 0
    assert len(fake_db.finalize_calls) == 1


def test_post_returning_unsupported_type_raises_typeerror(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    with pytest.raises(TypeError, match="post_fn must return"):
        run_chunked_transform_post(
            stage,
            rows=[1, 2, 3],
            setup_fn=lambda _r: None,
            transform_fn=lambda chunk, _s: list(chunk),
            post_fn=lambda _r, _c, _s: "not allowed",  # type: ignore[return-value]
            fhir_database=fake_db,  # type: ignore[arg-type]
            context=_make_context(),
        )
    assert len(fake_db.finalize_calls) == 1


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def test_posted_and_failed_counts_summed_across_chunks(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    rows = list(range(7))  # chunks: 3, 3, 1
    posted_per_chunk = iter([3, 3, 1])
    failed_per_chunk = iter([0, 1, 0])

    def post(resources: list[int], _chunk: list[int], _setup: object):
        return _materialize_result(
            posted=next(posted_per_chunk),
            failed=next(failed_per_chunk),
            source="ifdb_oracle",
            method="direct_postgres",
        )

    result = run_chunked_transform_post(
        stage,
        rows=rows,
        setup_fn=lambda _r: None,
        transform_fn=lambda chunk, _s: list(chunk),
        post_fn=post,
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )

    assert result.metadata["posted_count"].value == 7
    assert result.metadata["failed_count"].value == 1
    assert result.metadata["chunk_count"].value == 3
    assert result.metadata["source"].value == "ifdb_oracle"
    assert result.metadata["method"].value == "direct_postgres"


def test_post_returning_int_is_treated_as_posted_count(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    rows = list(range(4))

    def post(resources: list[int], _chunk: list[int], _setup: object) -> int:
        return len(resources)

    result = run_chunked_transform_post(
        stage,
        rows=rows,
        setup_fn=lambda _r: None,
        transform_fn=lambda chunk, _s: list(chunk),
        post_fn=post,
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(),
    )
    assert result.metadata["posted_count"].value == 4
    assert result.metadata["failed_count"].value == 0


def test_metadata_carries_partition_key_and_chunk_size(
    stage: ChunkedPipelineStage, fake_db: _FakeFHIRDatabase
) -> None:
    result = run_chunked_transform_post(
        stage,
        rows=[1, 2],
        setup_fn=lambda _r: None,
        transform_fn=lambda chunk, _s: list(chunk),
        post_fn=lambda r, _c, _s: _materialize_result(posted=len(r)),
        fhir_database=fake_db,  # type: ignore[arg-type]
        context=_make_context(partition_key="2026-04-01"),
    )
    assert result.metadata["partition_key"].value == "2026-04-01"
    assert result.metadata["chunk_size"].value == 3


# ---------------------------------------------------------------------------
# from_env behaviour
# ---------------------------------------------------------------------------


def test_from_env_uses_env_var_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MYTEST_PIPE_CHUNK_SIZE", "12345")
    stage = ChunkedPipelineStage.from_env(
        stage_id="x",
        env_prefix="MYTEST_PIPE",
        default_chunk_size=100,
        cleanup_resource_types={"Condition"},
    )
    assert stage.chunk_size == 12345


def test_from_env_falls_back_to_default_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MYTEST_PIPE_CHUNK_SIZE", raising=False)
    stage = ChunkedPipelineStage.from_env(
        stage_id="x",
        env_prefix="MYTEST_PIPE",
        default_chunk_size=99,
        cleanup_resource_types={"Condition"},
    )
    assert stage.chunk_size == 99


def test_from_env_falls_back_for_malformed_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MYTEST_PIPE_CHUNK_SIZE", "not-a-number")
    stage = ChunkedPipelineStage.from_env(
        stage_id="x",
        env_prefix="MYTEST_PIPE",
        default_chunk_size=88,
        cleanup_resource_types={"Condition"},
    )
    assert stage.chunk_size == 88


def test_from_env_clamps_to_minimum_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MYTEST_PIPE_CHUNK_SIZE", "0")
    stage = ChunkedPipelineStage.from_env(
        stage_id="x",
        env_prefix="MYTEST_PIPE",
        default_chunk_size=88,
        cleanup_resource_types={"Condition"},
    )
    assert stage.chunk_size == 1


def test_direct_construction_rejects_chunk_size_below_one() -> None:
    with pytest.raises(ValueError, match="chunk_size must be >= 1"):
        ChunkedPipelineStage(
            stage_id="x",
            chunk_size=0,
            cleanup_resource_types=frozenset(),
        )
