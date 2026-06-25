# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Generic chunked transform+post pipeline for IFDB CPO assets.

This module owns *only* the orchestration of "split rows into chunks, transform
each chunk into FHIR resources, post each chunk, aggregate results". It must
not contain any FHIR-domain knowledge: no reference configs, no identifier
systems, no per-resource dependency rules, no Pydantic-vs-dict payload
specifics. Those live in the per-asset transform/post helpers that this
orchestrator calls.

Design contract:

- ``setup_fn`` is called *once* per partition before any chunk is processed.
  Per-partition lookups (e.g. tillægskoder, encounter refs) belong here so
  they aren't re-fetched per chunk.
- For each chunk: ``suspend_identifier_caches()`` is called to release mmap
  memory, then ``transform_fn(chunk, setup)`` builds resources, then
  ``post_fn(resources, chunk, setup)`` posts them. The post function is
  responsible for its own reference resolution, ID resolution, and
  dependency mutation invalidation — exactly as it did before chunking
  was introduced.
- ``cleanup_resource_types`` are finalized via
  ``finalize_identifier_caches`` in a ``finally`` block so cache files are
  purged and in-memory state is cleared when the partition is done.
- A single aggregated ``dg.MaterializeResult`` is returned with the same
  metadata keys the underlying post helpers already emit (posted_count,
  failed_count, source, partition_key, method) plus chunk-level stats.

Partial-persistence semantics: a failure in chunk N means chunks 0..N-1 are
already persisted. Retry must be idempotent at the post layer — the shared
``prepare_resources_for_upsert`` + upsert path provides this.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import dagster as dg
from core.parallel.policy import env_int

if TYPE_CHECKING:
    from core.resources import FHIRDatabaseResource

RowT = TypeVar("RowT")
SetupT = TypeVar("SetupT")
ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True)
class ChunkedPipelineStage:
    """Configuration for one chunked transform+post stage.

    Attributes:
        stage_id: Stable identifier for logs and tracing
            (e.g. ``"ifdb.condition.diagnose.transform_post"``).
        chunk_size: Maximum number of input rows processed per chunk. Must
            be >= 1.
        cleanup_resource_types: FHIR resource types whose identifier caches
            are purged and cleared in the ``finally`` block after the
            partition finishes. Typically the resource type that was just
            posted.
    """

    stage_id: str
    chunk_size: int
    cleanup_resource_types: frozenset[str]

    def __post_init__(self) -> None:
        if self.chunk_size < 1:
            raise ValueError(
                f"ChunkedPipelineStage.chunk_size must be >= 1, "
                f"got {self.chunk_size} for stage_id={self.stage_id!r}"
            )

    @classmethod
    def from_env(
        cls,
        *,
        stage_id: str,
        env_prefix: str,
        default_chunk_size: int,
        cleanup_resource_types: set[str] | frozenset[str],
    ) -> ChunkedPipelineStage:
        """Build a stage config, reading ``{env_prefix}_CHUNK_SIZE`` if set.

        Falls back to ``default_chunk_size`` when the env var is missing,
        empty, malformed, or below 1. Reuses ``core.parallel.env_int`` so
        the behaviour matches every other parallel stage on the platform.
        """
        chunk_size = env_int(
            f"{env_prefix}_CHUNK_SIZE",
            default_chunk_size,
            minimum=1,
        )
        return cls(
            stage_id=stage_id,
            chunk_size=chunk_size,
            cleanup_resource_types=frozenset(cleanup_resource_types),
        )


@dataclass
class _ChunkAggregate(Generic[SetupT]):
    posted_count: int = 0
    failed_count: int = 0
    chunk_count: int = 0
    source: str = "ifdb_oracle"
    method: str = "direct_postgres"
    extra_metadata: dict[str, Any] | None = None


def _read_int_metadata(
    materialize_result: dg.MaterializeResult,
    key: str,
    default: int = 0,
) -> int:
    """Pull an integer metadata value off a MaterializeResult safely.

    Falls back to ``default`` when the key is missing or the value is not
    an int-compatible MetadataValue. Defensive on purpose: if a post helper
    ever returns a malformed result we want to log zero rather than crash
    the chunk loop and lose progress on already-posted chunks.
    """
    if not materialize_result.metadata:
        return default
    value = materialize_result.metadata.get(key)
    if value is None:
        return default
    raw = getattr(value, "value", value)
    if isinstance(raw, bool):
        return default
    if isinstance(raw, int):
        return raw
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _read_str_metadata(
    materialize_result: dg.MaterializeResult,
    key: str,
    default: str,
) -> str:
    if not materialize_result.metadata:
        return default
    value = materialize_result.metadata.get(key)
    if value is None:
        return default
    raw = getattr(value, "value", value)
    if isinstance(raw, str):
        return raw
    return default


def run_chunked_transform_post(
    stage: ChunkedPipelineStage,
    *,
    rows: list[RowT],
    transform_fn: Callable[[list[RowT], SetupT], list[ResourceT]],
    post_fn: Callable[
        [list[ResourceT], list[RowT], SetupT],
        int | dg.MaterializeResult,
    ],
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
    setup_fn: Callable[[list[RowT]], SetupT] | None = None,
) -> dg.MaterializeResult:
    """Run setup once, then transform+post per chunk, with cache hygiene.

    Args:
        stage: Stage configuration (id, chunk size, cleanup resource types).
        rows: Extracted source rows for this partition.
        transform_fn: ``(chunk, setup) -> resources``. Called once per chunk.
        post_fn: ``(resources, chunk, setup) -> int | MaterializeResult``.
            Called once per chunk with the resources produced by
            ``transform_fn``. The same ``setup`` value is passed in for
            convenience (some post functions use lookups built in setup).
            Return value:
              - ``int``: interpreted as posted count, failed=0.
              - ``MaterializeResult``: posted/failed are read from metadata
                using the same keys the existing IFDB post helpers already
                emit (``posted_count``, ``failed_count``, ``source``,
                ``method``).
        fhir_database: Used for cache suspension before each chunk's
            transform and for cleanup in ``finally``.
        context: Dagster execution context, used for logging and partition
            metadata. The helper does not modify it.
        setup_fn: Optional ``rows -> setup`` callable invoked exactly once
            before any chunk is processed. When ``None`` the value passed to
            ``transform_fn`` and ``post_fn`` is also ``None``. Use this for
            per-partition lookups that must not be re-fetched per chunk.

    Returns:
        A single ``dg.MaterializeResult`` with aggregated metadata:
        ``posted_count``, ``failed_count``, ``chunk_count``, ``chunk_size``,
        ``extracted_count``, ``partition_key``, ``source``, ``method``.

    Behaviour:
        - Empty rows: returns a zero-result without invoking ``setup_fn``,
          ``transform_fn``, or ``post_fn``. Cleanup of cache types is still
          considered idempotent and is skipped to avoid touching caches when
          nothing was posted.
        - Single chunk: same code path as multi-chunk. Setup runs once,
          one chunk is transformed and posted, cleanup runs.
        - Multi-chunk: ``suspend_identifier_caches()`` is called *before*
          each chunk's transform to release SQLite mmap memory. Chunk-local
          intermediates (``resources``, ``chunk``) are explicitly ``del``-ed
          after each post so the next chunk runs at a clean memory baseline.
        - Exceptions: any exception in setup, transform, or post propagates
          out, but the ``finally`` cleanup of cache types still runs so the
          next partition starts cleanly. Chunks already posted are not
          rolled back — retry must be idempotent at the post layer.
    """
    logger = dg.get_dagster_logger()
    partition_key = context.partition_key if context.has_partition_key else "N/A"
    total_rows = len(rows)
    chunk_size = stage.chunk_size

    if total_rows == 0:
        logger.info(
            "[%s] no rows to process for partition=%s",
            stage.stage_id,
            partition_key,
        )
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(0),
                "failed_count": dg.MetadataValue.int(0),
                "chunk_count": dg.MetadataValue.int(0),
                "chunk_size": dg.MetadataValue.int(chunk_size),
                "extracted_count": dg.MetadataValue.int(0),
                "partition_key": dg.MetadataValue.text(partition_key),
                "source": dg.MetadataValue.text("ifdb_oracle"),
                "method": dg.MetadataValue.text("direct_postgres"),
            }
        )

    n_chunks = (total_rows + chunk_size - 1) // chunk_size
    logger.info(
        "[%s] chunked transform+post: %d rows in %d chunks of %d (partition=%s)",
        stage.stage_id,
        total_rows,
        n_chunks,
        chunk_size,
        partition_key,
    )

    aggregate = _ChunkAggregate[SetupT]()

    try:
        # Run setup exactly once before any chunk. Keep it inside the
        # try/finally so the documented cleanup contract also holds if a
        # future setup function touches identifier-cache state before raising.
        setup: SetupT
        if setup_fn is not None:
            setup = setup_fn(rows)
        else:
            # Cast through Any: callers using ``setup_fn=None`` should declare
            # SetupT as ``None`` in their type annotations; we don't enforce it
            # at runtime.
            setup = None  # type: ignore[assignment]

        for chunk_idx, start in enumerate(range(0, total_rows, chunk_size), start=1):
            chunk = rows[start : start + chunk_size]
            logger.info(
                "[%s] chunk %d/%d: %d rows",
                stage.stage_id,
                chunk_idx,
                n_chunks,
                len(chunk),
            )

            fhir_database.suspend_identifier_caches()

            resources = transform_fn(chunk, setup)
            post_result = post_fn(resources, chunk, setup)

            if isinstance(post_result, dg.MaterializeResult):
                aggregate.posted_count += _read_int_metadata(post_result, "posted_count")
                aggregate.failed_count += _read_int_metadata(post_result, "failed_count")
                # Carry through the source/method labels from the underlying
                # post helper so the aggregated result still identifies which
                # backend handled the writes.
                aggregate.source = _read_str_metadata(post_result, "source", aggregate.source)
                aggregate.method = _read_str_metadata(post_result, "method", aggregate.method)
            elif isinstance(post_result, int):
                aggregate.posted_count += post_result
            else:
                raise TypeError(
                    f"[{stage.stage_id}] post_fn must return int or "
                    f"MaterializeResult, got {type(post_result).__name__}"
                )

            aggregate.chunk_count += 1

            # Drop chunk-local references so the next iteration starts at a
            # clean memory baseline. Pydantic resource lists from FHIR
            # transforms are the dominant memory footprint.
            del resources
            del chunk
    finally:
        if stage.cleanup_resource_types:
            fhir_database.finalize_identifier_caches(
                set(stage.cleanup_resource_types),
                reason=f"{stage.stage_id} post-partition cleanup",
            )

    logger.info(
        "[%s] chunked complete: %d posted, %d failed across %d chunks " "(partition=%s)",
        stage.stage_id,
        aggregate.posted_count,
        aggregate.failed_count,
        aggregate.chunk_count,
        partition_key,
    )

    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(aggregate.posted_count),
            "failed_count": dg.MetadataValue.int(aggregate.failed_count),
            "chunk_count": dg.MetadataValue.int(aggregate.chunk_count),
            "chunk_size": dg.MetadataValue.int(chunk_size),
            "extracted_count": dg.MetadataValue.int(total_rows),
            "partition_key": dg.MetadataValue.text(partition_key),
            "source": dg.MetadataValue.text(aggregate.source),
            "method": dg.MetadataValue.text(aggregate.method),
        }
    )
