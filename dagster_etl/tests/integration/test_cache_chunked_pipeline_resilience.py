# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Integration-style resilience tests for cache behavior in chunked pipelines."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import dagster as dg
from core.cache import CacheCoordinator
from core.parallel.chunked_pipeline import ChunkedPipelineStage, run_chunked_transform_post
from core.resources import FHIRDatabaseResource, SqliteIdentifierCache

_SYSTEM = "urn:test:chunked-resilience:patient"


@dataclass
class _Setup:
    lookup: SqliteIdentifierCache
    db_path: str
    marker_path: Path
    cache_key: tuple[str, frozenset[str]]


def _build_resource(tmp_path: Path) -> FHIRDatabaseResource:
    resource = FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    return resource


def _context() -> MagicMock:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.has_partition_key = True
    ctx.partition_key = "2026-05-01"
    return ctx


def test_chunked_pipeline_survives_per_chunk_invalidation_and_reopen(
    tmp_path: Path,
) -> None:
    rows = [f"P{i}" for i in range(1, 9)]
    resource = _build_resource(tmp_path)
    stage = ChunkedPipelineStage(
        stage_id="integration.cache.chunked.resilience",
        chunk_size=3,
        cleanup_resource_types=frozenset({"Patient"}),
    )
    setup_ref: dict[str, _Setup] = {}

    def setup_fn(_rows: list[str]) -> _Setup:
        db_path = SqliteIdentifierCache.cache_path(
            "Patient",
            frozenset([_SYSTEM]),
            cache_dir=str(tmp_path),
        )
        SqliteIdentifierCache.build_from_iter(
            ((_SYSTEM, value, f"patient-{value}") for value in rows),
            db_path,
        )
        lookup = SqliteIdentifierCache(db_path)
        cache_key = ("Patient", frozenset([_SYSTEM]))
        resource._identifier_caches[cache_key] = lookup
        resource._identifier_cache = lookup
        resource._active_cache_key = cache_key
        setup = _Setup(
            lookup=lookup,
            db_path=db_path,
            marker_path=tmp_path / "patient.invalidated",
            cache_key=cache_key,
        )
        setup_ref["setup"] = setup
        return setup

    def transform_fn(chunk: list[str], setup: _Setup) -> list[str]:
        # Force a reopen path per chunk. This is the exact edge where file
        # deletion previously caused sqlite "unable to open database file".
        setup.lookup.close()
        resolved: list[str] = []
        for value in chunk:
            fhir_id = setup.lookup.get((_SYSTEM, value))
            assert fhir_id is not None
            resolved.append(fhir_id)
        return resolved

    def post_fn(resources: list[str], _chunk: list[str], setup: _Setup) -> dg.MaterializeResult:
        # Simulate frequent invalidation pressure while reusing the same
        # lookup object across chunks.
        resource.clear_identifier_caches_for_resources({"Patient"})
        resource._identifier_caches[setup.cache_key] = setup.lookup
        resource._identifier_cache = setup.lookup
        resource._active_cache_key = setup.cache_key
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(len(resources)),
                "failed_count": dg.MetadataValue.int(0),
            }
        )

    result = run_chunked_transform_post(
        stage,
        rows=rows,
        setup_fn=setup_fn,
        transform_fn=transform_fn,
        post_fn=post_fn,
        fhir_database=resource,
        context=_context(),
    )

    assert result.metadata["posted_count"].value == len(rows)
    assert result.metadata["failed_count"].value == 0
    assert result.metadata["chunk_count"].value == 3

    setup = setup_ref["setup"]
    assert setup.marker_path.exists()
    # After finalize_identifier_caches in the finally block, cache files
    # are purged. The file should no longer exist on disk.
    assert not os.path.exists(setup.db_path)
