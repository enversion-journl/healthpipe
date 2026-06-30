# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Source-agnostic materialization helpers for batched extract assets."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

import dagster as dg
from core.extract.batching import ExtractBatchingPolicy
from core.extract.pickle_list import write_batches_to_pickle_list
from core.extract.protocols import RowBatchSource


@dataclass(frozen=True)
class BatchedPickleListArtifact:
    """Pre-materialized list pickle produced by a batched extract asset."""

    temp_path: str
    batch_dir: str | None
    row_count: int
    batch_count: int
    column_names: tuple[str, ...]
    source_label: str
    batch_size: int


def materialize_row_batches_to_pickle_artifact(
    *,
    batch_source: RowBatchSource,
    policy: ExtractBatchingPolicy,
    context: dg.AssetExecutionContext,
    source_label: str,
    temp_dir: str | None = None,
) -> BatchedPickleListArtifact:
    """Stream row batches to a pickle artifact without building one big list."""

    logger = context.log
    partition_key = context.partition_key if context.has_partition_key else "N/A"
    effective_temp_dir = temp_dir or tempfile.gettempdir()

    def _on_batch(batch_count: int, row_count: int) -> None:
        if batch_count == 1 or batch_count % policy.log_every_batches == 0:
            logger.info(
                "[%s] materialized %d rows across %d batches " "(batch_size=%d, partition=%s)",
                source_label,
                row_count,
                batch_count,
                policy.fetch_size,
                partition_key,
            )

    try:
        write_result = write_batches_to_pickle_list(
            batch_source.iter_batches(),
            temp_dir=effective_temp_dir,
            on_batch=_on_batch,
            write_batch_sidecars=policy.write_batch_sidecars,
        )
    finally:
        close = getattr(batch_source, "close", None)
        if callable(close):
            close()

    column_names = tuple(batch_source.column_names)
    artifact = BatchedPickleListArtifact(
        temp_path=write_result.path,
        batch_dir=write_result.batch_dir,
        row_count=write_result.row_count,
        batch_count=write_result.batch_count,
        column_names=column_names,
        source_label=source_label,
        batch_size=policy.fetch_size,
    )

    context.add_output_metadata(
        {
            "batched_extract": dg.MetadataValue.bool(True),
            "row_count": dg.MetadataValue.int(artifact.row_count),
            "batch_count": dg.MetadataValue.int(artifact.batch_count),
            "batch_size": dg.MetadataValue.int(artifact.batch_size),
            "streamable_batches": dg.MetadataValue.bool(artifact.batch_dir is not None),
            "source_label": dg.MetadataValue.text(source_label),
            "column_count": dg.MetadataValue.int(len(column_names)),
            "temp_path": dg.MetadataValue.path(str(Path(artifact.temp_path))),
        }
    )
    logger.info(
        "[%s] batched extract materialized %d rows across %d batches",
        source_label,
        artifact.row_count,
        artifact.batch_count,
    )
    return artifact
