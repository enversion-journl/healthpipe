# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared parallel execution utilities for ETL stages."""

from core.parallel.chunked_pipeline import (
    ChunkedPipelineStage,
    run_chunked_transform_post,
)
from core.parallel.policy import ParallelPolicy, ParallelStage, env_bool, env_int
from core.parallel.runner import (
    ParallelCall,
    ParallelExecutionSummary,
    resolve_worker_count,
    run_parallel_calls,
    run_parallel_map,
    run_parallel_stage,
)
from core.parallel.source_semaphore import SourceConnectionSemaphore
from core.parallel.spawn import (
    configure_spawn_import_paths_for_file,
    ensure_spawn_import_paths,
    resolve_spawn_import_paths_for_file,
)

__all__ = [
    "ChunkedPipelineStage",
    "ParallelCall",
    "ParallelExecutionSummary",
    "ParallelPolicy",
    "ParallelStage",
    "SourceConnectionSemaphore",
    "configure_spawn_import_paths_for_file",
    "ensure_spawn_import_paths",
    "env_bool",
    "env_int",
    "resolve_spawn_import_paths_for_file",
    "resolve_worker_count",
    "run_chunked_transform_post",
    "run_parallel_calls",
    "run_parallel_map",
    "run_parallel_stage",
]
