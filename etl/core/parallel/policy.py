# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Policy primitives for core parallel execution."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

import dagster as dg

ExecutorKind = Literal["process", "thread"]


def env_int(name: str, default: int, *, minimum: int = 0) -> int:
    """Read int env var with a lower bound and safe default fallback."""
    try:
        value = dg.EnvVar.int(name).get_value(default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, cast(int, value))


def env_bool(name: str, default: bool = False) -> bool:
    """Read bool-ish env var values with a safe default fallback."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass(frozen=True)
class ParallelPolicy:
    """Execution policy for one parallel stage."""

    max_workers: int
    parallel_threshold: int
    chunksize: int
    executor_kind: ExecutorKind = "process"


@dataclass(frozen=True)
class ParallelStage:
    """Clean call-site contract for one parallel stage."""

    stage_id: str
    policy: ParallelPolicy
    max_logged_errors: int = 0
    mp_context: Any | None = None
    before_parallel_start: Callable[[], None] | None = None

    @classmethod
    def from_env(
        cls,
        *,
        stage_id: str,
        env_prefix: str,
        default_max_workers: int,
        default_parallel_threshold: int,
        default_chunksize: int,
        executor_kind: ExecutorKind = "process",
        default_max_logged_errors: int = 0,
        mp_context: Any | None = None,
        before_parallel_start: Callable[[], None] | None = None,
    ) -> ParallelStage:
        """Build stage config from the standard parallel env-var convention."""
        return cls(
            stage_id=stage_id,
            policy=ParallelPolicy(
                max_workers=env_int(
                    f"{env_prefix}_MAX_WORKERS",
                    default_max_workers,
                    minimum=1,
                ),
                parallel_threshold=env_int(
                    f"{env_prefix}_PARALLEL_THRESHOLD",
                    default_parallel_threshold,
                    minimum=1,
                ),
                chunksize=env_int(
                    f"{env_prefix}_CHUNKSIZE",
                    default_chunksize,
                    minimum=1,
                ),
                executor_kind=executor_kind,
            ),
            max_logged_errors=env_int(
                f"{env_prefix}_MAX_LOGGED_ERRORS",
                default_max_logged_errors,
                minimum=0,
            ),
            mp_context=mp_context,
            before_parallel_start=before_parallel_start,
        )

    def with_stage_id(self, stage_id: str) -> ParallelStage:
        """Reuse the same policy/runtime hooks with a more specific stage id."""
        return replace(self, stage_id=stage_id)
