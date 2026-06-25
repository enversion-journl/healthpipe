# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Core runner for standardized parallel/sequential stage execution."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from multiprocessing.context import BaseContext
from typing import Any, Literal, TypeVar

from core.parallel.policy import ParallelPolicy, ParallelStage

TaskT = TypeVar("TaskT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class ParallelCall:
    """Named function call for small thread-parallel fan-outs."""

    name: str
    fn: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ParallelExecutionSummary:
    """Execution metadata returned by ``run_parallel_map``."""

    mode: Literal["parallel", "sequential"]
    reason: str
    worker_count: int
    task_count: int
    executor_kind: str


def resolve_worker_count(
    *,
    configured_max_workers: int,
    task_count: int,
    parallel_threshold: int,
) -> tuple[int, str]:
    """Resolve effective worker count and mode reason."""
    max_workers = max(1, configured_max_workers)
    threshold = max(1, parallel_threshold)

    if task_count <= 0:
        return 1, "no_tasks"
    if max_workers <= 1:
        return 1, "max_workers<=1"
    if task_count < threshold:
        return 1, "below_threshold"
    return max(1, min(max_workers, task_count)), "parallel"


def _kill_pool_workers(pool: ProcessPoolExecutor, timeout: float = 5.0) -> None:
    """Best-effort terminate/kill all child processes in a ProcessPoolExecutor.

    Used on cancellation (SIGTERM from Dagster UI) to stop spawn'd workers that
    would otherwise continue running their SQL queries until completion.
    """
    procs = list(getattr(pool, "_processes", {}).values())
    for p in procs:
        try:
            p.terminate()
        except OSError:
            pass
    deadline = time.monotonic() + timeout
    for p in procs:
        remaining = max(0, deadline - time.monotonic())
        p.join(timeout=remaining)
        if p.is_alive():
            try:
                p.kill()
            except OSError:
                pass


def run_parallel_map(
    *,
    stage_id: str,
    tasks: Sequence[TaskT],
    worker_fn: Callable[[TaskT], ResultT],
    policy: ParallelPolicy,
    logger: Any,
    mp_context: BaseContext | None = None,
    before_parallel_start: Callable[[], None] | None = None,
    consume_results: Callable[[Iterable[ResultT]], None] | None = None,
) -> tuple[list[ResultT] | None, ParallelExecutionSummary]:
    """Run ``worker_fn`` over ``tasks`` using standardized parallel semantics.

    If ``consume_results`` is supplied, results are passed to that callable as
    an iterator while the executor is still alive. This keeps memory-sensitive
    stages from materialising a second full result list.
    """
    task_count = len(tasks)
    worker_count, reason = resolve_worker_count(
        configured_max_workers=policy.max_workers,
        task_count=task_count,
        parallel_threshold=policy.parallel_threshold,
    )
    chunksize = max(1, policy.chunksize)

    def _collect_or_consume(results_iter: Iterable[ResultT]) -> list[ResultT] | None:
        if consume_results is None:
            return list(results_iter)
        consume_results(results_iter)
        return None

    if worker_count <= 1:
        logger.info(
            "%s mode=sequential reason=%s tasks=%d",
            stage_id,
            reason,
            task_count,
        )
        results = _collect_or_consume(map(worker_fn, tasks))
        return results, ParallelExecutionSummary(
            mode="sequential",
            reason=reason,
            worker_count=1,
            task_count=task_count,
            executor_kind=policy.executor_kind,
        )

    if before_parallel_start:
        before_parallel_start()

    try:
        logger.info(
            "%s mode=parallel executor=%s workers=%d tasks=%d chunksize=%d",
            stage_id,
            policy.executor_kind,
            worker_count,
            task_count,
            chunksize,
        )
        if policy.executor_kind == "process":
            pool = ProcessPoolExecutor(
                max_workers=worker_count,
                mp_context=mp_context,
            )
            futures = [pool.submit(worker_fn, t) for t in tasks]
            try:
                done_iter = (f.result() for f in as_completed(futures))
                results = _collect_or_consume(done_iter)
                pool.shutdown(wait=True)
            except BaseException:
                for f in futures:
                    f.cancel()
                _kill_pool_workers(pool)
                pool.shutdown(wait=False, cancel_futures=True)
                raise
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as pool:
                results = _collect_or_consume(pool.map(worker_fn, tasks))

        return results, ParallelExecutionSummary(
            mode="parallel",
            reason="parallel",
            worker_count=worker_count,
            task_count=task_count,
            executor_kind=policy.executor_kind,
        )
    except Exception as exc:
        logger.warning(
            "%s parallel execution failed; falling back to sequential: %s",
            stage_id,
            exc,
            exc_info=True,
        )
        results = _collect_or_consume(map(worker_fn, tasks))
        return results, ParallelExecutionSummary(
            mode="sequential",
            reason="parallel_failed",
            worker_count=1,
            task_count=task_count,
            executor_kind=policy.executor_kind,
        )


def run_parallel_stage(
    stage: ParallelStage,
    *,
    tasks: Sequence[TaskT],
    worker_fn: Callable[[TaskT], ResultT],
    logger: Any,
    consume_results: Callable[[Iterable[ResultT]], None] | None = None,
) -> tuple[list[ResultT] | None, ParallelExecutionSummary]:
    """Run a configured stage with no executor/policy details at the call site."""
    return run_parallel_map(
        stage_id=stage.stage_id,
        tasks=tasks,
        worker_fn=worker_fn,
        policy=stage.policy,
        logger=logger,
        mp_context=stage.mp_context,
        before_parallel_start=stage.before_parallel_start,
        consume_results=consume_results,
    )


def run_parallel_calls(
    *,
    stage_id: str,
    calls: Sequence[ParallelCall],
    logger: Any,
    max_workers: int | None = None,
    thread_name_prefix: str | None = None,
) -> dict[str, Any]:
    """Run a small set of named calls concurrently and return results by name."""
    call_list = list(calls)
    if not call_list:
        return {}

    worker_count = min(max(1, max_workers or len(call_list)), len(call_list))
    if worker_count == 1:
        logger.info("%s mode=sequential calls=%d", stage_id, len(call_list))
        return {call.name: call.fn(*call.args, **dict(call.kwargs)) for call in call_list}

    logger.info(
        "%s mode=parallel executor=thread workers=%d calls=%d",
        stage_id,
        worker_count,
        len(call_list),
    )
    results: dict[str, Any] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix=thread_name_prefix,
    ) as pool:
        futures = {
            pool.submit(call.fn, *call.args, **dict(call.kwargs)): call.name for call in call_list
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return results
