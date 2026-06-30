# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for shared core.parallel runner semantics."""

from __future__ import annotations

import sys
from collections.abc import Iterable

from core.parallel import (
    ParallelCall,
    ParallelPolicy,
    ParallelStage,
    configure_spawn_import_paths_for_file,
    env_bool,
    env_int,
    resolve_worker_count,
    run_parallel_calls,
    run_parallel_map,
    run_parallel_stage,
)


class _LoggerStub:
    def __init__(self) -> None:
        self.infos: list[tuple[str, tuple]] = []
        self.warnings: list[tuple[str, tuple, dict]] = []

    def info(self, msg: str, *args) -> None:
        self.infos.append((msg, args))

    def warning(self, msg: str, *args, **kwargs) -> None:
        self.warnings.append((msg, args, kwargs))


def _square(x: int) -> int:
    return x * x


def _add(x: int, y: int = 1) -> int:
    return x + y


def test_env_int_falls_back_on_malformed_value(monkeypatch) -> None:
    monkeypatch.setenv("CORE_PARALLEL_TEST_INT", "abc")

    assert env_int("CORE_PARALLEL_TEST_INT", 7, minimum=1) == 7


def test_env_int_enforces_minimum(monkeypatch) -> None:
    monkeypatch.setenv("CORE_PARALLEL_TEST_INT", "-5")

    assert env_int("CORE_PARALLEL_TEST_INT", 7, minimum=1) == 1


def test_env_bool_accepts_common_values(monkeypatch) -> None:
    monkeypatch.setenv("CORE_PARALLEL_TEST_BOOL", "yes")
    assert env_bool("CORE_PARALLEL_TEST_BOOL") is True

    monkeypatch.setenv("CORE_PARALLEL_TEST_BOOL", "off")
    assert env_bool("CORE_PARALLEL_TEST_BOOL", default=True) is False

    monkeypatch.setenv("CORE_PARALLEL_TEST_BOOL", "surprise")
    assert env_bool("CORE_PARALLEL_TEST_BOOL", default=True) is True


def test_parallel_stage_from_env_uses_standard_names(monkeypatch) -> None:
    monkeypatch.setenv("CORE_PARALLEL_STAGE_MAX_WORKERS", "6")
    monkeypatch.setenv("CORE_PARALLEL_STAGE_PARALLEL_THRESHOLD", "50")
    monkeypatch.setenv("CORE_PARALLEL_STAGE_CHUNKSIZE", "25")
    monkeypatch.setenv("CORE_PARALLEL_STAGE_MAX_LOGGED_ERRORS", "3")

    stage = ParallelStage.from_env(
        stage_id="test.stage",
        env_prefix="CORE_PARALLEL_STAGE",
        default_max_workers=2,
        default_parallel_threshold=10,
        default_chunksize=5,
        default_max_logged_errors=1,
        executor_kind="thread",
    )

    assert stage.stage_id == "test.stage"
    assert stage.policy.max_workers == 6
    assert stage.policy.parallel_threshold == 50
    assert stage.policy.chunksize == 25
    assert stage.policy.executor_kind == "thread"
    assert stage.max_logged_errors == 3
    assert stage.with_stage_id("test.other").stage_id == "test.other"


def test_configure_spawn_import_paths_for_file_is_idempotent(monkeypatch, tmp_path) -> None:
    package_root = tmp_path / "dagster_etl"
    module_file = package_root / "DSA" / "FHIR" / "Observation" / "asset.py"
    module_file.parent.mkdir(parents=True)
    module_file.write_text("# test module\n")

    monkeypatch.setattr(sys, "path", [])
    monkeypatch.setenv("PYTHONPATH", "")

    ensure = configure_spawn_import_paths_for_file(str(module_file))
    ensure()

    assert sys.path == [str(package_root), str(tmp_path)]
    assert sys.path == list(dict.fromkeys(sys.path))


def test_resolve_worker_count_no_tasks() -> None:
    workers, reason = resolve_worker_count(
        configured_max_workers=8,
        task_count=0,
        parallel_threshold=1,
    )
    assert workers == 1
    assert reason == "no_tasks"


def test_resolve_worker_count_below_threshold() -> None:
    workers, reason = resolve_worker_count(
        configured_max_workers=8,
        task_count=99,
        parallel_threshold=100,
    )
    assert workers == 1
    assert reason == "below_threshold"


def test_run_parallel_map_thread_mode_parallel() -> None:
    logger = _LoggerStub()
    policy = ParallelPolicy(
        max_workers=4,
        parallel_threshold=1,
        chunksize=50,
        executor_kind="thread",
    )

    results, summary = run_parallel_map(
        stage_id="test.thread.parallel",
        tasks=[1, 2, 3, 4],
        worker_fn=_square,
        policy=policy,
        logger=logger,
    )

    assert results == [1, 4, 9, 16]
    assert summary.mode == "parallel"
    assert summary.worker_count >= 2


def test_run_parallel_map_can_stream_to_consumer_without_materializing() -> None:
    logger = _LoggerStub()
    calls: list[int] = []
    consumed: list[int] = []

    def _recording_worker(x: int) -> int:
        calls.append(x)
        return x * x

    def _consume_first(results_iter: Iterable[int]) -> None:
        consumed.append(next(iter(results_iter)))

    results, summary = run_parallel_map(
        stage_id="test.sequential.streaming",
        tasks=[1, 2, 3, 4],
        worker_fn=_recording_worker,
        policy=ParallelPolicy(
            max_workers=1,
            parallel_threshold=1,
            chunksize=50,
            executor_kind="thread",
        ),
        logger=logger,
        consume_results=_consume_first,
    )

    assert results is None
    assert consumed == [1]
    assert calls == [1]
    assert summary.mode == "sequential"


def test_run_parallel_map_thread_consumer_receives_iterator_not_list() -> None:
    logger = _LoggerStub()
    consumed: list[int] = []
    was_list: list[bool] = []

    def _consume_all(results_iter: Iterable[int]) -> None:
        was_list.append(isinstance(results_iter, list))
        consumed.extend(results_iter)

    results, summary = run_parallel_map(
        stage_id="test.thread.streaming",
        tasks=[1, 2, 3, 4],
        worker_fn=_square,
        policy=ParallelPolicy(
            max_workers=4,
            parallel_threshold=1,
            chunksize=50,
            executor_kind="thread",
        ),
        logger=logger,
        consume_results=_consume_all,
    )

    assert results is None
    assert consumed == [1, 4, 9, 16]
    assert was_list == [False]
    assert summary.mode == "parallel"


def test_run_parallel_stage_hides_policy_and_runtime_hooks() -> None:
    logger = _LoggerStub()
    hook_calls: list[str] = []
    stage = ParallelStage(
        stage_id="test.stage.wrapper",
        policy=ParallelPolicy(
            max_workers=2,
            parallel_threshold=1,
            chunksize=10,
            executor_kind="thread",
        ),
        before_parallel_start=lambda: hook_calls.append("called"),
    )

    results, summary = run_parallel_stage(
        stage,
        tasks=[1, 2, 3],
        worker_fn=_square,
        logger=logger,
    )

    assert results == [1, 4, 9]
    assert summary.mode == "parallel"
    assert hook_calls == ["called"]


def test_run_parallel_calls_returns_results_by_name() -> None:
    logger = _LoggerStub()

    results = run_parallel_calls(
        stage_id="test.named.calls",
        calls=[
            ParallelCall("left", _add, args=(1,), kwargs={"y": 2}),
            ParallelCall("right", _add, args=(10,), kwargs={"y": 5}),
        ],
        logger=logger,
        max_workers=2,
    )

    assert results == {"left": 3, "right": 15}


def test_run_parallel_map_process_fallback_to_sequential() -> None:
    logger = _LoggerStub()
    policy = ParallelPolicy(
        max_workers=4,
        parallel_threshold=1,
        chunksize=10,
        executor_kind="process",
    )

    # Lambdas are not pickleable for ProcessPoolExecutor. This forces the
    # parallel path to fail and validates standardized sequential fallback.
    results, summary = run_parallel_map(
        stage_id="test.process.fallback",
        tasks=[1, 2],
        worker_fn=lambda x: x + 1,  # noqa: E731
        policy=policy,
        logger=logger,
    )

    assert results == [2, 3]
    assert summary.mode == "sequential"
    assert summary.reason == "parallel_failed"
    assert logger.warnings, "Expected fallback warning log entry"


def test_kill_pool_workers_handles_empty_processes() -> None:
    """_kill_pool_workers must not raise when the pool has no child processes."""
    from core.parallel.runner import _kill_pool_workers

    class _FakePool:
        _processes: dict = {}

    _kill_pool_workers(_FakePool(), timeout=0.1)


def test_kill_pool_workers_handles_missing_attribute() -> None:
    """_kill_pool_workers must not raise when _processes is absent."""
    from core.parallel.runner import _kill_pool_workers

    _kill_pool_workers(object(), timeout=0.1)
