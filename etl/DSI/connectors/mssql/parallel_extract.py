# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Parallel full-load extraction engine: keyset window planning, windowed SELECT
building, ordering-key autodetect, the streaming extract loop, and the spawn
process-pool workers.

Each worker owns a half-open key range and streams it (keyset windowing, not
growing-offset paging which is O(N^2) on a large heap). The single production
entry point is ``parallel_stream_extract_and_insert``; ``should_run_parallel``
is the gate.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import queue
import resource
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any

from core.parallel import ParallelPolicy, ParallelStage, run_parallel_stage
from core.parallel.spawn import configure_spawn_import_paths_for_file
from core.resources import _connect_mssql_python, build_mssql_conn_str
from DSI.connectors.mssql.bulk_copy import (
    build_bcp_truncation_limits,
    compute_streaming_chunk_size,
)
from DSI.connectors.mssql.extractor import (
    create_ext_table_ddl,
    stream_bulkcopy_chunks,
)
from DSI.connectors.mssql.models import (
    TABLE_TYPE_BRIDGE,
    TABLE_TYPE_DEPENDENT,
    TableConfig,
    TableLoadResult,
)
from DSI.connectors.mssql.sql_builders import (
    build_relation_where_clause,
    get_table_columns_info,
)

# The supported public surface. Everything else (window planning, the streaming
# loop, the spawn-pool plumbing) is internal and may change without notice.
__all__ = ["parallel_stream_extract_and_insert", "should_run_parallel"]

# ---------------------------------------------------------------------------
# Parallel gating helpers
# ---------------------------------------------------------------------------

# Hard ceiling on parallel workers (= source connections) per table. Env-tunable
# so it can be raised beyond the current sweet spot without a code change (raise
# the matching *_MAX_SOURCE_CONNS budget too so the semaphore doesn't gate). The
# real RAM ceiling is the dagster code-server (~4 GB dev/qa, ~20 GB prod), so keep
# worker counts low enough that concurrent full-load assets don't OOM it.
MAX_SOURCE_CONNECTIONS = int(os.getenv("EXT_MAX_SOURCE_CONNECTIONS", "16"))


def should_run_parallel(config, *, is_full_load: bool, is_mssql_python: bool) -> bool:
    """True iff the table is opted-in (>1 worker), this is a full-load run, the
    source driver supports bulk-copy (mssql-python), and the global kill-switch
    is off."""
    if os.environ.get("EXT_PARALLEL_DISABLE", "false").lower() == "true":
        return False
    return (
        bool(config.parallel_workers and config.parallel_workers > 1)
        and is_full_load
        and is_mssql_python
    )


def source_budget(source_system: str, default: int = 16) -> int:
    """Per-source-system max concurrent connections (env-tunable)."""
    env = f"{source_system.upper()}_MAX_SOURCE_CONNS"
    try:
        return max(1, int(os.environ.get(env, default)))
    except (TypeError, ValueError):
        return default


def clamp_worker_count(config) -> int:
    """Per-table worker count, clamped to [1, MAX_SOURCE_CONNECTIONS]. (Distinct
    from the framework's task-count-driven resolve_worker_count — this is a
    per-table opt-in clamped to the source-connection ceiling.)"""
    return max(1, min(MAX_SOURCE_CONNECTIONS, int(config.parallel_workers or 1)))


def read_rss_mb() -> float:
    """Current resident set size in MB. Linux reads /proc; else uses getrusage."""
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/self/statm", encoding="ascii") as fh:
                resident_pages = int(fh.read().split()[1])
            page_size = resource.getpagesize()
            return resident_pages * page_size / (1024 * 1024)
        except (OSError, ValueError, IndexError):
            pass
    # Fallback: peak RSS for the whole process. ru_maxrss is KiB on Linux,
    # bytes on macOS.
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return maxrss / (1024 * 1024)
    return maxrss / 1024


class PeakRssSampler:
    """Background sampler that tracks the peak RSS (MB) during a trial.

    Polls every ``interval`` seconds. On Linux this gives a true per-trial peak
    (independent of other trials in the same process); elsewhere it degrades to
    the process-lifetime peak, which is acceptable for correctness-only local
    runs.
    """

    def __init__(self, interval: float = 0.2):
        self._interval = interval
        self._peak = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> PeakRssSampler:
        self._peak = read_rss_mb()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            current = read_rss_mb()
            if current > self._peak:
                self._peak = current

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        current = read_rss_mb()
        if current > self._peak:
            self._peak = current

    @property
    def peak_mb(self) -> float:
        return self._peak


@dataclass(frozen=True)
class KeysetRange:
    lo: Any
    hi: Any | None  # None => unbounded upper (last worker)
    is_last: bool
    is_first: bool = False  # first window also sweeps NULL-keyed rows


def _mark_first(ranges: list[KeysetRange]) -> list[KeysetRange]:
    """Tag the first window so build_keyset_select sweeps NULL-keyed rows into it.
    NULL keys sort first and satisfy no >=/< bound, so without this they'd be
    extracted by no window at all (silent data loss)."""
    if ranges:
        ranges[0] = replace(ranges[0], is_first=True)
    return ranges


def _col_list(column_names: list[str]) -> str:
    return ", ".join(f"[{c}]" for c in column_names)


def _where(base_where: str | None, window_pred: str | None) -> str:
    """Combine an optional base predicate with an optional window predicate."""
    parts = []
    if base_where:
        parts.append(f"({base_where})")
    if window_pred:
        parts.append(window_pred)
    return (" WHERE " + " AND ".join(parts)) if parts else ""


def _sql_literal(value: Any) -> str:
    """Encode a key value as a T-SQL literal (ints, strings, dates)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, datetime | date):
        return f"'{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


def build_keyset_select(
    column_names: list[str],
    schema: str,
    table: str,
    ordering_key: str,
    rng: KeysetRange,
    base_where: str | None = None,
) -> str:
    preds: list[str] = []
    if rng.lo is not None:
        preds.append(f"[{ordering_key}] >= {_sql_literal(rng.lo)}")
    if rng.hi is not None:
        # Inner windows are half-open [lo, hi); the last window, when it carries
        # an upper bound at all, is a row_limit cap and must be inclusive so the
        # cap row itself is included. A last window with hi=None is a full-load
        # (unbounded) tail.
        op = "<=" if rng.is_last else "<"
        preds.append(f"[{ordering_key}] {op} {_sql_literal(rng.hi)}")
    window_pred = " AND ".join(preds) if preds else None
    if rng.is_first and window_pred is not None:
        # NULL keys compare UNKNOWN against every >=/< bound, so they match no
        # window. They sort first, so fold them into the first window exactly once.
        window_pred = f"({window_pred} OR [{ordering_key}] IS NULL)"
    return (
        f"SELECT {_col_list(column_names)} FROM [{schema}].[{table}]"
        f"{_where(base_where, window_pred)} ORDER BY [{ordering_key}]"
    )


def detect_ordering_key(
    source_conn, schema: str, table: str, override: str | None
) -> tuple[str, bool]:
    """Return ``(ordering_key, is_unique)``.

    Resolution order: explicit override -> clustered index leading column ->
    primary-key leading column -> first column. ``is_unique`` reflects the index
    backing the chosen key (False for override / first-column fallback).
    """
    if override:
        return override, False

    fq = f"{schema}.{table}"
    # Clustered index leading column (index_id = 1), plus its uniqueness.
    clustered_sql = """
    SELECT TOP (1) c.name, i.is_unique
    FROM sys.indexes i
    JOIN sys.index_columns ic
      ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c
      ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.object_id = OBJECT_ID(?) AND i.index_id = 1 AND ic.key_ordinal = 1
    """
    cursor = source_conn.cursor()
    try:
        cursor.execute(clustered_sql, fq)
        rows = cursor.fetchall()
        if rows:
            return rows[0][0], bool(rows[0][1])

        pk_sql = """
        SELECT TOP (1) c.name, i.is_unique
        FROM sys.indexes i
        JOIN sys.index_columns ic
          ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c
          ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1 AND ic.key_ordinal = 1
        """
        cursor.execute(pk_sql, fq)
        rows = cursor.fetchall()
        if rows:
            return rows[0][0], bool(rows[0][1])

        first_col_sql = """
        SELECT TOP (1) COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(first_col_sql, schema, table)
        rows = cursor.fetchall()
        if rows:
            return rows[0][0], False
    finally:
        cursor.close()

    raise ValueError(f"Could not determine an ordering key for {fq}")


def _plan_range_windows(source_conn, schema, table, ordering_key, workers, base_where):
    """Cheap full-load window plan: split the [MIN, MAX] integer-key range into
    ``workers`` equal sub-ranges via two index seeks (MIN/MAX) — no full-table
    sort, unlike NTILE. Returns ``None`` if the key isn't integer-typed, so the
    caller can fall back to NTILE; returns a single unbounded range for an empty
    table. Assumes a roughly-dense identity-style key (good enough balance for
    parallel extraction; the last window is unbounded so all rows are covered)."""
    cursor = source_conn.cursor()
    try:
        cursor.execute(
            f"SELECT MIN([{ordering_key}]) AS lo, MAX([{ordering_key}]) AS hi "
            f"FROM [{schema}].[{table}]{_where(base_where, None)}"
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
    if not rows or rows[0][0] is None:
        return _mark_first([KeysetRange(lo=None, hi=None, is_last=True)])
    lo_all, hi_all = rows[0][0], rows[0][1]
    if isinstance(lo_all, bool) or not isinstance(lo_all, int) or not isinstance(hi_all, int):
        return None  # non-integer key -> caller falls back to NTILE
    if workers <= 1 or hi_all <= lo_all:
        return _mark_first([KeysetRange(lo=lo_all, hi=None, is_last=True)])
    span = hi_all - lo_all + 1
    width = -(-span // workers)  # ceil division
    ranges: list[KeysetRange] = []
    start = lo_all
    while start <= hi_all:
        nxt = start + width
        is_last = nxt > hi_all
        ranges.append(KeysetRange(lo=start, hi=(None if is_last else nxt), is_last=is_last))
        start = nxt
    return _mark_first(ranges)


def plan_keyset_windows(
    source_conn,
    schema: str,
    table: str,
    ordering_key: str,
    workers: int,
    row_limit: int | None,
    base_where: str | None = None,
) -> list[KeysetRange]:
    """Compute balanced key ranges via NTILE over the (optionally capped) key.

    Inner ranges are half-open [lo_i, lo_{i+1}). The last range is unbounded
    above (hi=None) for a full load, but when ``row_limit`` is set it is capped
    at the max key within the TOP(row_limit) set — otherwise the unbounded last
    window would stream the ENTIRE table regardless of the requested subset.

    When ``base_where`` is provided the NTILE subquery is filtered to the same
    row space as the workers, so windows are balanced over the filtered set.
    """
    workers = max(1, workers)
    # Full load: prefer the cheap MIN/MAX range split (avoids NTILE's full-table
    # sort, which costs minutes on the largest tables). Falls back to NTILE when
    # the key isn't integer-typed.
    if not row_limit:
        range_split = _plan_range_windows(
            source_conn, schema, table, ordering_key, workers, base_where
        )
        if range_split is not None:
            return range_split
    top_clause = f"TOP ({row_limit}) " if row_limit else ""
    # The inner ORDER BY is REQUIRED with TOP (to take the first N keys) but is
    # ILLEGAL in a derived table without TOP/OFFSET (SQL Server rejects it). On a
    # full load (no row_limit) NTILE's own OVER (ORDER BY key) handles bucketing,
    # so the inner ORDER BY is both redundant and must be omitted.
    inner_order = f"\n            ORDER BY [{ordering_key}]" if top_clause else ""
    ntile_sql = f"""
    WITH ranked AS (
        SELECT [{ordering_key}] AS k,
               NTILE({workers}) OVER (ORDER BY [{ordering_key}]) AS bucket
        FROM (
            SELECT {top_clause}[{ordering_key}]
            FROM [{schema}].[{table}]{_where(base_where, None)}{inner_order}
        ) AS sub
    )
    SELECT bucket, MIN(k) AS lo, MAX(k) AS hi
    FROM ranked
    GROUP BY bucket
    ORDER BY bucket
    """
    cursor = source_conn.cursor()
    try:
        cursor.execute(ntile_sql)
        rows = cursor.fetchall()
    finally:
        cursor.close()

    los = [row[1] for row in rows]
    his = [row[2] for row in rows]
    if not los:
        return _mark_first([KeysetRange(lo=None, hi=None, is_last=True)])

    ranges: list[KeysetRange] = []
    for idx, lo in enumerate(los):
        is_last = idx == len(los) - 1
        if is_last:
            # Cap the tail at the subset's max key; leave unbounded for full load.
            hi = his[idx] if row_limit else None
        else:
            hi = los[idx + 1]
        ranges.append(KeysetRange(lo=lo, hi=hi, is_last=is_last))
    return _mark_first(ranges)


_CONN_REFRESH_INTERVAL = 100

# Spawn-child import bootstrap (the proven mechanism FHIR uses). This is
# LOAD-BEARING: calling configure_spawn_import_paths_for_file(__file__) mutates
# the PARENT's live sys.path, and run_window_workers passes the returned hook to
# the runner as before_parallel_start — so the runner re-applies it right before
# spawning. On spawn, CPython snapshots the parent's sys.path into the child's
# prep-data and installs it (multiprocessing spawn `prepare()`) BEFORE the child
# unpickles the pool args — which forces the import of this module and its DSI.*/
# core.* deps. Without it, that import fails in the Nomad code-server image and
# the child dies during bootstrap as BrokenProcessPool "terminated abruptly" (the
# real ModuleNotFoundError never reaches the parent). NO per-child initializer is
# needed (it would run too late, after the worker module is already imported).
_SPAWN_IMPORT_PATHS = configure_spawn_import_paths_for_file(__file__)


_EXTRA_COLUMNS_DEF = [
    ("SourceSystem", "NVARCHAR(50)", False),
    ("dw_load_timestamp", "DATETIME2", False),
    ("dw_partition_date", "DATE", True),
]


@dataclass
class WindowResult:
    rows: int
    t_fetch_s: float
    t_prep_s: float
    t_insert_s: float
    retries: int
    splits: int
    truncations: int


class _CountingContext:
    """Wraps a logger to count retry/split log lines per worker.

    ``log`` is any object exposing ``info``/``warning``/``error`` — the Dagster
    context's logger in thread mode, or a lightweight process-side logger in
    process mode. Exposes ``.log = self`` so the bulk-copy primitives, which
    call ``context.log.info(...)``, work unchanged.
    """

    def __init__(self, log):
        self._log = log
        self.retries = 0
        self.splits = 0
        self.log = self

    def info(self, msg: str, *a, **k) -> None:
        if "Splitting into" in msg:
            self.splits += 1
        if "(refresh," in msg:
            # Timeout-driven fresh-conn recovery — bulk_copy logs label
            # "(refresh, splitting)" / "(refresh, at split floor)". Excludes the
            # periodic "chunk N refresh" (different label, not a retry).
            self.retries += 1
        self._log.info(msg)

    def warning(self, msg: str, *a, **k) -> None:
        self._log.warning(msg)

    def error(self, msg: str, *a, **k) -> None:
        self._log.error(msg)


def _bcp_target_config(target_table: str) -> TableConfig:
    """Minimal TableConfig the bulk-copy primitives need — they read only
    ``.target_table`` (the ext.<target> bulk-copy destination)."""
    return TableConfig(
        output_name=target_table,
        source_schema="dbo",
        source_table=target_table,
        target_table=target_table,
        description=f"parallel extract -> ext.{target_table}",
    )


def _extract_window(
    *,
    log,
    source_conn,
    target_conn,
    target_table: str,
    column_names: list[str],
    select_sql: str,
    chunk_size: int,
    table_lock: bool,
    load_timestamp: datetime,
    partition_date: date | None,
    source_system: str,
    target_resource,
    bcp_limits: list | None = None,
    window_label: str = "",
) -> WindowResult:
    """Stream one keyset window into the shared heap via the shared bulk-copy pump.

    ``log`` is a logger (Dagster context logger or process-side logger);
    ``target_resource`` (or None) lets the pump refresh its bulk-copy connection
    every _CONN_REFRESH_INTERVAL chunks. Retries/splits are tallied by wrapping
    ``log`` in a _CountingContext. Called by the spawn process worker.
    """
    counting = _CountingContext(log)
    config = _bcp_target_config(target_table)
    if bcp_limits is None:
        bcp_limits = [None] * len(column_names)

    source_cursor = source_conn.cursor()
    source_cursor.execute(select_sql)
    try:
        stats = stream_bulkcopy_chunks(
            context=counting,
            source_cursor=source_cursor,
            bc_conn=target_conn,
            target_resource=target_resource,
            config=config,
            column_names=column_names,
            bcp_limits=bcp_limits,
            chunk_size=chunk_size,
            source_system=source_system,
            load_timestamp=load_timestamp,
            partition_date=partition_date,
            table_lock=table_lock,
            refresh_interval=_CONN_REFRESH_INTERVAL,
            label=window_label,
        )
    finally:
        source_cursor.close()

    return WindowResult(
        rows=stats.rows,
        t_fetch_s=stats.t_fetch,
        t_prep_s=stats.t_prep,
        t_insert_s=stats.t_insert,
        retries=counting.retries,
        splits=counting.splits,
        truncations=stats.truncations,
    )


def _create_scratch_heap(context, target_conn, target_table, columns_info) -> None:
    # A HEAP (no clustered index) is load-bearing for the parallel write: all N
    # workers bulk-copy into this same table with TABLOCK, and on a heap TABLOCK
    # takes a Bulk-Update lock that is compatible across concurrent bulk loads
    # (SQL Server's documented parallel-import pattern). Add a clustered index
    # here and concurrent TABLOCK would serialize the workers (or deadlock).
    cursor = target_conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS [ext].[{target_table}]")
        target_conn.commit()
        ddl = create_ext_table_ddl(
            target_table,
            columns_info,
            _EXTRA_COLUMNS_DEF,
            [None] * len(columns_info),
            avoid_max_types=True,
        )
        cursor.execute(ddl)
        target_conn.commit()
        context.log.info(f"Created scratch heap ext.{target_table}")
    finally:
        cursor.close()


def _drop_scratch_heap(context, target_conn, target_table) -> None:
    try:
        cursor = target_conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS [ext].[{target_table}]")
            target_conn.commit()
        finally:
            cursor.close()
    except Exception as exc:  # best-effort cleanup
        context.log.warning(f"Failed to drop scratch heap ext.{target_table}: {exc}")


# ---------------------------------------------------------------------------
# Process-mode plumbing. Threads share one GIL, so the per-row Python prep on a
# wide table can't run in parallel; process workers each get their own GIL and
# their own connections, so fetch I/O AND prep parallelize across cores.
# ---------------------------------------------------------------------------


class _ProcessLogger:
    """Logger for process workers: prints to stdout/stderr.

    A spawned worker can't reach the Dagster context logger, but Dagster
    captures each step subprocess's stdout/stderr (the run's "captured_logs" /
    stdout+stderr tabs), so printing surfaces worker progress there. The wrapping
    _CountingContext still sees these strings to tally retries/splits.
    """

    def info(self, msg="", *a, **k) -> None:
        print(msg, flush=True)

    def warning(self, msg="", *a, **k) -> None:
        print(msg, file=sys.stderr, flush=True)

    def error(self, msg="", *a, **k) -> None:
        print(msg, file=sys.stderr, flush=True)


class _QueueLogger:
    """Worker-side logger that ships (level, message) to the parent via a queue.

    Spawn grandchildren can't reach Dagster's per-step log capture, so the parent
    step process drains this queue and re-emits each line through context.log —
    the only logger Dagster actually captures. Best-effort: never raises into the
    extract loop."""

    def __init__(self, q):
        self._q = q

    def _put(self, level: str, msg) -> None:
        try:
            self._q.put((level, str(msg)))
        except Exception:
            pass

    def info(self, msg="", *a, **k) -> None:
        self._put("info", msg)

    def warning(self, msg="", *a, **k) -> None:
        self._put("warning", msg)

    def error(self, msg="", *a, **k) -> None:
        self._put("error", msg)


class _ConnStrResource:
    """Resource-like reconnect helper: ``get_connection()`` opens a fresh
    mssql-python connection from a connection string, so a process worker can
    refresh its bulk-copy connection exactly like the thread path does with a
    real Dagster resource."""

    def __init__(self, conn_str: str):
        self._conn_str = conn_str

    @contextmanager
    def get_connection(self):
        conn = _connect_mssql_python(self._conn_str, timeout=6000)
        try:
            yield conn
        finally:
            conn.close()


def _resource_to_conn_str(resource) -> str:
    """Picklable mssql-python connection string from an MSSQL resource (the
    shared builder resolves a Dagster-secret password to a plain value)."""
    return build_mssql_conn_str(
        resource.host, resource.port, resource.database, resource.user, resource.password
    )


def build_window_task(
    *,
    source_conn_str,
    target_conn_str,
    select_sql,
    target_table,
    column_names,
    bcp_limits,
    chunk_size,
    table_lock,
    load_timestamp,
    source_system,
    source_key,
    source_capacity,
    window_label="",
    progress_q=None,
) -> dict:
    """Picklable task dict for one window worker (incl. the source-semaphore identity).

    ``progress_q`` (a Manager queue proxy, or None) lets the worker stream
    progress back to the parent for re-logging via Dagster's context logger.
    """
    return {
        "source_conn_str": source_conn_str,
        "target_conn_str": target_conn_str,
        "select_sql": select_sql,
        "target_table": target_table,
        "column_names": column_names,
        "bcp_limits": bcp_limits,
        "chunk_size": chunk_size,
        "table_lock": table_lock,
        "load_timestamp": load_timestamp,
        "source_system": source_system,
        "source_key": source_key,
        "source_capacity": source_capacity,
        "window_label": window_label,
        "progress_q": progress_q,
    }


def _window_process_worker(task: dict) -> dict:
    """Top-level (picklable) process worker: opens its own source + target
    connections and extracts one window via the shared _extract_window loop.

    The SOURCE connection (and the whole window's work) is gated behind a
    cross-process source-connection semaphore slot so concurrent workers across
    unrelated processes never exceed the source's connection budget.

    Returns a picklable dict incl. this child's peak RSS so the parent can sum
    per-process footprints (the parent's RSS sampler can't see child memory).
    """
    from core.parallel.source_semaphore import SourceConnectionSemaphore

    sem = SourceConnectionSemaphore(task["source_key"], task["source_capacity"])
    with sem.slot(timeout=3600.0):
        source_conn = _connect_mssql_python(task["source_conn_str"], timeout=6000)
        target_conn = _connect_mssql_python(task["target_conn_str"], timeout=6000)
        progress_q = task.get("progress_q")
        worker_log = _QueueLogger(progress_q) if progress_q is not None else _ProcessLogger()
        try:
            with PeakRssSampler() as sampler:
                wr = _extract_window(
                    log=worker_log,
                    source_conn=source_conn,
                    target_conn=target_conn,
                    target_table=task["target_table"],
                    column_names=task["column_names"],
                    select_sql=task["select_sql"],
                    chunk_size=task["chunk_size"],
                    table_lock=task["table_lock"],
                    load_timestamp=task["load_timestamp"],
                    partition_date=None,
                    source_system=task["source_system"],
                    target_resource=_ConnStrResource(task["target_conn_str"]),
                    bcp_limits=task["bcp_limits"],
                    window_label=task.get("window_label", ""),
                )
            peak = sampler.peak_mb
        finally:
            for conn in (source_conn, target_conn):
                try:
                    conn.close()
                except Exception:
                    pass
    return {
        "rows": wr.rows,
        "t_fetch_s": wr.t_fetch_s,
        "t_prep_s": wr.t_prep_s,
        "t_insert_s": wr.t_insert_s,
        "retries": wr.retries,
        "splits": wr.splits,
        "truncations": wr.truncations,
        "peak_rss_mb": peak,
    }


# ---------------------------------------------------------------------------
# Production entry point
# ---------------------------------------------------------------------------


def _drain_progress(progress_q, stop: threading.Event, log) -> None:
    """Re-emit worker progress (level, msg) through the parent's Dagster logger.

    Runs in a background thread in the step process while the pool works; after
    ``stop`` is set it flushes any remaining messages. ``getattr`` keeps it safe
    against unexpected levels."""

    def _emit(level, msg):
        if log is not None:
            getattr(log, level, log.info)(msg)

    while not stop.is_set():
        try:
            level, msg = progress_q.get(timeout=0.3)
        except queue.Empty:
            continue
        _emit(level, msg)
    while True:  # final flush
        try:
            level, msg = progress_q.get_nowait()
        except queue.Empty:
            break
        _emit(level, msg)


def run_window_workers(
    *,
    source_resource,
    target_resource,
    select_sqls,
    workers,
    target_table,
    column_names,
    bcp_limits,
    chunk_size,
    table_lock,
    load_timestamp,
    source_system,
    source_key,
    source_capacity,
    base_where=None,
    log=None,
):
    """Run windowed SELECTs as spawn process workers; return (results, summed peak RSS).

    Routes through the shared ``core.parallel`` runner (run_parallel_stage) in
    spawn mode — it handles the spawn context, the parent-side import priming
    (before_parallel_start), standardized logging, and a sequential fallback if
    the pool can't start. ``log`` also receives live per-window progress that the
    spawn workers stream back via a Manager queue (spawn-grandchild stdout is NOT
    captured by Dagster, so the queue is the only path to the UI).
    """
    src_cs = _resource_to_conn_str(source_resource)
    tgt_cs = _resource_to_conn_str(target_resource)
    mp_ctx = multiprocessing.get_context("spawn")
    manager = mp_ctx.Manager()
    progress_q = manager.Queue()
    tasks = [
        build_window_task(
            source_conn_str=src_cs,
            target_conn_str=tgt_cs,
            select_sql=sql,
            target_table=target_table,
            column_names=column_names,
            bcp_limits=bcp_limits,
            chunk_size=chunk_size,
            table_lock=table_lock,
            load_timestamp=load_timestamp,
            source_system=source_system,
            source_key=source_key,
            source_capacity=source_capacity,
            window_label=f"w{i}",
            progress_q=progress_q,
        )
        for i, sql in enumerate(select_sqls)
    ]
    stage = ParallelStage(
        stage_id=f"ext.parallel_extract.{source_system.lower()}",
        policy=ParallelPolicy(
            max_workers=workers,
            parallel_threshold=1,
            chunksize=1,
            executor_kind="process",
        ),
        mp_context=mp_ctx,
        before_parallel_start=_SPAWN_IMPORT_PATHS,
    )
    runner_log = log if log is not None else logging.getLogger(__name__)
    stop = threading.Event()
    drainer = threading.Thread(target=_drain_progress, args=(progress_q, stop, log), daemon=True)
    drainer.start()
    try:
        raw, _summary = run_parallel_stage(
            stage,
            tasks=tasks,
            worker_fn=_window_process_worker,
            logger=runner_log,
        )
    except BaseException as exc:
        if isinstance(exc, KeyboardInterrupt | SystemExit):
            runner_log.warning(
                "[parallel] %s: run cancelled — child workers terminated", target_table
            )
        raise
    finally:
        stop.set()
        drainer.join()  # drains promptly once stopped; join fully before manager.shutdown
        try:
            manager.shutdown()
        except Exception:
            pass
    raw = raw or []
    results = [
        WindowResult(
            r["rows"],
            r["t_fetch_s"],
            r["t_prep_s"],
            r["t_insert_s"],
            r["retries"],
            r["splits"],
            r["truncations"],
        )
        for r in raw
    ]
    return results, sum(r["peak_rss_mb"] for r in raw)


def parallel_stream_extract_and_insert(
    context, source_resource, target_resource, config, source_system
) -> TableLoadResult:
    """Production entry point: windowed parallel extract of one full-load table into ext."""
    workers = clamp_worker_count(config)
    source_key = source_system.lower()
    capacity = source_budget(source_system)
    is_dependent = config.table_type in (TABLE_TYPE_DEPENDENT, TABLE_TYPE_BRIDGE)

    with source_resource.get_connection() as setup_src:
        columns_info, column_names = get_table_columns_info(setup_src, config)
        if config.exclude_columns:
            # Honor the table contract's dropped columns (the serial path does
            # this via _compute_filtered_indices). The parallel path builds an
            # explicit column list, so dropping them from column_names/columns_info
            # excludes them from the SELECT, the scratch-heap DDL, and bcp_limits.
            excluded = {c.lower() for c in config.exclude_columns}
            kept = [
                (ci, name)
                for ci, name in zip(columns_info, column_names, strict=True)
                if name.lower() not in excluded
            ]
            columns_info = [ci for ci, _ in kept]
            column_names = [name for _, name in kept]
        if source_system == "ILS":
            column_names = [n.replace("__", "##") for n in column_names]
        # For dependent/bridge FULL-LOAD tables, build_relation_where_clause
        # returns config.custom_where_clause (or None) — a composable predicate
        # that ANDs cleanly with the keyset window and ORDER BY. (The
        # parent-partition FK subquery is only produced for incremental runs,
        # which never reach this parallel full-load entry point.)
        base_where = (
            build_relation_where_clause(config, "", source_system, False, False)
            if is_dependent
            else None
        )
        ordering_key, _unique = detect_ordering_key(
            setup_src, config.source_schema, config.source_table, None
        )
        keyset_ranges = plan_keyset_windows(
            setup_src,
            config.source_schema,
            config.source_table,
            ordering_key,
            workers,
            None,
            base_where=base_where,
        )

    with target_resource.get_connection() as setup_tgt:
        _create_scratch_heap(context, setup_tgt, config.target_table, columns_info)

    chunk_size = compute_streaming_chunk_size(columns_info)[0]
    # Honor a per-table bulk-copy chunk cap (same as the serial path) — e.g. the
    # wide mysupply tables cap at 5k to dodge the mssql-python bulkcopy wedge.
    if config.bcp_max_chunk_size is not None and chunk_size > config.bcp_max_chunk_size:
        chunk_size = config.bcp_max_chunk_size
    bcp_limits = build_bcp_truncation_limits(columns_info)
    load_timestamp = datetime.utcnow().replace(microsecond=0)
    select_sqls = [
        build_keyset_select(
            column_names,
            config.source_schema,
            config.source_table,
            ordering_key,
            rng,
            base_where=base_where,
        )
        for rng in keyset_ranges
    ]
    try:
        results, peak = run_window_workers(
            source_resource=source_resource,
            target_resource=target_resource,
            select_sqls=select_sqls,
            workers=workers,
            target_table=config.target_table,
            column_names=column_names,
            bcp_limits=bcp_limits,
            chunk_size=chunk_size,
            table_lock=True,
            load_timestamp=load_timestamp,
            source_system=source_system,
            source_key=source_key,
            source_capacity=capacity,
            base_where=base_where,
            log=context.log,
        )
    except Exception:
        # A worker failed mid-load, leaving a partially-populated ext.<target>
        # heap. Drop it so a re-run from failure doesn't read half-loaded data.
        with target_resource.get_connection() as cleanup_tgt:
            _drop_scratch_heap(context, cleanup_tgt, config.target_table)
        raise
    total_rows = sum(r.rows for r in results)
    context.log.info(
        f"[parallel] {config.target_table} w={workers} ({source_system}): "
        f"{total_rows:,} rows, peak {peak:.0f} MB"
    )
    return TableLoadResult(
        source_table=f"{config.source_schema}.{config.source_table}",
        target_table=f"ext.{config.target_table}",
        row_count=total_rows,
    )
