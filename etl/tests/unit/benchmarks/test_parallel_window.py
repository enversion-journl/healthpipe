# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Worker streaming loop with fake connections (parallel_extract engine)."""

from datetime import datetime
from types import SimpleNamespace

import pytest
from DSI.connectors.mssql.parallel_extract import (
    WindowResult,
    _CountingContext,
    _extract_window,
    _resource_to_conn_str,
)


class _RecordingLog:
    def info(self, *a, **k):
        pass

    warning = error = info


@pytest.mark.unit
def test_counting_context_counts_timeout_refreshes_and_splits():
    # `retries` must count timeout-driven fresh-conn recoveries (label
    # "(refresh, ...)") but NOT the periodic chunk refresh, and `splits` must
    # count split events. Previously retries matched the wrong substring on the
    # wrong channel and was always 0.
    cc = _CountingContext(_RecordingLog())
    cc.info("[rows=5,000] (refresh, splitting) Opening fresh bulk-copy connection...")
    cc.info("[rows=2,500] (refresh, at split floor) Opening fresh bulk-copy connection...")
    cc.info("[ext.t] chunk 100 refresh Opening fresh bulk-copy connection...")  # periodic
    cc.info("[rows=5,000] Splitting into 2,500 + 2,500 (depth=1)")
    assert cc.retries == 2
    assert cc.splits == 1


class _FakeSourceCursor:
    """Yields ``chunks`` of rows from fetchmany, then []."""

    def __init__(self, chunks):
        self._chunks = list(chunks)
        self.description = [("c1",), ("c2",)]

    def execute(self, sql, *params):
        self._sql = sql

    def fetchmany(self, n):
        return self._chunks.pop(0) if self._chunks else []

    def close(self):
        pass


class _FakeSourceConn:
    def __init__(self, chunks):
        self._chunks = chunks

    def cursor(self):
        return _FakeSourceCursor(self._chunks)

    def close(self):
        pass


class _FakeTargetCursor:
    def __init__(self, sink):
        self._sink = sink

    def bulkcopy(self, target, data, **kwargs):
        self._sink["inserted"] += len(data)
        self._sink["last_table_lock"] = kwargs.get("table_lock")
        return {"rows_copied": len(data)}

    def close(self):
        pass


class _FakeTargetConn:
    def __init__(self, sink):
        self._sink = sink

    def cursor(self):
        return _FakeTargetCursor(self._sink)

    def close(self):
        pass


def _ctx():
    log = SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None
    )
    return SimpleNamespace(log=log)


@pytest.mark.unit
def test_extract_window_streams_and_inserts_all_rows():
    sink = {"inserted": 0, "last_table_lock": None}
    source_conn = _FakeSourceConn([[(1, "a"), (2, "b")], [(3, "c")]])
    target_conn = _FakeTargetConn(sink)

    result = _extract_window(
        log=_ctx().log,
        source_conn=source_conn,
        target_conn=target_conn,
        target_table="bench_t",
        column_names=["c1", "c2"],
        select_sql="SELECT ...",
        chunk_size=10_000,
        table_lock=False,
        load_timestamp=datetime(2026, 6, 16),
        partition_date=None,
        source_system="MySupply",
        target_resource=None,
    )
    assert isinstance(result, WindowResult)
    assert result.rows == 3
    assert sink["inserted"] == 3
    assert sink["last_table_lock"] is False
    assert result.t_fetch_s >= 0.0 and result.t_insert_s >= 0.0


class _RefreshableTargetConn:
    """Target conn that records its own close, for refresh-lifetime testing."""

    def __init__(self, sink, conn_id):
        self._sink = sink
        self.conn_id = conn_id

    def cursor(self):
        return _FakeTargetCursor(self._sink)

    def close(self):
        self._sink["closed_conns"].append(self.conn_id)


class _CtxMgr:
    """Context manager that closes its connection on __exit__ (mirrors get_connection)."""

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        self._conn.close()
        return False


class _FakeTargetResource:
    """Hands out fresh _RefreshableTargetConn instances, recording opens."""

    def __init__(self, sink):
        self._sink = sink
        self._counter = 0

    def get_connection(self):
        self._counter += 1
        self._sink["opened_conns"].append(self._counter)
        return _CtxMgr(_RefreshableTargetConn(self._sink, self._counter))


@pytest.mark.unit
def test_extract_window_refreshes_connection_every_100_chunks():
    # 101 single-row chunks => exactly one refresh when chunk_num hits 100.
    sink = {"inserted": 0, "last_table_lock": None, "opened_conns": [], "closed_conns": []}
    source_conn = _FakeSourceConn([[(i, "x")] for i in range(101)])
    initial_target = _RefreshableTargetConn(sink, 0)  # the passed-in connection
    resource = _FakeTargetResource(sink)

    result = _extract_window(
        log=_ctx().log,
        source_conn=source_conn,
        target_conn=initial_target,
        target_table="bench_t",
        column_names=["c1", "c2"],
        select_sql="SELECT ...",
        chunk_size=1,
        table_lock=True,
        load_timestamp=datetime(2026, 6, 16),
        partition_date=None,
        source_system="MySupply",
        target_resource=resource,
    )

    assert result.rows == 101
    assert sink["inserted"] == 101
    # Exactly one refresh => one fresh connection drawn from the resource.
    assert sink["opened_conns"] == [1]
    # The original connection was closed by the refresh...
    assert 0 in sink["closed_conns"]
    # ...and the refreshed connection's context was drained in the finally block.
    assert 1 in sink["closed_conns"]


@pytest.mark.unit
def test_resource_to_conn_str_builds_string_and_resolves_secret():
    class _Secret:
        def get_value(self):
            return "s3cret"

    class _Res:
        host = "mysupply.example"
        port = 1433
        database = "mysupply_db"
        user = "reader"
        password = _Secret()

    cs = _resource_to_conn_str(_Res())
    assert "SERVER=mysupply.example,1433" in cs
    assert "DATABASE=mysupply_db" in cs
    assert "UID=reader" in cs
    assert "PWD=s3cret" in cs  # secret resolved via get_value()
    assert "Encrypt=yes;TrustServerCertificate=yes;" in cs

    class _ResPlain:
        host, port, database, user, password = "h", 1433, "d", "u", "plain"

    assert "PWD=plain" in _resource_to_conn_str(_ResPlain())


@pytest.mark.unit
def test_extract_window_emits_throttled_progress_log():
    """First chunk always logs a progress line (with the window label) via the
    provided logger — this is what surfaces in a process worker's stdout."""
    logged: list[str] = []
    log = SimpleNamespace(
        info=lambda m, *a, **k: logged.append(m),
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )
    sink = {"inserted": 0, "last_table_lock": None}
    source_conn = _FakeSourceConn([[(1, "a"), (2, "b")], [(3, "c")]])
    _extract_window(
        log=log,
        source_conn=source_conn,
        target_conn=_FakeTargetConn(sink),
        target_table="bench_t",
        column_names=["c1", "c2"],
        select_sql="SELECT ...",
        chunk_size=10_000,
        table_lock=False,
        load_timestamp=datetime(2026, 6, 16),
        partition_date=None,
        source_system="MySupply",
        target_resource=None,
        window_label="w0",
    )
    progress = [m for m in logged if "chunk 1" in m]
    assert progress, "expected a progress log on the first chunk"
    assert "w0" in progress[0]  # window label included


@pytest.mark.unit
def test_queue_logger_and_drain_reemit_via_parent_log():
    """Worker _QueueLogger -> queue -> _drain_progress -> parent logger, at the
    right level. This bridge gets spawn-worker progress into Dagster's captured
    logs (spawn-grandchild stdout is NOT captured)."""
    import queue as _queue
    import threading

    from DSI.connectors.mssql.parallel_extract import _drain_progress, _QueueLogger

    pq = _queue.Queue()
    wlog = _QueueLogger(pq)
    wlog.info("[w0] chunk 100: 1,000,000 rows")
    wlog.warning("[w0] Splitting into 5,000 + 5,000")

    emitted: list[tuple[str, str]] = []
    parent_log = SimpleNamespace(
        info=lambda m, *a, **k: emitted.append(("info", m)),
        warning=lambda m, *a, **k: emitted.append(("warning", m)),
        error=lambda m, *a, **k: emitted.append(("error", m)),
    )
    stop = threading.Event()
    stop.set()  # skip the loop; exercise the final flush
    _drain_progress(pq, stop, parent_log)

    assert ("info", "[w0] chunk 100: 1,000,000 rows") in emitted
    assert ("warning", "[w0] Splitting into 5,000 + 5,000") in emitted
