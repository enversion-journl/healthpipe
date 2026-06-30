# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""parallel_stream_extract_and_insert: base WHERE per table type; returns TableLoadResult."""

import DSI.connectors.mssql.parallel_extract as pe
import pytest
from DSI.connectors.mssql.models import TABLE_TYPE_DEPENDENT, TableConfig, TableLoadResult


class _FakeLog:
    def info(self, *a, **k):
        pass

    warning = error = info


class _FakeCtx:
    def __init__(self):
        self.log = _FakeLog()


class _FakeCM:
    def __init__(self, conn):
        self._c = conn

    def __enter__(self):
        return self._c

    def __exit__(self, *a):
        return False


class _FakeConn:
    def cursor(self):
        class _C:
            def execute(self, *a):
                pass

            def fetchall(self):
                return [(0,)]

            def close(self):
                pass

        return _C()


class _FakeResource:
    host, port, database, user, password = "h", 1433, "d", "u", "p"

    def __init__(self, conn):
        self._c = conn

    def get_connection(self):
        return _FakeCM(self._c)


def _common_monkeypatch(monkeypatch, captured):
    monkeypatch.setattr(
        pe,
        "get_table_columns_info",
        lambda conn, cfg: ([("Id", None, None, None, None, None)], ["Id"]),
    )
    monkeypatch.setattr(pe, "create_ext_table_ddl", lambda *a, **k: "CREATE TABLE x (Id INT)")
    monkeypatch.setattr(pe, "detect_ordering_key", lambda *a, **k: ("Id", True))
    monkeypatch.setattr(pe, "plan_keyset_windows", lambda *a, **k: [pe.KeysetRange(0, None, True)])
    monkeypatch.setattr(pe, "compute_streaming_chunk_size", lambda ci: (10_000, 100))
    monkeypatch.setattr(pe, "build_bcp_truncation_limits", lambda ci: [None])
    monkeypatch.setattr(pe, "_create_scratch_heap", lambda *a, **k: None)

    def _fake_run(**kwargs):
        captured["base_where"] = kwargs["base_where"]
        captured["select_sqls"] = kwargs["select_sqls"]
        captured["source_system"] = kwargs["source_system"]
        return ([pe.WindowResult(5, 0, 0, 0, 0, 0, 0)], 123.0)

    monkeypatch.setattr(pe, "run_window_workers", _fake_run)


@pytest.mark.unit
def test_dependent_uses_relation_base_where(monkeypatch):
    captured = {}
    _common_monkeypatch(monkeypatch, captured)
    monkeypatch.setattr(pe, "build_relation_where_clause", lambda *a, **k: "[FK] IN (1,2)")
    cfg = TableConfig(
        output_name="o",
        source_schema="dbo",
        source_table="child",
        target_table="child",
        description="d",
        table_type=TABLE_TYPE_DEPENDENT,
        parallel_workers=4,
    )
    res = pe.parallel_stream_extract_and_insert(
        _FakeCtx(),
        _FakeResource(_FakeConn()),
        _FakeResource(_FakeConn()),
        cfg,
        "MySupply",
    )
    assert isinstance(res, TableLoadResult)
    assert res.row_count == 5
    assert captured["base_where"] == "[FK] IN (1,2)"
    assert "[FK] IN (1,2)" in captured["select_sqls"][0]


@pytest.mark.unit
def test_exclude_columns_dropped_from_parallel_select(monkeypatch):
    # The serial path honors config.exclude_columns; the parallel path must too,
    # or it would load a column the table contract says to drop.
    captured = {}
    _common_monkeypatch(monkeypatch, captured)
    monkeypatch.setattr(
        pe,
        "get_table_columns_info",
        lambda conn, cfg: (
            [("Id", None, None, None, None, None), ("Secret", None, None, None, None, None)],
            ["Id", "Secret"],
        ),
    )
    cfg = TableConfig(
        output_name="o",
        source_schema="dbo",
        source_table="t",
        target_table="t",
        description="d",
        parallel_workers=4,
        exclude_columns=("Secret",),
    )
    pe.parallel_stream_extract_and_insert(
        _FakeCtx(),
        _FakeResource(_FakeConn()),
        _FakeResource(_FakeConn()),
        cfg,
        "MySupply",
    )
    assert "[Id]" in captured["select_sqls"][0]
    assert "Secret" not in captured["select_sqls"][0]


@pytest.mark.unit
def test_failed_worker_drops_partial_scratch_heap(monkeypatch):
    # A worker failure after the scratch heap is (re)created must drop the
    # partially-populated ext.<target> before re-raising, so a re-run from
    # failure doesn't read a half-loaded heap.
    captured = {}
    _common_monkeypatch(monkeypatch, captured)
    dropped = []
    monkeypatch.setattr(pe, "_drop_scratch_heap", lambda ctx, conn, target: dropped.append(target))

    def _boom(**kwargs):
        raise RuntimeError("worker died")

    monkeypatch.setattr(pe, "run_window_workers", _boom)
    cfg = TableConfig(
        output_name="o",
        source_schema="dbo",
        source_table="t",
        target_table="t",
        description="d",
        parallel_workers=4,
    )
    with pytest.raises(RuntimeError, match="worker died"):
        pe.parallel_stream_extract_and_insert(
            _FakeCtx(),
            _FakeResource(_FakeConn()),
            _FakeResource(_FakeConn()),
            cfg,
            "MySupply",
        )
    assert dropped == ["t"]


@pytest.mark.unit
def test_transactional_has_no_base_where(monkeypatch):
    captured = {}
    _common_monkeypatch(monkeypatch, captured)
    cfg = TableConfig(
        output_name="o",
        source_schema="dbo",
        source_table="t",
        target_table="t",
        description="d",
        parallel_workers=4,
    )
    res = pe.parallel_stream_extract_and_insert(
        _FakeCtx(),
        _FakeResource(_FakeConn()),
        _FakeResource(_FakeConn()),
        cfg,
        "MySupply",
    )
    assert res.row_count == 5
    assert captured["base_where"] is None
    assert captured["source_system"] == "MySupply"
