# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""table_lock param threads through bulkcopy_insert and the retry/split path."""

from types import SimpleNamespace

import pytest
from DSI.connectors.mssql.bulk_copy import bulkcopy_insert, bulkcopy_insert_with_retry
from DSI.connectors.mssql.models import TableConfig

_CFG = TableConfig(
    output_name="bench",
    source_schema="dbo",
    source_table="t",
    target_table="bench_t",
    description="bench",
)


class _FakeCursor:
    def __init__(self, sink):
        self._sink = sink

    def bulkcopy(self, target, data, **kwargs):
        self._sink["target"] = target
        self._sink["table_lock"] = kwargs.get("table_lock")
        self._sink["rows"] = len(data)
        return {"rows_copied": len(data)}

    def close(self):
        pass


class _FakeConn:
    def __init__(self, sink):
        self._sink = sink

    def cursor(self):
        return _FakeCursor(self._sink)


def _ctx():
    log = SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None
    )
    return SimpleNamespace(log=log)


@pytest.mark.unit
def test_bulkcopy_insert_defaults_to_table_lock_true():
    sink = {}
    rows = bulkcopy_insert(_ctx(), _FakeConn(sink), _CFG, ["c1"], [(1, "s", None, None)])
    assert rows == 1
    assert sink["table_lock"] is True
    assert sink["target"] == "ext.bench_t"


@pytest.mark.unit
def test_bulkcopy_insert_honors_table_lock_false():
    sink = {}
    bulkcopy_insert(_ctx(), _FakeConn(sink), _CFG, ["c1"], [(1, "s", None, None)], table_lock=False)
    assert sink["table_lock"] is False


@pytest.mark.unit
def test_retry_entry_threads_table_lock_false():
    sink = {}
    rows, _conn = bulkcopy_insert_with_retry(
        _ctx(), _FakeConn(sink), _CFG, ["c1"], [(1, "s", None, None)], table_lock=False
    )
    assert rows == 1
    assert sink["table_lock"] is False
