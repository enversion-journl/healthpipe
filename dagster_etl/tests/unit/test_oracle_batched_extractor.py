# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle import models as oracle_models
from DSI.connectors.oracle.extractor import OracleExtractor
from DSI.connectors.oracle.models import TableExtractionConfig


class _FakeLog:
    def __init__(self) -> None:
        self.infos: list[tuple] = []
        self.debugs: list[tuple] = []
        self.warnings: list[tuple] = []

    def info(self, *args) -> None:
        self.infos.append(args)

    def debug(self, *args) -> None:
        self.debugs.append(args)

    def warning(self, *args) -> None:
        self.warnings.append(args)


class _FakeRun:
    tags: dict[str, str] = {}


class _FakeContext:
    run = _FakeRun()

    def __init__(self) -> None:
        self.log = _FakeLog()

    def has_tag(self, _name: str) -> bool:
        return False


class _FakeCursor:
    def __init__(self, batches, columns=("ID", "VALUE")) -> None:
        self._batches = list(batches)
        self.description = [(column, None, None, None, None, None) for column in columns]
        self.executed_query: str | None = None
        self.fetchmany_sizes: list[int] = []
        self.close_count = 0
        self.fetchall_called = False

    def execute(self, query: str) -> None:
        self.executed_query = query

    def fetchmany(self, size: int):
        self.fetchmany_sizes.append(size)
        if not self._batches:
            return []
        return self._batches.pop(0)

    def fetchall(self):
        self.fetchall_called = True
        raise AssertionError("batched path must not call fetchall")

    def close(self) -> None:
        self.close_count += 1


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


class _FakeLOB:
    def __init__(self, value) -> None:
        self._value = value

    def read(self):
        return self._value


def _config() -> TableExtractionConfig:
    return TableExtractionConfig(
        table="IFDB.TEST_TABLE",
        columns=["ID", "VALUE"],
        partition_column="OPDATERET_TID",
        description="Test table",
    )


def test_make_row_batch_source_streams_fetchmany_without_fetchall():
    cursor = _FakeCursor(
        [
            [(1, "a"), (2, "b")],
            [(3, "c")],
        ]
    )
    extractor = OracleExtractor(
        _FakeConnection(cursor),
        context=_FakeContext(),
        source_system="IFDB",
    )
    policy = ExtractBatchingPolicy(enabled=True, fetch_size=2, env_prefix="TEST")

    source = extractor.make_row_batch_source(_config(), "2026-04", "monthly", policy)
    batches = list(source.iter_batches())
    source.close()
    source.close()

    assert batches == [
        [{"ID": 1, "VALUE": "a"}, {"ID": 2, "VALUE": "b"}],
        [{"ID": 3, "VALUE": "c"}],
    ]
    assert cursor.fetchall_called is False
    assert cursor.fetchmany_sizes == [2, 2, 2]
    assert cursor.close_count == 1
    assert "SELECT ID, VALUE FROM IFDB.TEST_TABLE" in cursor.executed_query


def test_make_row_batch_source_converts_lob_values_while_cursor_is_open(monkeypatch):
    monkeypatch.setattr(oracle_models.oracledb, "LOB", _FakeLOB)
    cursor = _FakeCursor([[(1, _FakeLOB("<xml />"))]], columns=("ID", "XML"))
    extractor = OracleExtractor(
        _FakeConnection(cursor),
        context=_FakeContext(),
        source_system="IFDB",
    )
    policy = ExtractBatchingPolicy(enabled=True, fetch_size=10, env_prefix="TEST")

    source = extractor.make_row_batch_source(_config(), "2026-04", "monthly", policy)

    assert list(source.iter_batches()) == [[{"ID": 1, "XML": "<xml />"}]]
    assert cursor.close_count == 0
    source.close()
    assert cursor.close_count == 1


def test_legacy_extract_collects_from_same_batched_cursor_path():
    cursor = _FakeCursor([[(1, "a")], [(2, "b")]])
    extractor = OracleExtractor(
        _FakeConnection(cursor),
        context=_FakeContext(),
        source_system="IFDB",
    )

    result = extractor.extract(_config(), "2026-04", "monthly")

    assert result.data == [{"ID": 1, "VALUE": "a"}, {"ID": 2, "VALUE": "b"}]
    assert result.column_names == ["ID", "VALUE"]
    assert result.row_count == 2
    assert cursor.fetchall_called is False
    assert cursor.close_count == 1
