# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import dagster as dg
from core.extract import ExtractBatchingPolicy
from core.io_managers import BatchAwareFilesystemIOManager
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig


class _FakeLog:
    def info(self, *args) -> None:
        pass

    def debug(self, *args) -> None:
        pass


class _FakeCursor:
    description = [
        ("ID", None, None, None, None, None),
        ("VALUE", None, None, None, None, None),
    ]

    def __init__(self) -> None:
        self._batches = [[(1, "a")], [(2, "b")]]
        self.fetchmany_sizes: list[int] = []
        self.executed_query: str | None = None
        self.close_count = 0

    def execute(self, query: str) -> None:
        self.executed_query = query

    def fetchmany(self, size: int):
        self.fetchmany_sizes.append(size)
        if not self._batches:
            return []
        return self._batches.pop(0)

    def close(self) -> None:
        self.close_count += 1


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


class _FakeOracleResource:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    @contextmanager
    def get_connection(self):
        yield _FakeConnection(self._cursor)


def _config(*, batching: ExtractBatchingPolicy | None) -> TableExtractionConfig:
    return TableExtractionConfig(
        table="IFDB.TEST_TABLE",
        columns=["ID", "VALUE"],
        partition_column="OPDATERET_TID",
        description="Test table",
        batching=batching,
    )


def _output_manager_key(asset) -> str:
    return asset.op.output_defs[0].io_manager_key


def _custom_builder(asset_kwargs: dict[str, Any], _partition_type: str):
    @dg.asset(**asset_kwargs)
    def _asset_fn() -> dict[str, Any]:
        return {"custom": True}

    return _asset_fn


def test_default_builder_uses_batch_io_manager_when_config_opts_in():
    (asset,) = create_extraction_assets(
        entity_name="test_table",
        config=_config(
            batching=ExtractBatchingPolicy(enabled=True, fetch_size=2, env_prefix="TEST")
        ),
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )

    assert _output_manager_key(asset) == "batched_extract_io_manager"


def test_default_builder_keeps_legacy_io_manager_without_batching():
    (asset,) = create_extraction_assets(
        entity_name="test_table",
        config=_config(batching=None),
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )

    assert _output_manager_key(asset) == "io_manager"


def test_custom_builder_keeps_legacy_io_manager_even_when_config_has_batching():
    (asset,) = create_extraction_assets(
        entity_name="test_table",
        config=_config(
            batching=ExtractBatchingPolicy(enabled=True, fetch_size=2, env_prefix="TEST")
        ),
        custom_builder=_custom_builder,
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )

    assert _output_manager_key(asset) == "io_manager"


def test_batched_asset_materializes_with_fake_oracle_resource(tmp_path):
    cursor = _FakeCursor()
    (asset,) = create_extraction_assets(
        entity_name="test_table",
        config=_config(
            batching=ExtractBatchingPolicy(enabled=True, fetch_size=1, env_prefix="TEST")
        ),
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )

    result = dg.materialize(
        [asset],
        partition_key="2025-04",
        resources={
            "ifdb_oracle": _FakeOracleResource(cursor),
            "batched_extract_io_manager": BatchAwareFilesystemIOManager(
                base_dir=str(tmp_path / "storage")
            ),
        },
    )

    assert result.success
    assert cursor.fetchmany_sizes == [1, 1, 1]
    assert cursor.close_count == 1
    assert "SELECT ID, VALUE FROM IFDB.TEST_TABLE" in cursor.executed_query


def test_batched_asset_can_be_disabled_by_env_and_use_legacy_list_path(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("TEST_BATCHED", "false")
    cursor = _FakeCursor()
    (asset,) = create_extraction_assets(
        entity_name="test_table",
        config=_config(
            batching=ExtractBatchingPolicy(enabled=True, fetch_size=1, env_prefix="TEST")
        ),
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )

    result = dg.materialize(
        [asset],
        partition_key="2025-04",
        resources={
            "ifdb_oracle": _FakeOracleResource(cursor),
            "batched_extract_io_manager": BatchAwareFilesystemIOManager(
                base_dir=str(tmp_path / "storage")
            ),
        },
    )

    assert result.success
    assert cursor.fetchmany_sizes == [2000, 2000, 2000]
    assert cursor.close_count == 1


def test_sidecar_opt_in_skips_daily_partition_outputs(tmp_path):
    cursor = _FakeCursor()
    (asset,) = create_extraction_assets(
        entity_name="test_table",
        config=_config(
            batching=ExtractBatchingPolicy(
                enabled=True,
                fetch_size=1,
                env_prefix="TEST",
                write_batch_sidecars=True,
            )
        ),
        include_daily=True,
        include_monthly=False,
        include_full_load=False,
    )

    result = dg.materialize(
        [asset],
        partition_key="2025-04-01",
        resources={
            "ifdb_oracle": _FakeOracleResource(cursor),
            "batched_extract_io_manager": BatchAwareFilesystemIOManager(
                base_dir=str(tmp_path / "storage")
            ),
        },
    )

    assert result.success
    assert list((tmp_path / "storage").rglob("*.batches")) == []


def test_sidecar_opt_in_writes_monthly_partition_outputs(tmp_path):
    cursor = _FakeCursor()
    (asset,) = create_extraction_assets(
        entity_name="test_table",
        config=_config(
            batching=ExtractBatchingPolicy(
                enabled=True,
                fetch_size=1,
                env_prefix="TEST",
                write_batch_sidecars=True,
            )
        ),
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )

    result = dg.materialize(
        [asset],
        partition_key="2025-04",
        resources={
            "ifdb_oracle": _FakeOracleResource(cursor),
            "batched_extract_io_manager": BatchAwareFilesystemIOManager(
                base_dir=str(tmp_path / "storage")
            ),
        },
    )

    assert result.success
    assert [path.name for path in (tmp_path / "storage").rglob("*.batches")]
