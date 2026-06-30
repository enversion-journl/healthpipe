# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the shared DSI extraction core factory.

Tests the generic create_extraction_assets() function that both
IFDB and ERP domain adapters delegate to.
"""

from dataclasses import dataclass
from typing import Any

import dagster as dg
from DSI.connectors.factory import create_extraction_assets
from DSI.connectors.protocol import ExtractionConfig
from DSI.connectors.types import PartitionType, TableType


@dataclass(frozen=True)
class _MockConfig:
    description: str
    partition_column: str | None


daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01", end_offset=1)
monthly_partitions = dg.MonthlyPartitionsDefinition(start_date="2024-01", end_offset=1, fmt="%Y-%m")
full_load_partitions = dg.StaticPartitionsDefinition(["full_load"])


def _mock_builder(asset_kwargs: dict[str, Any], partition_type: str):
    """Minimal asset builder for testing — creates a no-op @dg.asset."""

    @dg.asset(**asset_kwargs)
    def _asset_fn(context: dg.AssetExecutionContext) -> None:
        pass

    return _asset_fn


class TestMockConfigSatisfiesProtocol:
    def test_mock_config_is_extraction_config(self):
        config = _MockConfig(description="test", partition_column="col")
        assert isinstance(config, ExtractionConfig)


class TestCreateExtractionAssetsBasic:
    """Test the factory with default settings."""

    def test_returns_three_assets_when_all_enabled(self):
        config = _MockConfig(description="Test entity", partition_column="mod_date")
        assets = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=True,
            include_full_load=True,
            partition_defs={
                "daily": daily_partitions,
                "monthly": monthly_partitions,
                "full_load": full_load_partitions,
            },
        )
        assert len(assets) == 3

    def test_returns_only_enabled_assets(self):
        config = _MockConfig(description="Test", partition_column=None)
        assets = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=False,
            include_full_load=False,
        )
        assert len(assets) == 1

    def test_returns_empty_when_none_enabled(self):
        config = _MockConfig(description="Test", partition_column=None)
        assets = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=False,
            include_monthly=False,
            include_full_load=False,
        )
        assert len(assets) == 0


class TestAssetKeyGeneration:
    """Test asset key naming with and without prefix."""

    def test_flat_keys_without_prefix(self):
        config = _MockConfig(description="Test", partition_column=None)
        (asset,) = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=False,
            include_full_load=False,
            asset_key_prefix=None,
        )
        keys = list(dg.Definitions(assets=[asset]).get_all_asset_specs())
        assert any(spec.key == dg.AssetKey("widget_daily") for spec in keys)

    def test_nested_keys_with_prefix(self):
        config = _MockConfig(description="Test", partition_column=None)
        (asset,) = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=False,
            include_full_load=False,
            asset_key_prefix=["my_domain"],
        )
        keys = list(dg.Definitions(assets=[asset]).get_all_asset_specs())
        assert any(spec.key == dg.AssetKey(["my_domain", "widget_daily"]) for spec in keys)

    def test_no_suffix_when_append_partition_suffix_false(self):
        config = _MockConfig(description="Test", partition_column=None)
        (asset,) = create_extraction_assets(
            config=config,
            entity_name="ext_table_daily",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=False,
            include_full_load=False,
            append_partition_suffix=False,
        )
        keys = list(dg.Definitions(assets=[asset]).get_all_asset_specs())
        assert any(spec.key == dg.AssetKey("ext_table_daily") for spec in keys)


class TestGroupNameGeneration:
    """Test group naming with prefix and suffix overrides."""

    def test_default_group_suffix(self):
        config = _MockConfig(description="Test", partition_column=None)
        assets = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=True,
            include_full_load=True,
            group_prefix="my_extract",
            partition_defs={
                "daily": daily_partitions,
                "monthly": monthly_partitions,
                "full_load": full_load_partitions,
            },
        )
        specs = []
        for asset in assets:
            specs.extend(dg.Definitions(assets=[asset]).get_all_asset_specs())
        group_names = {s.group_name for s in specs}
        assert group_names == {"my_extract_daily", "my_extract_monthly", "my_extract_full_load"}

    def test_custom_group_suffix_override(self):
        config = _MockConfig(description="Test", partition_column=None)
        (asset,) = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=False,
            include_monthly=False,
            include_full_load=True,
            group_prefix="erp_extract",
            group_suffixes={"full_load": ""},
        )
        specs = list(dg.Definitions(assets=[asset]).get_all_asset_specs())
        assert specs[0].group_name == "erp_extract"


class TestTagsMerging:
    """Test that base tags are merged with partition-specific tags."""

    def test_partition_tag_added(self):
        config = _MockConfig(description="Test", partition_column=None)
        (asset,) = create_extraction_assets(
            config=config,
            entity_name="widget",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=False,
            include_full_load=False,
            tags={"layer": "extract", "source": "test"},
        )
        specs = list(dg.Definitions(assets=[asset]).get_all_asset_specs())
        assert specs[0].tags["partition"] == "daily"
        assert specs[0].tags["layer"] == "extract"
        assert specs[0].tags["source"] == "test"


class TestFunctionNaming:
    """Test that generated asset functions have correct __name__."""

    def test_function_name_with_suffix(self):
        config = _MockConfig(description="Test", partition_column=None)
        (asset,) = create_extraction_assets(
            config=config,
            entity_name="patient",
            asset_builder=_mock_builder,
            include_daily=True,
            include_monthly=False,
            include_full_load=False,
        )
        assert asset.__name__ == "extract_patient_daily"

    def test_function_name_without_suffix(self):
        config = _MockConfig(description="Test", partition_column=None)
        (asset,) = create_extraction_assets(
            config=config,
            entity_name="ext_table_monthly",
            asset_builder=_mock_builder,
            include_daily=False,
            include_monthly=True,
            include_full_load=False,
            append_partition_suffix=False,
        )
        assert asset.__name__ == "extract_ext_table_monthly"


class TestPartitionTypes:
    """Test PartitionType enum values."""

    def test_partition_type_values(self):
        assert PartitionType.DAILY == "daily"
        assert PartitionType.MONTHLY == "monthly"
        assert PartitionType.FULL_LOAD == "full_load"


class TestTableTypeEnum:
    """Test TableType enum values."""

    def test_table_type_values(self):
        assert TableType.MASTER_DATA == "master_data"
        assert TableType.MASTER_DATA_PARTITIONED == "master_data_partitioned"
        assert TableType.TRANSACTIONAL == "transactional"
        assert TableType.DEPENDENT == "dependent"
        assert TableType.BRIDGE == "bridge"
