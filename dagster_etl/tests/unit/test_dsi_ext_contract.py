# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for DSI ext-only extraction contract.

Validates that the DSI extraction layer:
- Writes only to the ext schema (not stg)
- Does not generate CleanCode columns
- Uses ext_* asset keys for all extraction outputs
- DSA staging module owns all pre_stg, stg_dim, stg_fct assets
- Config classes satisfy the ExtractionConfig protocol
"""

from DSI.connectors.mssql.models import TableConfig
from DSI.connectors.oracle.models import TableExtractionConfig
from DSI.connectors.protocol import ExtractionConfig
from DSI.mssql import (
    ILS_TABLE_CONFIGS,
    ILS_TABLE_CONFIGS_DAILY,
    ILS_TABLE_CONFIGS_MONTHLY,
    MYSUPPLY_TABLE_CONFIGS_DAILY,
    MYSUPPLY_TABLE_CONFIGS_MONTHLY,
    SYDAX_TABLE_CONFIGS_DAILY,
    SYDAX_TABLE_CONFIGS_MONTHLY,
)


class TestExtOnlyContract:
    """DSI extraction layer must only write to ext schema with no CleanCode generation."""

    def test_table_config_has_no_clean_code_columns_field(self):
        """TableConfig should not have a clean_code_columns attribute."""
        config = TableConfig(
            output_name="ext_test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test",
        )
        assert not hasattr(config, "clean_code_columns")

    def test_all_ils_configs_use_ext_prefix(self):
        """All ILS extraction configs must have ext_* output names."""
        for config in ILS_TABLE_CONFIGS:
            assert config.output_name.startswith(
                "ext_"
            ), f"{config.output_name} should start with ext_"

    def test_all_ils_daily_configs_use_ext_prefix(self):
        """All ILS daily extraction configs must have ext_* output names."""
        for config in ILS_TABLE_CONFIGS_DAILY:
            assert config.output_name.startswith(
                "ext_"
            ), f"{config.output_name} should start with ext_"

    def test_all_ils_monthly_configs_use_ext_prefix(self):
        """All ILS monthly extraction configs must have ext_* output names."""
        for config in ILS_TABLE_CONFIGS_MONTHLY:
            assert config.output_name.startswith(
                "ext_"
            ), f"{config.output_name} should start with ext_"

    def test_all_sydax_configs_use_ext_prefix(self):
        """All SydAX extraction configs must have ext_* output names."""
        for config in SYDAX_TABLE_CONFIGS_DAILY:
            assert config.output_name.startswith(
                "ext_"
            ), f"{config.output_name} should start with ext_"

    def test_all_sydax_monthly_configs_use_ext_prefix(self):
        """All SydAX monthly extraction configs must have ext_* output names."""
        for config in SYDAX_TABLE_CONFIGS_MONTHLY:
            assert config.output_name.startswith(
                "ext_"
            ), f"{config.output_name} should start with ext_"

    def test_all_mysupply_configs_use_ext_prefix(self):
        """All MySupply extraction configs must have ext_* output names."""
        for config in MYSUPPLY_TABLE_CONFIGS_DAILY:
            assert config.output_name.startswith(
                "ext_"
            ), f"{config.output_name} should start with ext_"

    def test_all_mysupply_monthly_configs_use_ext_prefix(self):
        """All MySupply monthly extraction configs must have ext_* output names."""
        for config in MYSUPPLY_TABLE_CONFIGS_MONTHLY:
            assert config.output_name.startswith(
                "ext_"
            ), f"{config.output_name} should start with ext_"

    def test_no_stg_prefix_in_any_config(self):
        """No extraction config should use the old stg_* prefix."""
        all_configs = (
            list(ILS_TABLE_CONFIGS)
            + list(ILS_TABLE_CONFIGS_DAILY)
            + list(ILS_TABLE_CONFIGS_MONTHLY)
            + list(SYDAX_TABLE_CONFIGS_DAILY)
            + list(SYDAX_TABLE_CONFIGS_MONTHLY)
            + list(MYSUPPLY_TABLE_CONFIGS_DAILY)
            + list(MYSUPPLY_TABLE_CONFIGS_MONTHLY)
        )
        for config in all_configs:
            assert not config.output_name.startswith(
                "stg_"
            ), f"Extraction config {config.output_name} still uses old stg_ prefix"


class TestExtractAssetGroups:
    """Verify extraction assets use correct group names."""

    def test_extract_assets_use_ext_group_names(self):
        """All extract assets should use erp_extract* or allowed custom groups."""
        from dagster import AssetsDefinition
        from DSI.mssql import all_extract_assets

        allowed_groups = {"erp_lager", "erp_rekvirent", "erp_staging"}
        for asset_def in all_extract_assets:
            if not isinstance(asset_def, AssetsDefinition):
                continue
            for spec in asset_def.specs:
                if spec.group_name is None:
                    continue
                assert spec.group_name.startswith("erp_extract") or spec.group_name in (
                    allowed_groups
                ), f"Asset {spec.key} has group {spec.group_name}, expected erp_extract* or {allowed_groups}"

    def test_full_load_assets_not_in_base_extract_group(self):
        """*_full_load extract assets must live in erp_extract_full_load.

        The daily/monthly jobs select the base erp_extract group (small
        reference tables); on-demand full re-extraction assets must not leak
        into those runs.
        """
        from dagster import AssetsDefinition
        from DSI.mssql import all_extract_assets

        for asset_def in all_extract_assets:
            if not isinstance(asset_def, AssetsDefinition):
                continue
            for spec in asset_def.specs:
                if spec.key.path[-1].endswith("_full_load"):
                    assert spec.group_name == "erp_extract_full_load", (
                        f"Asset {spec.key} has group {spec.group_name}, "
                        "expected erp_extract_full_load"
                    )
                else:
                    assert spec.group_name != "erp_extract_full_load", (
                        f"Asset {spec.key} is in erp_extract_full_load but is not "
                        "a *_full_load asset"
                    )


class TestDSAStagingOwnership:
    """Verify DSA owns all pre_stg, stg_dim, and stg_fct assets."""

    def test_dsa_staging_contains_all_pre_stg(self):
        """DSA staging module should contain all pre_stg assets."""
        from DSA.SQL.staging import (
            pre_stg_AfdelingLagerbeholdning_ils,
            pre_stg_bridge_vare_ils,
            pre_stg_fakturalinje_mysupply,
            pre_stg_finanskladdelinje_sydax,
            pre_stg_Lagerbeholdning_ils,
            pre_stg_postering_sydax,
            pre_stg_vareaftaler_ils,
            pre_stg_vareudlevering_ils,
        )

        assets = [
            pre_stg_bridge_vare_ils,
            pre_stg_fakturalinje_mysupply,
            pre_stg_finanskladdelinje_sydax,
            pre_stg_postering_sydax,
            pre_stg_vareudlevering_ils,
            pre_stg_vareaftaler_ils,
            pre_stg_Lagerbeholdning_ils,
            pre_stg_AfdelingLagerbeholdning_ils,
        ]
        assert len(assets) == 8

    def test_dsa_staging_contains_all_stg_dim(self):
        """DSA staging module should contain all stg_dim assets."""
        from DSA.SQL.staging import (
            stg_dim_aftale_ils,
            stg_dim_faktura_mysupply,
            stg_dim_fakturalinje_mysupply,
            stg_dim_finanskladde_sydax,
            stg_dim_finanskladdelinje_sydax,
            stg_dim_kontoplan_sydax,
            stg_dim_kreditor_ils,
            stg_dim_kreditor_mysupply,
            stg_dim_kreditor_sydax,
            stg_dim_lager,
            stg_dim_postering_sydax,
            stg_dim_rekvirent,
            stg_dim_vare,
            stg_dim_vare_ils,
            stg_dim_vare_mysupply,
            stg_dim_vareudlevering_ils,
        )

        assets = [
            stg_dim_aftale_ils,
            stg_dim_faktura_mysupply,
            stg_dim_fakturalinje_mysupply,
            stg_dim_finanskladde_sydax,
            stg_dim_finanskladdelinje_sydax,
            stg_dim_kontoplan_sydax,
            stg_dim_kreditor_ils,
            stg_dim_kreditor_mysupply,
            stg_dim_kreditor_sydax,
            stg_dim_lager,
            stg_dim_postering_sydax,
            stg_dim_rekvirent,
            stg_dim_vareudlevering_ils,
            stg_dim_vare,
            stg_dim_vare_ils,
            stg_dim_vare_mysupply,
        ]
        assert len(assets) == 16

    def test_dsa_staging_contains_all_stg_fct(self):
        """DSA staging module should contain all stg_fct assets."""
        from DSA.SQL.staging import (
            stg_fct_faktura_mysupply,
            stg_fct_fakturalinje_mysupply,
            stg_fct_finanskladdelinje_sydax,
            stg_fct_postering_sydax,
            stg_fct_vareudlevering_ils,
        )

        assets = [
            stg_fct_faktura_mysupply,
            stg_fct_fakturalinje_mysupply,
            stg_fct_finanskladdelinje_sydax,
            stg_fct_postering_sydax,
            stg_fct_vareudlevering_ils,
        ]
        assert len(assets) == 5

    def test_dsa_staging_assets_list_count(self):
        """DSA staging_assets list should have all assets (ERP + kraeftpakker + org)."""
        from DSA.SQL.staging import staging_assets

        assert len(staging_assets) == 42

    def test_no_duplicate_asset_keys(self):
        """DSA + DSI extract should have no duplicate asset keys."""
        from dagster import AssetsDefinition
        from DSA.SQL.dim import all_assets as dim_all
        from DSA.SQL.fct import all_assets as fct_all
        from DSA.SQL.map import all_assets as map_all
        from DSA.SQL.staging import all_assets as staging_all
        from infrastructure.assets.index_maintenance import erp_index_maintenance

        erp_assets = [*staging_all, *dim_all, *map_all, *fct_all, erp_index_maintenance]
        from DSI.mssql import all_extract_assets

        all_keys = set()
        dupes = []
        for a in erp_assets + all_extract_assets:
            if isinstance(a, AssetsDefinition):
                for k in a.keys:
                    if k in all_keys:
                        dupes.append(k)
                    all_keys.add(k)
        assert not dupes, f"Duplicate asset keys found: {dupes}"


class TestExtractionConfigProtocol:
    """Verify that both domain config classes satisfy the ExtractionConfig protocol."""

    def test_table_config_satisfies_protocol(self):
        """ERP TableConfig satisfies ExtractionConfig protocol."""
        config = TableConfig(
            output_name="ext_test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test config",
            partition_column="created_date",
        )
        assert isinstance(config, ExtractionConfig)

    def test_table_extraction_config_satisfies_protocol(self):
        """IFDB TableExtractionConfig satisfies ExtractionConfig protocol."""
        config = TableExtractionConfig(
            table="IFDB.TEST",
            columns=["COL1"],
            partition_column="MOD_DATE",
            description="Test config",
        )
        assert isinstance(config, ExtractionConfig)

    def test_table_config_none_partition_satisfies_protocol(self):
        """Config with None partition_column still satisfies protocol."""
        config = TableConfig(
            output_name="ext_test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test config",
            partition_column=None,
        )
        assert isinstance(config, ExtractionConfig)

    def test_shared_core_module_importable(self):
        """Shared core module exports are importable."""
        from DSI.connectors import (
            ExtractionConfig,
            PartitionType,
            TableType,
            create_extraction_assets,
        )

        assert ExtractionConfig is not None
        assert PartitionType.FULL_LOAD == "full_load"
        assert TableType.MASTER_DATA == "master_data"
        assert callable(create_extraction_assets)


class TestCleanCodeInDSA:
    """Verify CleanCode builders are accessible from DSA."""

    def test_clean_codes_importable_from_dsa(self):
        """CleanCode builders should be importable from DSA staging."""
        from DSA.SQL.staging.clean_codes import (
            cvr_clean_code,
            kontoplan_clean_code,
            postering_clean_code,
        )

        assert callable(cvr_clean_code)
        assert callable(kontoplan_clean_code)
        assert callable(postering_clean_code)
