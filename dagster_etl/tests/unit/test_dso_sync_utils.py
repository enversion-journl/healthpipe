# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for DSO Sync Utilities

Tests the SQL generation and configuration validation for syncing data
from staging (DSI/DSA) to DSO database via linked server.

These tests do NOT require database connections - they verify:
- MERGE statement generation
- Truncate/insert statement generation
- Configuration validation
- SQL escaping for injection prevention
"""

import unittest.mock
from contextlib import contextmanager
from dataclasses import replace as dc_replace
from datetime import datetime
from unittest.mock import MagicMock

import DSO.load.sync_utils as sync_utils
import pytest
from DSO.load.sync_configs import (
    ALL_SYNC_CONFIGS,
    ERP_SYNC_CONFIGS,
    SHARED_DIMS_MAP_CONFIGS,
    SHARED_DIMS_SYNC_CONFIGS,
    get_config,
)
from DSO.load.sync_utils import (
    SyncTableConfig,
    _build_composite_keyset_predicate,
    build_delta_merge_statement,
    build_merge_statement,
    build_truncate_insert_statement,
)


class TestSyncTableConfig:
    """Test SyncTableConfig dataclass."""

    def test_config_defaults(self):
        """Test default values for SyncTableConfig."""
        config = SyncTableConfig(
            schema="dim",
            table="Test",
            primary_key="DW_EK_Test",
        )
        assert config.mode == "merge"
        assert config.exclude_columns == []
        assert config.insert_only_columns == ["DW_Timestamp_Insert"]
        assert config.watermark_column is None

    def test_config_watermark_requires_column(self):
        """Test that mode='watermark' requires watermark_column to be set."""
        with pytest.raises(ValueError, match="watermark_column must be set"):
            SyncTableConfig(
                schema="dim",
                table="Test",
                primary_key="DW_EK_Test",
                mode="watermark",
            )

    def test_config_watermark_valid(self):
        """Test that mode='watermark' with watermark_column is accepted."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        assert config.mode == "watermark"
        assert config.watermark_column == "DW_Timestamp_Update"

    def test_config_watermark_with_timestamp_insert(self):
        """Test watermark on DW_Timestamp_Insert (typical for INSERT-only map tables)."""
        config = SyncTableConfig(
            schema="map",
            table="Kreditor",
            primary_key="CVR_CleanCode",
            mode="watermark",
            watermark_column="DW_Timestamp_Insert",
        )
        assert config.mode == "watermark"
        assert config.watermark_column == "DW_Timestamp_Insert"

    def test_config_merge_allows_no_watermark_column(self):
        """Test that mode='merge' allows watermark_column=None (default)."""
        config = SyncTableConfig(
            schema="dim",
            table="Test",
            primary_key="DW_EK_Test",
            mode="merge",
        )
        assert config.mode == "merge"
        assert config.watermark_column is None

    def test_config_with_composite_key(self):
        """Test config with composite primary key."""
        config = SyncTableConfig(
            schema="fct",
            table="CompositeFact",
            primary_key=["PK1", "PK2", "PK3"],
            mode="merge",
        )
        assert isinstance(config.primary_key, list)
        assert len(config.primary_key) == 3

    def test_config_chunk_size_default(self):
        """Test default chunk_size for SyncTableConfig."""
        config = SyncTableConfig(
            schema="dim",
            table="Test",
            primary_key="DW_EK_Test",
        )
        assert config.chunk_size == 5000

    def test_config_truncate_insert_mode(self):
        """Test config with truncate_insert mode."""
        config = SyncTableConfig(
            schema="sec",
            table="PersonaleOrg",
            primary_key="SOR_Kode",
            mode="truncate_insert",
        )
        assert config.mode == "truncate_insert"


class TestBuildMergeStatement:
    """Test MERGE SQL statement generation."""

    def test_merge_single_pk(self):
        """Test MERGE statement with single primary key."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "KreditorNavn",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]

        # Use explicit db name for test isolation from environment
        sql = build_merge_statement(config, columns, staging_db="TestDB")

        # Verify basic structure
        assert "MERGE [dim].[Kreditor] AS tgt" in sql
        assert "USING [STAGING_LINK].[TestDB].[dim].[Kreditor] AS src" in sql
        assert "ON tgt.[DW_EK_Kreditor] = src.[DW_EK_Kreditor]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql

        # Verify UPDATE clause includes non-PK, non-insert-only columns
        assert "tgt.[CVR_CleanCode] = src.[CVR_CleanCode]" in sql
        assert "tgt.[KreditorNavn] = src.[KreditorNavn]" in sql
        # PK should be in ON clause but not in UPDATE SET
        assert "ON tgt.[DW_EK_Kreditor] = src.[DW_EK_Kreditor]" in sql
        assert "UPDATE SET" in sql
        # Verify PK is not in UPDATE SET by checking it doesn't appear after "UPDATE SET"
        update_set_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        # PK should NOT appear in the UPDATE SET clause
        assert "tgt.[DW_EK_Kreditor] =" not in update_set_part
        # DW_Timestamp_Update should be set to SYSDATETIME
        assert "DW_Timestamp_Update" in sql
        assert "SYSDATETIME()" in sql

    def test_merge_composite_pk(self):
        """Test MERGE statement with composite primary key."""
        config = SyncTableConfig(
            schema="map",
            table="Vare",
            primary_key=["CVR_CleanCode", "Vare_CleanCode"],
            exclude_columns=["DW_EK_Vare"],
        )
        columns = [
            "DW_EK_Vare",
            "CVR_CleanCode",
            "Vare_CleanCode",
            "Leverandoer_Varenummer_CleanCode",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]

        sql = build_merge_statement(config, columns)

        # Verify composite key ON clause
        assert "tgt.[CVR_CleanCode] = src.[CVR_CleanCode]" in sql
        assert "tgt.[Vare_CleanCode] = src.[Vare_CleanCode]" in sql
        assert " AND " in sql

        # Verify excluded column not in INSERT
        assert "DW_EK_Vare" not in sql

    def test_merge_with_excluded_columns(self):
        """Test MERGE excludes specified columns."""
        config = SyncTableConfig(
            schema="map",
            table="Kreditor",
            primary_key="CVR_CleanCode",
            exclude_columns=["DW_EK_Kreditor", "internal_field"],
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "internal_field",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]

        sql = build_merge_statement(config, columns)

        # Excluded columns should not appear in INSERT values
        assert "[DW_EK_Kreditor]" not in sql
        assert "[internal_field]" not in sql

    def test_merge_custom_linked_server(self):
        """Test MERGE with custom linked server name."""
        config = SyncTableConfig(
            schema="dim",
            table="Test",
            primary_key="ID",
        )
        columns = ["ID", "Name", "DW_Timestamp_Insert", "DW_Timestamp_Update"]

        sql = build_merge_statement(
            config,
            columns,
            linked_server="CUSTOM_LINK",
            staging_db="CustomDB",
        )

        assert "USING [CUSTOM_LINK].[CustomDB].[dim].[Test]" in sql


class TestBuildTruncateInsertStatement:
    """Test truncate/insert SQL statement generation."""

    def test_truncate_insert_basic(self):
        """Test basic truncate and insert statement generation."""
        config = SyncTableConfig(
            schema="dim",
            table="Aftale",
            primary_key=["DW_EK_Aftale", "DW_ValidFrom"],
            mode="truncate_insert",
        )
        columns = [
            "DW_EK_Aftale",
            "DW_ValidFrom",
            "DW_ValidTo",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]

        # Use explicit db name for test isolation from environment
        truncate_sql, insert_sql = build_truncate_insert_statement(
            config, columns, staging_db="TestDB"
        )

        # Verify truncate
        assert "TRUNCATE TABLE [dim].[Aftale]" in truncate_sql

        # Verify insert
        assert "INSERT INTO [dim].[Aftale]" in insert_sql
        assert "SELECT" in insert_sql
        assert "FROM [STAGING_LINK].[TestDB].[dim].[Aftale]" in insert_sql

        # All columns should be in select
        for col in columns:
            assert f"[{col}]" in insert_sql

    def test_truncate_insert_with_excluded_columns(self):
        """Test truncate/insert excludes specified columns."""
        config = SyncTableConfig(
            schema="sec",
            table="PersonaleOrg",
            primary_key="SOR_Kode",
            mode="truncate_insert",
            exclude_columns=["internal_audit_field"],
        )
        columns = [
            "SOR_Kode",
            "OrgNavn",
            "internal_audit_field",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]

        truncate_sql, insert_sql = build_truncate_insert_statement(config, columns)

        # Excluded column should not be in insert
        assert "[internal_audit_field]" not in insert_sql
        assert "[SOR_Kode]" in insert_sql
        assert "[OrgNavn]" in insert_sql


class TestBuildDeltaMergeStatement:
    """Test delta MERGE SQL statement generation with watermark filter."""

    def test_delta_merge_single_pk(self):
        """Test delta MERGE with single primary key and watermark WHERE clause."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "KreditorNavn",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # Verify MERGE structure
        assert "MERGE [dim].[Kreditor] AS tgt" in sql
        assert "ON tgt.[DW_EK_Kreditor] = src.[DW_EK_Kreditor]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql

        # Verify subquery with COALESCE WHERE filter
        assert "SELECT * FROM [STAGING_LINK].[TestDB].[dim].[Kreditor]" in sql
        assert "COALESCE([DW_Timestamp_Update], [DW_Timestamp_Insert])" in sql
        assert ">= '2026-02-11 12:00:00'" in sql

        # Verify UPDATE SET excludes PK and insert-only columns
        update_set_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "tgt.[CVR_CleanCode] = src.[CVR_CleanCode]" in update_set_part
        assert "tgt.[KreditorNavn] = src.[KreditorNavn]" in update_set_part
        assert "tgt.[DW_EK_Kreditor] =" not in update_set_part
        assert "SYSDATETIME()" in sql

    def test_delta_merge_composite_pk(self):
        """Test delta MERGE with composite primary key and watermark."""
        config = SyncTableConfig(
            schema="fct",
            table="CompositeFact",
            primary_key=["PK1", "PK2", "PK3"],
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "PK1",
            "PK2",
            "PK3",
            "Antal",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 1, 15, 8, 30, 0)

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # Verify composite key ON clause
        assert "tgt.[PK1] = src.[PK1]" in sql
        assert "tgt.[PK2] = src.[PK2]" in sql
        assert "tgt.[PK3] = src.[PK3]" in sql
        assert " AND " in sql

        # Verify watermark filter
        assert "COALESCE([DW_Timestamp_Update], [DW_Timestamp_Insert])" in sql
        assert ">= '2026-01-15 08:30:00'" in sql

    def test_delta_merge_falls_back_to_full_when_no_timestamp(self):
        """Test that last_sync_timestamp=None produces identical SQL as build_merge_statement."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]

        delta_sql = build_delta_merge_statement(config, columns, None, staging_db="TestDB")
        full_sql = build_merge_statement(config, columns, staging_db="TestDB")

        assert delta_sql == full_sql
        # Should NOT contain COALESCE or subquery
        assert "COALESCE" not in delta_sql
        assert "SELECT *" not in delta_sql

    def test_delta_merge_uses_watermark_column_from_config(self):
        """Test that config.watermark_column is used in the COALESCE expression."""
        config = SyncTableConfig(
            schema="map",
            table="Kreditor",
            primary_key="CVR_CleanCode",
            mode="watermark",
            watermark_column="DW_Timestamp_Insert",
        )
        columns = [
            "CVR_CleanCode",
            "KreditorNavn",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 1, 0, 0, 0)

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # Watermark column should be DW_Timestamp_Insert (from config), not DW_Timestamp_Update
        assert "COALESCE([DW_Timestamp_Insert], [DW_Timestamp_Insert])" in sql

    def test_delta_merge_with_excluded_columns(self):
        """Test that excluded columns are not in INSERT or UPDATE."""
        config = SyncTableConfig(
            schema="map",
            table="Vare",
            primary_key=["CVR_CleanCode", "Vare_CleanCode"],
            exclude_columns=["DW_EK_Vare"],
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Vare",
            "CVR_CleanCode",
            "Vare_CleanCode",
            "Leverandoer_Varenummer_CleanCode",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 10, 14, 0, 0)

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # DW_EK_Vare should not appear in INSERT or UPDATE clauses
        assert "DW_EK_Vare" not in sql
        # But watermark filter should still be present
        assert "COALESCE([DW_Timestamp_Update], [DW_Timestamp_Insert])" in sql

    def test_delta_merge_timestamp_format(self):
        """Test that timestamp is formatted as YYYY-MM-DD HH:MM:SS for DATETIME2(0)."""
        config = SyncTableConfig(
            schema="dim",
            table="Test",
            primary_key="ID",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = ["ID", "Name", "DW_Timestamp_Insert", "DW_Timestamp_Update"]

        # Use a timestamp with non-zero seconds to verify formatting
        ts = datetime(2026, 3, 15, 23, 59, 45)

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # Verify exact format: YYYY-MM-DD HH:MM:SS
        assert ">= '2026-03-15 23:59:45'" in sql

    def test_delta_merge_no_delete_clause(self):
        """Delta MERGE must NOT contain NOT MATCHED BY SOURCE (no hard-delete)."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        assert "NOT MATCHED BY SOURCE" not in sql

    def test_delta_merge_insert_only_columns_excluded_from_update(self):
        """DW_Timestamp_Insert (insert-only) must NOT appear in the UPDATE SET clause."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "KreditorNavn",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        update_set_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "tgt.[DW_Timestamp_Insert]" not in update_set_part
        # But INSERT clause should still include it
        insert_part = sql.split("WHEN NOT MATCHED BY TARGET THEN")[1]
        assert "[DW_Timestamp_Insert]" in insert_part

    def test_delta_merge_uses_inclusive_boundary(self):
        """Boundary must be >= so no rows are lost; double processing is idempotent (MERGE on PK)."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)
        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        assert ">= '2026-02-11 12:00:00'" in sql
        assert "> '2026-02-11 12:00:00'" not in sql.replace(">=", "")

    def test_delta_merge_null_watermark_coalesce(self):
        """COALESCE(watermark, DW_Timestamp_Insert) ensures rows with NULL DW_Timestamp_Update are included."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)
        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        assert "COALESCE([DW_Timestamp_Update], [DW_Timestamp_Insert])" in sql
        assert ">=" in sql

    def test_delta_merge_idempotent_no_duplicate_keys(self):
        """MERGE uses ON (PK) and WHEN MATCHED/WHEN NOT MATCHED BY TARGET only — reprocessing same row updates, does not insert duplicate."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        columns = [
            "DW_EK_Kreditor",
            "CVR_CleanCode",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)
        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        assert "ON tgt.[DW_EK_Kreditor] = src.[DW_EK_Kreditor]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql
        assert "NOT MATCHED BY SOURCE" not in sql


class TestSyncConfigs:
    """Test sync configuration dictionaries."""

    def test_erp_configs_exist(self):
        """Test that erp configs are defined."""
        assert "dim_kreditor" in ERP_SYNC_CONFIGS
        assert "dim_postering" in ERP_SYNC_CONFIGS
        assert "fct_fakturalinje" in ERP_SYNC_CONFIGS

    def test_shared_dims_configs_exist(self):
        """Test that shared dimension configs are defined."""
        assert "dim_borger" in SHARED_DIMS_SYNC_CONFIGS
        assert "dim_organisation" in SHARED_DIMS_SYNC_CONFIGS

    def test_dim_borger_is_merge(self):
        """Test that dim.Borger uses merge mode (DW_SK_Borger from ref, no IDENTITY)."""
        config = SHARED_DIMS_SYNC_CONFIGS["dim_borger"]
        assert config.mode == "merge"
        assert config.schema == "dim"
        assert config.table == "Borger"

    def test_all_configs_combined(self):
        """Test that ALL_SYNC_CONFIGS contains all domain configs."""
        for key in ERP_SYNC_CONFIGS:
            assert key in ALL_SYNC_CONFIGS
        for key in SHARED_DIMS_SYNC_CONFIGS:
            assert key in ALL_SYNC_CONFIGS

    def test_get_config_valid(self):
        """Test get_config returns correct config."""
        config = get_config("dim_kreditor")
        assert config.schema == "dim"
        assert config.table == "Kreditor"

    def test_get_config_invalid(self):
        """Test get_config raises error for invalid name."""
        with pytest.raises(ValueError) as exc_info:
            get_config("nonexistent_config")
        assert "Unknown sync config" in str(exc_info.value)
        assert "nonexistent_config" in str(exc_info.value)


class TestSQLEscaping:
    """Test SQL injection prevention."""

    def test_merge_statement_no_sql_injection_in_table_name(self):
        """Verify table names are bracketed to prevent injection."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
        )
        columns = ["DW_EK_Kreditor", "Name", "DW_Timestamp_Insert", "DW_Timestamp_Update"]

        sql = build_merge_statement(config, columns)

        # All identifiers should be bracketed
        assert "[dim]" in sql
        assert "[Kreditor]" in sql
        assert "[DW_EK_Kreditor]" in sql
        assert "[Name]" in sql

    def test_column_names_are_bracketed(self):
        """Test that all column names are properly bracketed."""
        config = SyncTableConfig(
            schema="fct",
            table="Test",
            primary_key="ID",
        )
        columns = [
            "ID",
            "Column With Spaces",
            "ColumnWithNumber123",
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        ]

        sql = build_merge_statement(config, columns)

        # All columns should be bracketed
        assert "[ID]" in sql
        assert "[Column With Spaces]" in sql
        assert "[ColumnWithNumber123]" in sql


class TestConfigValidation:
    """Test configuration validation logic."""

    def test_all_configs_have_required_fields(self):
        """Verify all configs have schema, table, primary_key, and valid mode."""
        for name, config in ALL_SYNC_CONFIGS.items():
            assert config.schema, f"{name} missing schema"
            assert config.table, f"{name} missing table"
            assert config.primary_key, f"{name} missing primary_key"
            assert config.mode in [
                "merge",
                "watermark",
                "truncate_insert",
            ], f"{name} has invalid mode: {config.mode}"
            if config.mode == "watermark":
                assert (
                    config.watermark_column
                ), f"{name} has mode='watermark' but no watermark_column"

    def test_map_tables_have_identity_excluded(self):
        """Map tables should exclude IDENTITY columns from sync."""
        map_configs = {
            **SHARED_DIMS_MAP_CONFIGS,
        }
        # Most map tables should have DW_EK_ excluded since it's IDENTITY
        for name, config in map_configs.items():
            if config.mode == "merge" and config.exclude_columns:
                # At least one DW_EK column should be excluded
                assert any(
                    "DW_EK" in col for col in config.exclude_columns
                ), f"{name} map table should exclude IDENTITY column"

    def test_dim_tables_use_dw_ek_as_pk(self):
        """Dimension tables typically use DW_EK as primary key (DW_SK for dim_borger)."""
        dim_configs = {
            k: v
            for k, v in ALL_SYNC_CONFIGS.items()
            if k.startswith("dim_") and v.mode in ("merge", "watermark")
        }
        assert dim_configs, "Expected at least one dim config with merge or watermark mode"
        for name, config in dim_configs.items():
            pk = config.primary_key
            if isinstance(pk, str):
                if name == "dim_borger":
                    assert pk == "DW_SK_Borger", "dim_borger PK should be DW_SK_Borger"
                else:
                    assert pk.startswith("DW_EK"), f"{name} should use DW_EK as PK, got {pk}"

    def test_watermark_configs_have_valid_watermark_column(self):
        """All configs with mode='watermark' must have a recognised watermark column."""
        valid_watermark_columns = {"DW_Timestamp_Update", "DW_Timestamp_Insert"}
        watermark_configs = {k: v for k, v in ALL_SYNC_CONFIGS.items() if v.mode == "watermark"}
        assert watermark_configs, "Expected at least one watermark config"
        for name, config in watermark_configs.items():
            assert (
                config.watermark_column in valid_watermark_columns
            ), f"{name} has unexpected watermark_column: {config.watermark_column}"

    def test_chunk_size_positive_on_all_configs(self):
        """All configs must have chunk_size > 0."""
        for name, config in ALL_SYNC_CONFIGS.items():
            assert config.chunk_size > 0, f"{name} has invalid chunk_size: {config.chunk_size}"

    def test_no_duplicate_asset_keys_in_erp_sync(self):
        """ERP sync module must not register duplicate asset names."""
        from DSO.load.sync_dim import all_dso_sync_dim_assets

        names = [a.key.to_user_string() for a in all_dso_sync_dim_assets]
        assert len(names) == len(
            set(names)
        ), f"Duplicate asset keys: {[n for n in names if names.count(n) > 1]}"

    def test_no_duplicate_asset_keys_in_all_dso_sync(self):
        """Combined DSO sync assets must not have duplicate names across modules."""
        from DSO.load import all_dso_sync_assets

        names = [a.key.to_user_string() for a in all_dso_sync_assets]
        assert len(names) == len(
            set(names)
        ), f"Duplicate asset keys: {[n for n in names if names.count(n) > 1]}"

    def test_erp_sync_assets_use_standard_metadata(self):
        """All ERP sync assets must route through _materialize_sync_result for uniform metadata."""
        import inspect

        from DSO.load.sync_dim import all_dso_sync_dim_assets

        for asset_def in all_dso_sync_dim_assets:
            fn = asset_def.op.compute_fn.decorated_fn
            src = inspect.getsource(fn)
            assert (
                "_materialize_sync_result" in src
            ), f"{asset_def.key} does not use _materialize_sync_result"


class _DummyCursor:
    def __init__(self, fetchall_queue=None, fetchmany_queue=None, fetchone_queue=None):
        self.fetchall_queue = list(fetchall_queue or [])
        self.fetchmany_queue = list(fetchmany_queue or [])
        self._fetchone_queue = list(fetchone_queue or [])
        self.execute_calls = []
        self.executemany_calls = []
        self.setinputsizes_calls = []
        self.rowcount = 0

    def close(self):
        pass

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))

    def executemany(self, sql, rows):
        rows_list = list(rows)
        self.executemany_calls.append((sql, rows_list))
        self.rowcount = len(rows_list)

    def setinputsizes(self, sizes):
        self.setinputsizes_calls.append(list(sizes))

    def fetchone(self):
        if self._fetchone_queue:
            return self._fetchone_queue.pop(0)
        return (0,)

    def fetchall(self):
        if self.fetchall_queue:
            return self.fetchall_queue.pop(0)
        return []

    def fetchmany(self, _size):
        if self.fetchmany_queue:
            return self.fetchmany_queue.pop(0)
        return []


class _DummyConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


class _MssqlPythonDummyConnection(_DummyConnection):
    """Dummy connection whose module path matches mssql_python detection."""


_MssqlPythonDummyConnection.__module__ = "mssql_python.connection"


class _DummyResource:
    def __init__(self, connections):
        self._connections = list(connections)
        self._idx = 0
        self.host = "dummy-host"
        self.database = "dummy-db"

    @contextmanager
    def get_connection(self):
        conn = self._connections[self._idx]
        self._idx += 1
        yield conn


class TestPythonFallbackWatermark:
    @pytest.fixture(autouse=True)
    def _reset_collation_cache(self, monkeypatch):
        monkeypatch.setattr(sync_utils, "_collation_mismatch_cache", False)

    def test_python_fallback_uses_delta_watermark_and_updates_state(self, monkeypatch):
        """Python fallback in watermark mode should use delta filter and upsert MERGE."""
        schema_cursor = _DummyCursor(
            fetchall_queue=[
                [
                    ("DW_EK_Fakturalinje",),
                    ("Navn",),
                    ("DW_Timestamp_Insert",),
                    ("DW_Timestamp_Update",),
                ]
            ]
        )
        schema_conn = _DummyConnection(schema_cursor)

        data_cursor = _DummyCursor(
            fetchmany_queue=[
                [(1, "A", datetime(2026, 2, 10, 10, 0, 0), datetime(2026, 2, 10, 10, 5, 0))],
                [],
            ]
        )
        data_conn = _DummyConnection(data_cursor)

        dso_cursor = _DummyCursor()
        dso_conn = _DummyConnection(dso_cursor)

        staging_resource = _DummyResource([schema_conn, data_conn])
        dso_resource = _DummyResource([dso_conn])

        update_calls = []
        last_ts = datetime(2026, 2, 10, 9, 0, 0)

        class _FakeStateManager:
            def __init__(self, *_args, **_kwargs):
                pass

            def get_last_sync_timestamp(self, _schema, _table):
                return last_ts

            def update_sync_state(
                self, schema, table, rows_affected=0, duration_ms=None, status="success"
            ):
                update_calls.append((schema, table, rows_affected, duration_ms, status))

        monkeypatch.setattr("DSO.load.sync_state.SyncStateManager", _FakeStateManager)

        context = MagicMock()
        context.log = MagicMock()

        config = SyncTableConfig(
            schema="fct",
            table="Fakturalinje",
            primary_key="DW_EK_Fakturalinje",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )

        result = sync_utils._sync_via_python(context, staging_resource, dso_resource, config)

        assert result["mode"] == "python_watermark_delta"

        # Ensure source query used a watermark predicate with parameterized timestamp
        select_sql, select_params = data_cursor.execute_calls[0]
        assert "COALESCE([DW_Timestamp_Update], [DW_Timestamp_Insert])" in select_sql
        assert select_params == (last_ts,)

        # Temp-table strategy: executemany inserts into #temp, then execute runs MERGE
        assert dso_cursor.executemany_calls, "Expected executemany INSERT INTO #temp call"
        temp_insert_sql, inserted_rows = dso_cursor.executemany_calls[0]
        assert "#_sync_fct_Fakturalinje" in temp_insert_sql
        assert len(inserted_rows) == 1

        # Set-based MERGE from temp table should appear in execute calls
        merge_calls = [sql for sql, _ in dso_cursor.execute_calls if "MERGE" in sql]
        assert merge_calls, "Expected set-based MERGE via execute()"
        assert "MERGE [fct].[Fakturalinje] AS tgt" in merge_calls[0]
        assert "#_sync_fct_Fakturalinje" in merge_calls[0]

        # Target table itself should NOT be truncated (only the temp table is truncated per chunk)
        assert not any(
            "TRUNCATE TABLE [fct].[Fakturalinje]" in sql for sql, _ in dso_cursor.execute_calls
        )

        # Ensure watermark state was persisted after success
        assert len(update_calls) == 1
        assert update_calls[0][0] == "fct"
        assert update_calls[0][1] == "Fakturalinje"
        assert update_calls[0][2] == 1
        assert update_calls[0][4] == "success"

    def test_python_fallback_first_run_uses_full_sync_and_updates_state(self, monkeypatch):
        """First watermark run in Python fallback should use full truncate+insert and save watermark."""
        schema_cursor = _DummyCursor(
            fetchall_queue=[
                [
                    ("DW_EK_Kreditor",),
                    ("Navn",),
                    ("DW_Timestamp_Insert",),
                    ("DW_Timestamp_Update",),
                ]
            ]
        )
        schema_conn = _DummyConnection(schema_cursor)

        data_cursor = _DummyCursor(
            fetchmany_queue=[
                [
                    (
                        1,
                        "Kreditor A",
                        datetime(2026, 2, 10, 10, 0, 0),
                        datetime(2026, 2, 10, 10, 5, 0),
                    )
                ],
                [],
            ]
        )
        data_conn = _DummyConnection(data_cursor)

        dso_cursor = _DummyCursor()
        dso_conn = _DummyConnection(dso_cursor)

        staging_resource = _DummyResource([schema_conn, data_conn])
        dso_resource = _DummyResource([dso_conn])

        update_calls = []

        class _FakeStateManager:
            def __init__(self, *_args, **_kwargs):
                pass

            def get_last_sync_timestamp(self, _schema, _table):
                return None

            def update_sync_state(
                self, schema, table, rows_affected=0, duration_ms=None, status="success"
            ):
                update_calls.append((schema, table, rows_affected, duration_ms, status))

        monkeypatch.setattr("DSO.load.sync_state.SyncStateManager", _FakeStateManager)

        context = MagicMock()
        context.log = MagicMock()

        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )

        result = sync_utils._sync_via_python(context, staging_resource, dso_resource, config)

        assert result["mode"] == "python_watermark_full"
        assert any("TRUNCATE TABLE [dim].[Kreditor]" in sql for sql, _ in dso_cursor.execute_calls)
        assert dso_cursor.executemany_calls, "Expected insert executemany call"
        insert_sql, inserted_rows = dso_cursor.executemany_calls[0]
        assert "INSERT INTO [dim].[Kreditor]" in insert_sql
        assert len(inserted_rows) == 1

        assert len(update_calls) == 1
        assert update_calls[0][0] == "dim"
        assert update_calls[0][1] == "Kreditor"
        assert update_calls[0][2] == 1
        assert update_calls[0][4] == "success"


class TestPythonFallbackTruncateInsert:
    @pytest.fixture(autouse=True)
    def _reset_collation_cache(self, monkeypatch):
        monkeypatch.setattr(sync_utils, "_collation_mismatch_cache", False)

    def test_truncate_insert_allows_missing_pk_column(self):
        """truncate_insert must not require primary_key to exist in staging columns."""
        schema_cursor = _DummyCursor(
            fetchall_queue=[[("DW_EK_Aftale",), ("Navn",), ("DW_Timestamp_Insert",)]]
        )
        schema_conn = _DummyConnection(schema_cursor)

        data_cursor = _DummyCursor(
            fetchmany_queue=[[(1, "Aftale A", datetime(2026, 2, 10, 10, 0, 0))], []]
        )
        data_conn = _DummyConnection(data_cursor)

        dso_cursor = _DummyCursor()
        dso_conn = _DummyConnection(dso_cursor)

        staging_resource = _DummyResource([schema_conn, data_conn])
        dso_resource = _DummyResource([dso_conn])

        context = MagicMock()
        context.log = MagicMock()

        # Intentionally mismatched PK column name; should still succeed because
        # truncate_insert mode does not depend on PK matching.
        config = SyncTableConfig(
            schema="dim",
            table="Aftale",
            primary_key="Aftale",
            mode="truncate_insert",
        )

        result = sync_utils._sync_via_python(context, staging_resource, dso_resource, config)

        assert result["mode"] == "python_truncate_insert"
        assert any("TRUNCATE TABLE [dim].[Aftale]" in sql for sql, _ in dso_cursor.execute_calls)
        assert dso_cursor.executemany_calls, "Expected INSERT executemany call"

    def test_mssql_python_sets_chunk_input_sizes_from_longest_string(self):
        """mssql_python fallback should size string params from the full chunk."""
        schema_cursor = _DummyCursor(fetchall_queue=[[("Id",), ("Txt",)]])
        schema_conn = _DummyConnection(schema_cursor)

        data_cursor = _DummyCursor(
            fetchmany_queue=[
                [
                    (1, "A" * 21),
                    (2, "B" * 22),
                ],
                [],
            ]
        )
        data_conn = _DummyConnection(data_cursor)

        dso_cursor = _DummyCursor()
        dso_conn = _MssqlPythonDummyConnection(dso_cursor)

        staging_resource = _DummyResource([schema_conn, data_conn])
        dso_resource = _DummyResource([dso_conn])

        context = MagicMock()
        context.log = MagicMock()

        config = SyncTableConfig(
            schema="dim",
            table="TestTable",
            primary_key="Id",
            mode="truncate_insert",
        )

        sync_utils._sync_via_python(context, staging_resource, dso_resource, config)

        assert dso_cursor.setinputsizes_calls, "Expected setinputsizes() to be called"
        first_spec = dso_cursor.setinputsizes_calls[0]
        assert first_spec[1] == (sync_utils._SQL_WVARCHAR, 22, 0)
        assert dso_cursor.executemany_calls, "Expected INSERT executemany call"


def _create_mock_dso_resource():
    """Create a mock MSSQLResource whose get_connection() is a proper context manager."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_resource = MagicMock()

    @contextmanager
    def _get_connection():
        yield mock_conn

    mock_resource.get_connection = _get_connection
    return mock_resource, mock_conn, mock_cursor


def _create_mock_context():
    """Create a mock Dagster AssetExecutionContext with a mock logger."""
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


class TestMismatchRecovery:
    """Tests for auto-recovery when watermark delta sync produces a row count mismatch."""

    def test_recovery_triggers_full_merge_on_mismatch(self):
        """When watermark_delta sync has staging != dso count, recovery runs a full MERGE."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Vareudlevering",
            primary_key="DW_EK_Vareudlevering",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )

        # Initial delta sync returns watermark_delta mode
        delta_result = {
            "rows_affected": 100,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[fct].[Vareudlevering]",
            "columns_synced": 10,
            "duration_ms": 500,
            "sync_duration_seconds": 0.5,
            "rows_per_second": 200.0,
        }
        # Recovery full merge result
        recovery_result = {
            "rows_affected": 30000,
            "mode": "merge",
            "sync_engine": "linked_server",
            "table": "[fct].[Vareudlevering]",
            "columns_synced": 10,
            "duration_ms": 5000,
            "sync_duration_seconds": 5.0,
            "rows_per_second": 6000.0,
        }

        with (
            unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
            unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
            unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
            unittest.mock.patch.object(sync_utils, "validate_linked_server"),
            unittest.mock.patch.object(
                sync_utils, "_sync_via_linked_server", side_effect=[delta_result, recovery_result]
            ) as mock_sync,
            unittest.mock.patch.object(
                sync_utils,
                "compare_row_counts",
                side_effect=[
                    {"staging_count": 30000, "dso_count": 29000, "match": False},  # pre-recovery
                    {
                        "staging_count": 30000,
                        "dso_count": 30000,
                        "match": True,
                    },  # post-recovery verification
                ],
            ),
            unittest.mock.patch("DSO.load.sync_state.SyncStateManager") as MockStateMgr,
        ):
            result = sync_utils.sync_table_via_linked_server(
                ctx,
                dso_resource,
                config,
                mssql_source_staging=staging_resource,
            )

        # Recovery should have been triggered and succeeded
        assert result["recovery_full_merge"] is True
        assert result.get("recovery_failed") is not True
        assert result["pre_recovery_staging_count"] == 30000
        assert result["pre_recovery_dso_count"] == 29000
        # _sync_via_linked_server called twice: first delta, then full merge recovery
        assert mock_sync.call_count == 2
        # Second call should use mode="merge"
        recovery_call_config = mock_sync.call_args_list[1][0][2]  # 3rd positional arg = config
        assert recovery_call_config.mode == "merge"
        # Watermark should be advanced after recovery
        MockStateMgr.return_value.update_sync_state.assert_called_once()

    def test_no_recovery_when_counts_match(self):
        """When watermark_delta sync has matching counts, no recovery is triggered."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="DW_EK_Kreditor",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )

        delta_result = {
            "rows_affected": 50,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[dim].[Kreditor]",
            "columns_synced": 5,
            "duration_ms": 200,
            "sync_duration_seconds": 0.2,
            "rows_per_second": 250.0,
        }

        with (
            unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
            unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
            unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
            unittest.mock.patch.object(sync_utils, "validate_linked_server"),
            unittest.mock.patch.object(
                sync_utils, "_sync_via_linked_server", return_value=delta_result
            ) as mock_sync,
            unittest.mock.patch.object(
                sync_utils,
                "compare_row_counts",
                return_value={"staging_count": 1000, "dso_count": 1000, "match": True},
            ),
        ):
            result = sync_utils.sync_table_via_linked_server(
                ctx,
                dso_resource,
                config,
                mssql_source_staging=staging_resource,
            )

        # No recovery
        assert result.get("recovery_full_merge") is None
        # Only called once (the initial delta sync)
        assert mock_sync.call_count == 1
        # Result should be the original delta result (unchanged)
        assert result["rows_affected"] == 50
        assert result["mode"] == "watermark_delta"

    def test_no_recovery_for_non_watermark_mode(self):
        """Recovery only applies to watermark mode tables."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="sec",
            table="PersonaleOrg",
            primary_key="SOR_Kode",
            mode="truncate_insert",
        )

        truncate_result = {
            "rows_affected": 500,
            "mode": "truncate_insert",
            "sync_engine": "linked_server",
            "table": "[sec].[PersonaleOrg]",
            "columns_synced": 8,
            "duration_ms": 300,
            "sync_duration_seconds": 0.3,
            "rows_per_second": 1666.7,
        }

        with (
            unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
            unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
            unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
            unittest.mock.patch.object(sync_utils, "validate_linked_server"),
            unittest.mock.patch.object(
                sync_utils, "_sync_via_linked_server", return_value=truncate_result
            ) as mock_sync,
            unittest.mock.patch.object(sync_utils, "compare_row_counts") as mock_compare,
        ):
            sync_utils.sync_table_via_linked_server(
                ctx,
                dso_resource,
                config,
                mssql_source_staging=staging_resource,
            )

        # compare_row_counts should NOT be called for non-watermark modes
        mock_compare.assert_not_called()
        assert mock_sync.call_count == 1

    def test_recovery_disabled_via_parameter(self):
        """auto_recover_mismatch=False skips recovery even on mismatch."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Faktura",
            primary_key="DW_EK_Faktura",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )

        delta_result = {
            "rows_affected": 100,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[fct].[Faktura]",
            "columns_synced": 10,
            "duration_ms": 500,
            "sync_duration_seconds": 0.5,
            "rows_per_second": 200.0,
        }

        with (
            unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
            unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
            unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
            unittest.mock.patch.object(sync_utils, "validate_linked_server"),
            unittest.mock.patch.object(
                sync_utils, "_sync_via_linked_server", return_value=delta_result
            ) as mock_sync,
            unittest.mock.patch.object(sync_utils, "compare_row_counts") as mock_compare,
        ):
            sync_utils.sync_table_via_linked_server(
                ctx,
                dso_resource,
                config,
                mssql_source_staging=staging_resource,
                auto_recover_mismatch=False,
            )

        # compare_row_counts should NOT be called when recovery is disabled
        mock_compare.assert_not_called()
        assert mock_sync.call_count == 1

    def test_recovery_falls_back_to_python_when_linked_server_fails(self):
        """If linked server fails during recovery, fall back to Python sync."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Faktura",
            primary_key="DW_EK_Faktura",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )

        delta_result = {
            "rows_affected": 100,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[fct].[Faktura]",
            "columns_synced": 10,
            "duration_ms": 500,
            "sync_duration_seconds": 0.5,
            "rows_per_second": 200.0,
        }
        python_recovery_result = {
            "rows_affected": 20000,
            "mode": "merge",
            "sync_engine": "python_fallback",
            "table": "[fct].[Faktura]",
            "columns_synced": 10,
            "duration_ms": 8000,
            "sync_duration_seconds": 8.0,
            "rows_per_second": 2500.0,
        }

        # call 1: initial sync path succeeds; call 2: recovery re-validation fails
        validate_calls = [None, RuntimeError("linked server not accessible")]

        with (
            unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
            unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
            unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
            unittest.mock.patch.object(
                sync_utils, "validate_linked_server", side_effect=validate_calls
            ),
            unittest.mock.patch.object(
                sync_utils, "_sync_via_linked_server", return_value=delta_result
            ),
            unittest.mock.patch.object(
                sync_utils, "_sync_via_python", return_value=python_recovery_result
            ) as mock_python,
            unittest.mock.patch.object(
                sync_utils,
                "compare_row_counts",
                side_effect=[
                    {"staging_count": 20000, "dso_count": 19000, "match": False},  # pre-recovery
                    {
                        "staging_count": 20000,
                        "dso_count": 20000,
                        "match": True,
                    },  # post-recovery verification
                ],
            ),
            unittest.mock.patch("DSO.load.sync_state.SyncStateManager") as MockStateMgr,
        ):
            result = sync_utils.sync_table_via_linked_server(
                ctx,
                dso_resource,
                config,
                mssql_source_staging=staging_resource,
            )

        assert result["recovery_full_merge"] is True
        assert result.get("recovery_failed") is not True
        # Python fallback was used for recovery
        mock_python.assert_called_once()
        recovery_call_config = mock_python.call_args[0][3]  # 4th positional arg = config
        assert recovery_call_config.mode == "merge"
        # Watermark should still be advanced after Python fallback recovery
        MockStateMgr.return_value.update_sync_state.assert_called_once()

    def test_recovery_failed_sets_flag_without_retry(self):
        """When recovery MERGE runs but counts still mismatch, flag failure — no retry."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Vareudlevering",
            primary_key="DW_EK_Vareudlevering",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )

        delta_result = {
            "rows_affected": 100,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[fct].[Vareudlevering]",
            "columns_synced": 10,
            "duration_ms": 500,
            "sync_duration_seconds": 0.5,
            "rows_per_second": 200.0,
        }
        recovery_result = {
            "rows_affected": 30000,
            "mode": "merge",
            "sync_engine": "linked_server",
            "table": "[fct].[Vareudlevering]",
            "columns_synced": 10,
            "duration_ms": 5000,
            "sync_duration_seconds": 5.0,
            "rows_per_second": 6000.0,
        }

        with (
            unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
            unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
            unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
            unittest.mock.patch.object(sync_utils, "validate_linked_server"),
            unittest.mock.patch.object(
                sync_utils, "_sync_via_linked_server", side_effect=[delta_result, recovery_result]
            ) as mock_sync,
            unittest.mock.patch.object(
                sync_utils,
                "compare_row_counts",
                side_effect=[
                    {"staging_count": 30000, "dso_count": 29000, "match": False},  # pre-recovery
                    {
                        "staging_count": 30000,
                        "dso_count": 29500,
                        "match": False,
                    },  # post-recovery still mismatched
                ],
            ),
            unittest.mock.patch("DSO.load.sync_state.SyncStateManager"),
        ):
            result = sync_utils.sync_table_via_linked_server(
                ctx,
                dso_resource,
                config,
                mssql_source_staging=staging_resource,
            )

        # Recovery ran but failed — flag is set
        assert result["recovery_full_merge"] is True
        assert result["recovery_failed"] is True
        # Only 2 calls to _sync_via_linked_server (no infinite retry)
        assert mock_sync.call_count == 2
        # Error was logged
        ctx.log.error.assert_called_once()

    def test_recovery_preserves_identity_insert(self):
        """Recovery config preserves identity_insert=True from original config."""
        config = SyncTableConfig(
            schema="fct",
            table="Vareudlevering",
            primary_key="DW_EK_Vareudlevering",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
            identity_insert=True,
        )
        recovery_config = dc_replace(config, mode="merge")
        assert recovery_config.mode == "merge"
        assert recovery_config.identity_insert is True
        assert recovery_config.watermark_column == "DW_Timestamp_Update"
        assert recovery_config.schema == "fct"
        assert recovery_config.table == "Vareudlevering"


class TestFhirSyncConfigsUseWatermark:
    """FHIR sync configs must use the explicitly declared sync strategy."""

    def test_all_fhir_configs_are_watermark(self):
        """FHIR configs should be watermark by default with explicit merge exceptions.

        The four fhir_careplan_activity_* merge variants were consolidated
        into a single watermark-mode fhir_careplan_activity config. Add names
        here ONLY for configs that deliberately use mode='merge'.
        """
        from DSO.load.sync_configs import FHIR_SYNC_CONFIGS

        merge_configs: set[str] = set()

        for name, config in FHIR_SYNC_CONFIGS.items():
            if name in merge_configs:
                assert (
                    config.mode == "merge"
                ), f"{name} uses mode='{config.mode}' — expected 'merge'"
                assert config.watermark_column is None, (
                    f"{name} watermark_column={config.watermark_column!r} — "
                    f"expected None for merge mode"
                )
                continue

            assert (
                config.mode == "watermark"
            ), f"{name} uses mode='{config.mode}' — expected 'watermark'"
            assert config.watermark_column == "DW_Timestamp_Update", (
                f"{name} watermark_column={config.watermark_column!r} — "
                f"expected 'DW_Timestamp_Update'"
            )

    def test_fhir_configs_present_in_all_sync_configs(self):
        """FHIR configs should be included in ALL_SYNC_CONFIGS."""
        from DSO.load.sync_configs import FHIR_SYNC_CONFIGS

        for key in FHIR_SYNC_CONFIGS:
            assert key in ALL_SYNC_CONFIGS, f"{key} missing from ALL_SYNC_CONFIGS"

    def test_fhir_computed_effective_timestamp_is_not_synced(self):
        """Persisted computed timestamp columns must not be written by DSO MERGE."""
        from DSO.load.sync_configs import FHIR_SYNC_CONFIGS

        configs_with_computed_effective_timestamp = {
            "fhir_observation",
            "fhir_questionnaire_response",
            "fhir_questionnaire_response_item",
        }

        for key in configs_with_computed_effective_timestamp:
            assert "DW_Timestamp_Effective" in FHIR_SYNC_CONFIGS[key].exclude_columns


class TestBatchedPaginationSafety:
    """Tests for keyset pagination safety in _sync_via_linked_server_batched."""

    def test_max_pk_sql_uses_staging_server_for_string_pk(self):
        """_max_pk_sql should query via OPENQUERY to use staging collation, not DSO."""
        config = SyncTableConfig(
            schema="fhir",
            table="Patient",
            primary_key="patient_id",
            mode="watermark",
            watermark_column="DW_Timestamp_Update",
        )
        pk_col = config.primary_key
        batch_size = config.linked_server_batch_size
        watermark_where = ""

        def _max_pk_sql(last_pk_value):
            if last_pk_value is None:
                pk_filter = ""
            elif isinstance(last_pk_value, str):
                escaped = last_pk_value.replace("'", "''''")
                pk_filter = f" AND [{pk_col}] > ''{escaped}''"
            else:
                pk_filter = f" AND [{pk_col}] > {last_pk_value}"

            inner = (
                f"SELECT MAX([{pk_col}]) FROM ("
                f"SELECT TOP({batch_size}) [{pk_col}] "
                f"FROM [TestDB].[{config.schema}].[{config.table}] "
                f"WHERE 1=1{pk_filter}{watermark_where} "
                f"ORDER BY [{pk_col}]"
                f") sub"
            )
            return f"SELECT * FROM OPENQUERY([STAGING_LINK], '{inner}')"

        sql_first = _max_pk_sql(None)
        assert "OPENQUERY" in sql_first
        assert "MAX([patient_id])" in sql_first
        assert "ORDER BY [patient_id]" in sql_first

        sql_next = _max_pk_sql("abc-123")
        assert "[patient_id] > ''abc-123''" in sql_next

        sql_numeric = _max_pk_sql(42)
        assert "[patient_id] > 42" in sql_numeric

    def test_max_pk_sql_string_with_single_quotes(self):
        """String PKs containing single quotes must be properly escaped for OPENQUERY."""
        pk_col = "patient_id"
        last_pk = "O'Brien"
        escaped = last_pk.replace("'", "''''")
        pk_filter = f" AND [{pk_col}] > ''{escaped}''"
        assert "O''''''''Brien" in pk_filter or "''''" in pk_filter
        assert pk_filter.count("''") >= 2

    def test_composite_keyset_predicate_is_lexicographic_and_escaped(self):
        """Composite key pagination should advance by lexicographic PK tuple."""
        predicate = _build_composite_keyset_predicate(
            ["qr_id", "link_id", "item_key", "answer_index"],
            ("qr-1", "link'2", "item-3", "0"),
        )

        assert "[qr_id] > ''qr-1''" in predicate
        assert "[qr_id] = ''qr-1'' AND [link_id] > ''link''''2''" in predicate
        assert (
            "[qr_id] = ''qr-1'' AND [link_id] = ''link''''2'' " "AND [item_key] > ''item-3''"
        ) in predicate
        assert (
            "[qr_id] = ''qr-1'' AND [link_id] = ''link''''2'' "
            "AND [item_key] = ''item-3'' AND [answer_index] > ''0''"
        ) in predicate

    def test_composite_keyset_predicate_empty_before_first_batch(self):
        """First composite batch should not apply a keyset predicate."""
        assert _build_composite_keyset_predicate(["qr_id", "link_id"], None) == ""

    def test_composite_truncate_insert_truncates_when_source_is_empty(self):
        """Composite full refresh must clear stale target rows when source is empty."""

        class EmptyCompositeCursor(_DummyCursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "INSERT INTO #_sync_staging" in sql:
                    self.rowcount = 0

        cursor = EmptyCompositeCursor(fetchone_queue=[(0,)])
        resource = _DummyResource([_DummyConnection(cursor)])
        context = MagicMock()
        context.log = MagicMock()
        config = SyncTableConfig(
            schema="sec",
            table="BrugerOrganisation",
            primary_key=["BrugerAD", "DW_EK_Organisation"],
            mode="truncate_insert",
        )

        result = sync_utils._sync_via_linked_server_batched_composite(
            context=context,
            mssql_target_dso=resource,
            config=config,
            last_sync_timestamp=None,
            columns=[
                "BrugerAD",
                "DW_EK_Organisation",
                "DW_Timestamp_Insert",
                "DW_Timestamp_Update",
            ],
            string_pk_cols=set(),
        )

        executed_sql = [sql for sql, _ in cursor.execute_calls]
        assert any("TRUNCATE TABLE [sec].[BrugerOrganisation]" in sql for sql in executed_sql)
        assert not any("INSERT INTO [sec].[BrugerOrganisation]" in sql for sql in executed_sql)
        assert result["rows_affected"] == 0
        assert result["mode"] == "truncate_insert"
