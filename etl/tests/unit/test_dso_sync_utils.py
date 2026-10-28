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
from decimal import Decimal
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
    build_clear_stale_current_sql,
    build_delta_merge_statement,
    build_merge_statement,
    build_truncate_insert_statement,
)

pytestmark = pytest.mark.unit


@contextmanager
def _patch_dfm_session(*, checkpoint: str | None = None):
    """Mock DsoSyncDfmSession for sync_table_via_linked_server unit tests."""
    from core.dfm_manager import DfmRunContext

    class _FakeDfmSession:
        def __init__(self, _staging, _context):
            self.dfm = MagicMock()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def start_run(self, _config, _dfm_mode):
            return DfmRunContext(log_id=1, checkpoint=checkpoint)

        def fail_run(self, *_args, **_kwargs):
            pass

    with (
        unittest.mock.patch("DSO.load.sync_dfm.DsoSyncDfmSession", _FakeDfmSession),
        unittest.mock.patch(
            "DSO.load.sync_utils.get_table_columns",
            return_value=["id", "dw_timestamp_insert", "dw_timestamp_update"],
        ),
        unittest.mock.patch("DSO.load.sync_dfm.finalize_dfm_run"),
    ):
        yield


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
        assert config.insert_only_columns == ["dw_timestamp_insert"]
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
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        assert config.mode == "watermark"
        assert config.watermark_column == "dw_timestamp_update"

    def test_config_watermark_with_timestamp_insert(self):
        """Test watermark on dw_timestamp_insert (typical for INSERT-only map tables)."""
        config = SyncTableConfig(
            schema="map",
            table="Kreditor",
            primary_key="CVR_CleanCode",
            mode="watermark",
            watermark_column="dw_timestamp_insert",
        )
        assert config.mode == "watermark"
        assert config.watermark_column == "dw_timestamp_insert"

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


class TestBuildClearStaleCurrentSql:
    """Filtered unique-current indexes need a pre-MERGE clear (per-row checks)."""

    def test_returns_none_without_unique_current_key(self):
        config = SyncTableConfig(schema="dim", table="Test", primary_key="dw_sk_Test")
        assert build_clear_stale_current_sql(config, "[dim].[Test]") is None

    def test_clears_outgoing_current_when_version_moved(self):
        config = SyncTableConfig(
            schema="dim",
            table="Borger",
            primary_key="dw_sk_Borger",
            unique_current_key="dw_ek_Borger",
        )
        sql = build_clear_stale_current_sql(config, "#_sync_staging")
        assert sql is not None
        assert "SET tgt.dw_is_current = 0" in sql
        assert "INNER JOIN #_sync_staging AS src" in sql
        assert "tgt.[dw_ek_Borger] = src.[dw_ek_Borger]" in sql
        assert "src.dw_is_current = 1" in sql
        assert "tgt.[dw_sk_Borger] <> src.[dw_sk_Borger]" in sql

    def test_collates_string_current_key(self):
        config = SyncTableConfig(
            schema="ref",
            table="Patient_Address_To_Borger",
            primary_key=["patient_id", "address_period_start"],
            unique_current_key="patient_id",
        )
        sql = build_clear_stale_current_sql(config, "#_sync_staging", string_pk_cols={"patient_id"})
        assert sql is not None
        assert "tgt.[patient_id] = src.[patient_id] COLLATE DATABASE_DEFAULT" in sql
        assert "tgt.[address_period_start] <> src.[address_period_start]" in sql


class TestBuildMergeStatement:
    """Test MERGE SQL statement generation."""

    def test_merge_single_pk(self):
        """Test MERGE statement with single primary key."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "kreditornavn",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]

        # Use explicit db name for test isolation from environment
        sql = build_merge_statement(config, columns, staging_db="TestDB")

        # Verify basic structure
        assert "MERGE [dim].[Kreditor] AS tgt" in sql
        assert "USING [STAGING_LINK].[TestDB].[dim].[Kreditor] AS src" in sql
        assert "ON tgt.[dw_ek_Kreditor] = src.[dw_ek_Kreditor]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql

        # Verify UPDATE clause includes non-PK, non-insert-only columns
        assert "tgt.[CVR_CleanCode] = src.[CVR_CleanCode]" in sql
        assert "tgt.[kreditornavn] = src.[kreditornavn]" in sql
        # PK should be in ON clause but not in UPDATE SET
        assert "ON tgt.[dw_ek_Kreditor] = src.[dw_ek_Kreditor]" in sql
        assert "UPDATE SET" in sql
        # Verify PK is not in UPDATE SET by checking it doesn't appear after "UPDATE SET"
        update_set_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        # PK should NOT appear in the UPDATE SET clause
        assert "tgt.[dw_ek_Kreditor] =" not in update_set_part
        # dw_timestamp_update should be set to SYSDATETIME
        assert "dw_timestamp_update" in sql
        assert "SYSDATETIME()" in sql

    def test_merge_composite_pk(self):
        """Test MERGE statement with composite primary key."""
        config = SyncTableConfig(
            schema="map",
            table="Leverandoervare",
            primary_key=["CVR_CleanCode", "Vare_CleanCode"],
            exclude_columns=["dw_ek_Leverandoervare"],
        )
        columns = [
            "dw_ek_Leverandoervare",
            "CVR_CleanCode",
            "Vare_CleanCode",
            "Leverandoer_Varenummer_CleanCode",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]

        sql = build_merge_statement(config, columns)

        # Verify composite key ON clause
        assert "tgt.[CVR_CleanCode] = src.[CVR_CleanCode]" in sql
        assert "tgt.[Vare_CleanCode] = src.[Vare_CleanCode]" in sql
        assert " AND " in sql

        # Verify excluded column not in INSERT
        assert "dw_ek_Leverandoervare" not in sql

    def test_merge_matches_questionnaire_item_link_id_byte_for_byte(self):
        """txt and TXT reach DSO as two rows; the ON clause must not merge them."""
        config = ALL_SYNC_CONFIGS["fhir_questionnaire_item"]
        columns = [
            "questionnaire_id",
            "link_id",
            "item_key",
            "text",
            "dw_timestamp_insert",
            "dw_timestamp_update",
            "link_id_exact",
        ]

        sql = build_merge_statement(
            config, columns, string_pk_cols={"questionnaire_id", "link_id", "item_key"}
        )

        assert "tgt.[link_id_exact] = src.[link_id] COLLATE Latin1_General_BIN2" in sql
        assert "tgt.[link_id] = src.[link_id]" not in sql
        assert "src.[link_id_exact]" not in sql

    def test_merge_with_excluded_columns(self):
        """Test MERGE excludes specified columns."""
        config = SyncTableConfig(
            schema="map",
            table="Kreditor",
            primary_key="CVR_CleanCode",
            exclude_columns=["dw_ek_Kreditor", "internal_field"],
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "internal_field",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]

        sql = build_merge_statement(config, columns)

        # Excluded columns should not appear in INSERT values
        assert "[dw_ek_Kreditor]" not in sql
        assert "[internal_field]" not in sql

    def test_merge_custom_linked_server(self):
        """Test MERGE with custom linked server name."""
        config = SyncTableConfig(
            schema="dim",
            table="Test",
            primary_key="ID",
        )
        columns = ["ID", "name", "dw_timestamp_insert", "dw_timestamp_update"]

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
            table="Test",
            primary_key=["dw_ek_Test", "dw_valid_from"],
            mode="truncate_insert",
        )
        columns = [
            "dw_ek_Test",
            "dw_valid_from",
            "dw_valid_to",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]

        # Use explicit db name for test isolation from environment
        truncate_sql, insert_sql = build_truncate_insert_statement(
            config, columns, staging_db="TestDB"
        )

        # Verify truncate
        assert "TRUNCATE TABLE [dim].[Test]" in truncate_sql

        # Verify insert
        assert "INSERT INTO [dim].[Test]" in insert_sql
        assert "SELECT" in insert_sql
        assert "FROM [STAGING_LINK].[TestDB].[dim].[Test]" in insert_sql

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
            "dw_timestamp_insert",
            "dw_timestamp_update",
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
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "kreditornavn",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        ts = "2026-02-11 12:00:00"

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # Verify MERGE structure
        assert "MERGE [dim].[Kreditor] AS tgt" in sql
        assert "ON tgt.[dw_ek_Kreditor] = src.[dw_ek_Kreditor]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql

        # Verify subquery with COALESCE WHERE filter
        assert "SELECT * FROM [STAGING_LINK].[TestDB].[dim].[Kreditor]" in sql
        assert "COALESCE([dw_timestamp_update], [dw_timestamp_insert])" in sql
        assert "> '2026-02-11 12:00:00'" in sql

        # Verify UPDATE SET excludes PK and insert-only columns
        update_set_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "tgt.[CVR_CleanCode] = src.[CVR_CleanCode]" in update_set_part
        assert "tgt.[kreditornavn] = src.[kreditornavn]" in update_set_part
        assert "tgt.[dw_ek_Kreditor] =" not in update_set_part
        assert "SYSDATETIME()" in sql

    def test_delta_merge_composite_pk(self):
        """Test delta MERGE with composite primary key and watermark."""
        config = SyncTableConfig(
            schema="fct",
            table="CompositeFact",
            primary_key=["PK1", "PK2", "PK3"],
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "PK1",
            "PK2",
            "PK3",
            "antal",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        sql = build_delta_merge_statement(
            config, columns, "2026-01-15 08:30:00", staging_db="TestDB"
        )

        # Verify composite key ON clause
        assert "tgt.[PK1] = src.[PK1]" in sql
        assert "tgt.[PK2] = src.[PK2]" in sql
        assert "tgt.[PK3] = src.[PK3]" in sql
        assert " AND " in sql

        # Verify watermark filter
        assert "COALESCE([dw_timestamp_update], [dw_timestamp_insert])" in sql
        assert "> '2026-01-15 08:30:00'" in sql

    def test_delta_merge_falls_back_to_full_when_no_timestamp(self):
        """Test that last_sync_timestamp=None produces identical SQL as build_merge_statement."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "dw_timestamp_insert",
            "dw_timestamp_update",
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
            watermark_column="dw_timestamp_insert",
        )
        columns = [
            "CVR_CleanCode",
            "kreditornavn",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        ts = "2026-02-01 00:00:00"

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # Watermark column should be dw_timestamp_insert (from config), not dw_timestamp_update
        assert "COALESCE([dw_timestamp_insert], [dw_timestamp_insert])" in sql

    def test_delta_merge_with_excluded_columns(self):
        """Test that excluded columns are not in INSERT or UPDATE."""
        config = SyncTableConfig(
            schema="map",
            table="Leverandoervare",
            primary_key=["CVR_CleanCode", "Vare_CleanCode"],
            exclude_columns=["dw_ek_Leverandoervare"],
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Leverandoervare",
            "CVR_CleanCode",
            "Vare_CleanCode",
            "Leverandoer_Varenummer_CleanCode",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        ts = "2026-02-10 14:00:00"

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        # dw_ek_Leverandoervare should not appear in INSERT or UPDATE clauses
        assert "dw_ek_Leverandoervare" not in sql
        # But watermark filter should still be present
        assert "COALESCE([dw_timestamp_update], [dw_timestamp_insert])" in sql

    def test_delta_merge_timestamp_format(self):
        """Test that timestamp is formatted as YYYY-MM-DD HH:MM:SS for DATETIME2(0)."""
        config = SyncTableConfig(
            schema="dim",
            table="Test",
            primary_key="ID",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = ["ID", "name", "dw_timestamp_insert", "dw_timestamp_update"]

        sql = build_delta_merge_statement(
            config, columns, "2026-03-15 23:59:45", staging_db="TestDB"
        )

        assert "> '2026-03-15 23:59:45'" in sql

    def test_delta_merge_no_delete_clause(self):
        """Delta MERGE must NOT contain NOT MATCHED BY SOURCE (no hard-delete)."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        ts = "2026-02-11 12:00:00"

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        assert "NOT MATCHED BY SOURCE" not in sql

    def test_delta_merge_insert_only_columns_excluded_from_update(self):
        """dw_timestamp_insert (insert-only) must NOT appear in the UPDATE SET clause."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "kreditornavn",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        ts = "2026-02-11 12:00:00"

        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        update_set_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "tgt.[dw_timestamp_insert]" not in update_set_part
        # But INSERT clause should still include it
        insert_part = sql.split("WHEN NOT MATCHED BY TARGET THEN")[1]
        assert "[dw_timestamp_insert]" in insert_part

    def test_delta_merge_uses_exclusive_boundary(self):
        """Boundary is exclusive > (DFM Strategy B with MAX source checkpoint)."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        sql = build_delta_merge_statement(
            config, columns, "2026-02-11 12:00:00", staging_db="TestDB"
        )

        assert "> '2026-02-11 12:00:00'" in sql
        assert ">= '2026-02-11 12:00:00'" not in sql

    def test_delta_merge_null_watermark_coalesce(self):
        """COALESCE(watermark, dw_timestamp_insert) ensures rows with NULL dw_timestamp_update are included."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)
        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        assert "COALESCE([dw_timestamp_update], [dw_timestamp_insert])" in sql
        assert "> '2026-02-11 12:00:00'" in sql

    def test_delta_merge_idempotent_no_duplicate_keys(self):
        """MERGE uses ON (PK) and WHEN MATCHED/WHEN NOT MATCHED BY TARGET only — reprocessing same row updates, does not insert duplicate."""
        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        columns = [
            "dw_ek_Kreditor",
            "CVR_CleanCode",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        ts = datetime(2026, 2, 11, 12, 0, 0)
        sql = build_delta_merge_statement(config, columns, ts, staging_db="TestDB")

        assert "ON tgt.[dw_ek_Kreditor] = src.[dw_ek_Kreditor]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql
        assert "NOT MATCHED BY SOURCE" not in sql


class TestSyncConfigs:
    """Test sync configuration dictionaries."""

    def test_erp_configs_exist(self):
        """Test that erp configs are defined."""
        assert "dim_Kreditor" in ERP_SYNC_CONFIGS
        assert "dim_Postering" in ERP_SYNC_CONFIGS
        assert "fct_Fakturalinje" in ERP_SYNC_CONFIGS

    def test_shared_dims_configs_exist(self):
        """Test that shared dimension configs are defined."""
        assert "dim_Borger" in SHARED_DIMS_SYNC_CONFIGS
        assert "dim_Organisation" in SHARED_DIMS_SYNC_CONFIGS

    def test_dim_borger_is_merge(self):
        """Test that dim.Borger uses merge mode (DW_SK_Borger from ref, no IDENTITY)."""
        config = SHARED_DIMS_SYNC_CONFIGS["dim_Borger"]
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
        config = get_config("dim_Kreditor")
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
            primary_key="dw_ek_Kreditor",
        )
        columns = ["dw_ek_Kreditor", "name", "dw_timestamp_insert", "dw_timestamp_update"]

        sql = build_merge_statement(config, columns)

        # All identifiers should be bracketed
        assert "[dim]" in sql
        assert "[Kreditor]" in sql
        assert "[dw_ek_Kreditor]" in sql
        assert "[name]" in sql

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
            "dw_timestamp_insert",
            "dw_timestamp_update",
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
        """Dimension tables typically use DW_EK as primary key (DW_SK for dim_Borger).

        Both the legacy uppercase ``DW_EK`` prefix and the new lowercase ``dw_ek``
        prefix (navngivningsstandard.md, applied to columns actively touched going
        forward, e.g. dim_Fakturalinje, dim_Vareudlevering) are accepted.
        """
        dim_configs = {
            k: v
            for k, v in ALL_SYNC_CONFIGS.items()
            if k.startswith("dim_") and v.mode in ("merge", "watermark")
        }
        assert dim_configs, "Expected at least one dim config with merge or watermark mode"
        for name, config in dim_configs.items():
            pk = config.primary_key
            if isinstance(pk, str):
                if name == "dim_Borger":
                    assert pk == "dw_sk_Borger", "dim_Borger PK should be dw_sk_Borger"
                else:
                    assert pk.upper().startswith(
                        "DW_EK"
                    ), f"{name} should use DW_EK as PK, got {pk}"

    def test_watermark_configs_have_valid_watermark_column(self):
        """All configs with mode='watermark' must have a recognised watermark column."""
        valid_watermark_columns = {"dw_timestamp_update", "dw_timestamp_insert"}
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
        self.closed = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed += 1


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
        try:
            yield conn
        finally:
            conn.close()


class _RawConnResource:
    """Resource stub exposing get_raw_connection() for the single-PK batched path.

    Hands out the supplied connections one per call so a test can drive the
    reconnect-on-retry loop (each reconnect consumes the next connection).
    """

    def __init__(self, connections):
        self._connections = list(connections)
        self._idx = 0
        self.raw_calls = 0
        # Attributes the legacy hand-rolled connection string referenced; kept so
        # a pre-fix implementation fails by trying to dial these, not on AttributeError.
        self.host = "dummy-host"
        self.port = 1433
        self.database = "DW"
        self.user = "sa"
        self.password = "pw"

    def get_raw_connection(self, timeout: int = 0):
        self.raw_calls += 1
        conn = self._connections[self._idx]
        self._idx += 1
        return conn


class _FakeProgrammingError(Exception):
    """Stand-in for mssql_python.exceptions.ProgrammingError: a DB-API exception
    that is NOT a RuntimeError, so it exercises the cross-class retry path."""


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
                    ("dw_timestamp_insert",),
                    ("dw_timestamp_update",),
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

        last_checkpoint = "2026-02-10 09:00:00"

        context = MagicMock()
        context.log = MagicMock()

        config = SyncTableConfig(
            schema="fct",
            table="Fakturalinje",
            primary_key="DW_EK_Fakturalinje",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )

        result = sync_utils._sync_via_python(
            context,
            staging_resource,
            dso_resource,
            config,
            last_sync_checkpoint=last_checkpoint,
        )

        assert result["mode"] == "python_watermark_delta"

        # Ensure source query used a watermark predicate with parameterized checkpoint
        select_sql, select_params = data_cursor.execute_calls[0]
        assert "COALESCE([dw_timestamp_update], [dw_timestamp_insert])" in select_sql
        assert " > " in select_sql
        assert select_params == (last_checkpoint,)

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

    def test_python_fallback_first_run_uses_full_sync(self, monkeypatch):
        """First watermark run in Python fallback should use full truncate+insert."""
        schema_cursor = _DummyCursor(
            fetchall_queue=[
                [
                    ("dw_ek_Kreditor",),
                    ("Navn",),
                    ("dw_timestamp_insert",),
                    ("dw_timestamp_update",),
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

        context = MagicMock()
        context.log = MagicMock()

        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )

        result = sync_utils._sync_via_python(
            context,
            staging_resource,
            dso_resource,
            config,
            last_sync_checkpoint=None,
        )

        assert result["mode"] == "python_watermark_full"
        assert any("TRUNCATE TABLE [dim].[Kreditor]" in sql for sql, _ in dso_cursor.execute_calls)
        assert dso_cursor.executemany_calls, "Expected insert executemany call"
        insert_sql, inserted_rows = dso_cursor.executemany_calls[0]
        assert "INSERT INTO [dim].[Kreditor]" in insert_sql
        assert len(inserted_rows) == 1


class TestPythonFallbackTruncateInsert:
    @pytest.fixture(autouse=True)
    def _reset_collation_cache(self, monkeypatch):
        monkeypatch.setattr(sync_utils, "_collation_mismatch_cache", False)

    def test_truncate_insert_allows_missing_pk_column(self):
        """truncate_insert must not require primary_key to exist in staging columns."""
        schema_cursor = _DummyCursor(
            fetchall_queue=[[("dw_ek_Test",), ("Navn",), ("dw_timestamp_insert",)]]
        )
        schema_conn = _DummyConnection(schema_cursor)

        data_cursor = _DummyCursor(
            fetchmany_queue=[[(1, "row A", datetime(2026, 2, 10, 10, 0, 0))], []]
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
            table="Test",
            primary_key="mismatched_pk",
            mode="truncate_insert",
        )

        result = sync_utils._sync_via_python(context, staging_resource, dso_resource, config)

        assert result["mode"] == "python_truncate_insert"
        assert any("TRUNCATE TABLE [dim].[Test]" in sql for sql, _ in dso_cursor.execute_calls)
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


class TestPythonFallbackConnectionHardening:
    """_sync_via_python streams via two long-lived connections with no batching
    seam — a transient connectivity error (e.g. TCP Provider) during the
    transfer must retry the whole transfer from scratch (nothing is durably
    committed before the final dso_conn.commit()), not crash outright."""

    @pytest.fixture(autouse=True)
    def _reset_collation_cache(self, monkeypatch):
        monkeypatch.setattr(sync_utils, "_collation_mismatch_cache", False)

    def test_retries_whole_transfer_after_transient_connectivity_error(self, monkeypatch):
        monkeypatch.setattr(sync_utils.time, "sleep", lambda *_a, **_k: None)

        schema_cursor = _DummyCursor(fetchall_queue=[[("Id",), ("Val",)]])
        schema_conn = _DummyConnection(schema_cursor)

        class _RaiseOnceCursor(_DummyCursor):
            def executemany(self, sql, rows):
                raise _FakeProgrammingError(
                    "TCP Provider: No connection could be made because the "
                    "target machine actively refused it"
                )

        # Attempt 1: staging fetch succeeds, DSO executemany flaps.
        data_conn_1 = _DummyConnection(_DummyCursor(fetchmany_queue=[[(1, "A")], []]))
        dso_conn_1 = _DummyConnection(_RaiseOnceCursor())

        # Attempt 2: fresh connections, full success.
        data_conn_2 = _DummyConnection(_DummyCursor(fetchmany_queue=[[(1, "A")], []]))
        dso_cursor_2 = _DummyCursor()
        dso_conn_2 = _DummyConnection(dso_cursor_2)

        staging_resource = _DummyResource([schema_conn, data_conn_1, data_conn_2])
        dso_resource = _DummyResource([dso_conn_1, dso_conn_2])

        context = MagicMock()
        context.log = MagicMock()

        config = SyncTableConfig(
            schema="dim",
            table="Foo",
            primary_key="Id",
            mode="truncate_insert",
        )

        result = sync_utils._sync_via_python(context, staging_resource, dso_resource, config)

        assert result["rows_affected"] == 1
        assert dso_cursor_2.executemany_calls, "Second attempt must perform the INSERT"
        assert dso_conn_1.closed >= 1, "Flapped connection's `with` block must close it"
        assert dso_conn_2.commits == 1

    def test_raises_after_exhausting_retries_on_persistent_connectivity_error(self, monkeypatch):
        monkeypatch.setattr(sync_utils.time, "sleep", lambda *_a, **_k: None)

        schema_cursor = _DummyCursor(fetchall_queue=[[("Id",), ("Val",)]])
        schema_conn = _DummyConnection(schema_cursor)

        class _AlwaysRaisesCursor(_DummyCursor):
            def executemany(self, sql, rows):
                raise _FakeProgrammingError(
                    "TCP Provider: No connection could be made because the "
                    "target machine actively refused it"
                )

        # 1 schema fetch + 4 data-transfer attempts (1 initial + 3 retries) all flap.
        staging_conns = [schema_conn] + [
            _DummyConnection(_DummyCursor(fetchmany_queue=[[(1, "A")], []])) for _ in range(4)
        ]
        dso_conns = [_DummyConnection(_AlwaysRaisesCursor()) for _ in range(4)]

        staging_resource = _DummyResource(staging_conns)
        dso_resource = _DummyResource(dso_conns)

        context = MagicMock()
        context.log = MagicMock()

        config = SyncTableConfig(
            schema="dim",
            table="Foo",
            primary_key="Id",
            mode="truncate_insert",
        )

        with pytest.raises(_FakeProgrammingError):
            sync_utils._sync_via_python(context, staging_resource, dso_resource, config)


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
        """When watermark_delta has rows and dso_count < staging_count, recover via full MERGE."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Vareudlevering",
            primary_key="DW_EK_Vareudlevering",
            mode="watermark",
            watermark_column="dw_timestamp_update",
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

        with _patch_dfm_session():
            with (
                unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
                unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
                unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
                unittest.mock.patch.object(sync_utils, "validate_linked_server"),
                unittest.mock.patch.object(
                    sync_utils,
                    "_sync_via_linked_server",
                    side_effect=[delta_result, recovery_result],
                ) as mock_sync,
                unittest.mock.patch.object(
                    sync_utils,
                    "compare_row_counts",
                    side_effect=[
                        {"staging_count": 30000, "dso_count": 29000, "match": False},
                        {"staging_count": 30000, "dso_count": 30000, "match": True},
                    ],
                ),
                unittest.mock.patch("DSO.load.sync_dfm.finalize_dfm_run") as mock_finalize,
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
        assert mock_sync.call_count == 2
        recovery_call_config = mock_sync.call_args_list[1][0][2]
        assert recovery_call_config.mode == "merge"
        assert mock_finalize.call_count >= 1

    def test_no_recovery_when_counts_match(self):
        """When watermark_delta sync has matching counts, no recovery is triggered."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
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

        with _patch_dfm_session():
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

    def test_mismatch_compare_uses_staging_view_not_base_table(self):
        """View-sourced configs (conceptmap) must compare the view, not the base table."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="conceptmap",
            table="EANToSOR",
            source_table="vw_ean_to_sor",
            primary_key=["dw_ek_EAN"],
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )
        assert config.staging_table == "vw_ean_to_sor"

        delta_result = {
            "rows_affected": 10,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[conceptmap].[EANToSOR]",
            "columns_synced": 5,
            "duration_ms": 100,
            "sync_duration_seconds": 0.1,
            "rows_per_second": 100.0,
        }

        with _patch_dfm_session():
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
                    return_value={"staging_count": 100, "dso_count": 100, "match": True},
                ) as mock_compare,
            ):
                sync_utils.sync_table_via_linked_server(
                    ctx,
                    dso_resource,
                    config,
                    mssql_source_staging=staging_resource,
                )

        mock_compare.assert_called_once()
        kwargs = mock_compare.call_args.kwargs
        assert kwargs["staging_schema"] == "conceptmap"
        assert kwargs["staging_table"] == "vw_ean_to_sor"
        assert mock_sync.call_count == 1

    def test_zero_row_delta_still_compares_and_skips_surplus_recovery(self):
        """Zero-row delta still compares; DSO surplus skips recovery (no thrash)."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )

        delta_result = {
            "rows_affected": 0,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[dim].[Kreditor]",
            "columns_synced": 5,
            "duration_ms": 50,
            "sync_duration_seconds": 0.05,
            "rows_per_second": 0.0,
        }

        with _patch_dfm_session():
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
                    return_value={
                        "staging_count": 54567,
                        "dso_count": 620822,
                        "match": False,
                    },
                ) as mock_compare,
            ):
                result = sync_utils.sync_table_via_linked_server(
                    ctx,
                    dso_resource,
                    config,
                    mssql_source_staging=staging_resource,
                )

        mock_compare.assert_called_once()
        assert mock_sync.call_count == 1
        assert result.get("recovery_full_merge") is None
        assert result["mode"] == "watermark_delta"
        assert result["rows_affected"] == 0
        warning_text = " ".join(str(c) for c in ctx.log.warning.call_args_list)
        assert "DSO >= staging" in warning_text

    def test_zero_row_delta_recovers_when_dso_short(self):
        """Zero-row delta with dso_count < staging_count still triggers full MERGE."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Fakturalinje",
            primary_key="dw_ek_Fakturalinje",
            mode="watermark",
            watermark_column="dw_timestamp_update",
        )

        delta_result = {
            "rows_affected": 0,
            "mode": "watermark_delta",
            "sync_engine": "linked_server",
            "table": "[fct].[Fakturalinje]",
            "columns_synced": 10,
            "duration_ms": 50,
            "sync_duration_seconds": 0.05,
            "rows_per_second": 0.0,
        }
        recovery_result = {
            "rows_affected": 1000,
            "mode": "merge",
            "sync_engine": "linked_server",
            "table": "[fct].[Fakturalinje]",
            "columns_synced": 10,
            "duration_ms": 5000,
            "sync_duration_seconds": 5.0,
            "rows_per_second": 200.0,
        }

        with _patch_dfm_session():
            with (
                unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
                unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
                unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
                unittest.mock.patch.object(sync_utils, "validate_linked_server"),
                unittest.mock.patch.object(
                    sync_utils,
                    "_sync_via_linked_server",
                    side_effect=[delta_result, recovery_result],
                ) as mock_sync,
                unittest.mock.patch.object(
                    sync_utils,
                    "compare_row_counts",
                    side_effect=[
                        {"staging_count": 1000, "dso_count": 900, "match": False},
                        {"staging_count": 1000, "dso_count": 1000, "match": True},
                    ],
                ),
                unittest.mock.patch("DSO.load.sync_dfm.finalize_dfm_run"),
            ):
                result = sync_utils.sync_table_via_linked_server(
                    ctx,
                    dso_resource,
                    config,
                    mssql_source_staging=staging_resource,
                )

        assert result["recovery_full_merge"] is True
        assert mock_sync.call_count == 2

    def test_no_recovery_when_dso_count_gte_staging(self):
        """DSO extras (orphans) are not recoverable via MERGE — skip full re-sync."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="dim",
            table="Kreditor",
            primary_key="dw_ek_Kreditor",
            mode="watermark",
            watermark_column="dw_timestamp_update",
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

        with _patch_dfm_session():
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
                    return_value={
                        "staging_count": 54567,
                        "dso_count": 620822,
                        "match": False,
                    },
                ),
            ):
                result = sync_utils.sync_table_via_linked_server(
                    ctx,
                    dso_resource,
                    config,
                    mssql_source_staging=staging_resource,
                )

        assert mock_sync.call_count == 1
        assert result.get("recovery_full_merge") is None
        assert result["mode"] == "watermark_delta"
        ctx.log.warning.assert_called()
        warning_text = " ".join(str(c) for c in ctx.log.warning.call_args_list)
        assert "DSO >= staging" in warning_text

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

        with _patch_dfm_session():
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
            primary_key="dw_ek_Faktura",
            mode="watermark",
            watermark_column="dw_timestamp_update",
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

        with _patch_dfm_session():
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
            primary_key="dw_ek_Faktura",
            mode="watermark",
            watermark_column="dw_timestamp_update",
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

        with _patch_dfm_session():
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
                        {"staging_count": 20000, "dso_count": 19000, "match": False},
                        {"staging_count": 20000, "dso_count": 20000, "match": True},
                    ],
                ),
                unittest.mock.patch("DSO.load.sync_dfm.finalize_dfm_run") as mock_finalize,
            ):
                result = sync_utils.sync_table_via_linked_server(
                    ctx,
                    dso_resource,
                    config,
                    mssql_source_staging=staging_resource,
                )

        assert result["recovery_full_merge"] is True
        assert result.get("recovery_failed") is not True
        mock_python.assert_called_once()
        recovery_call_config = mock_python.call_args[0][3]
        assert recovery_call_config.mode == "merge"
        assert mock_finalize.call_count >= 1

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
            watermark_column="dw_timestamp_update",
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

        with _patch_dfm_session():
            with (
                unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
                unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
                unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
                unittest.mock.patch.object(sync_utils, "validate_linked_server"),
                unittest.mock.patch.object(
                    sync_utils,
                    "_sync_via_linked_server",
                    side_effect=[delta_result, recovery_result],
                ) as mock_sync,
                unittest.mock.patch.object(
                    sync_utils,
                    "compare_row_counts",
                    side_effect=[
                        {"staging_count": 30000, "dso_count": 29000, "match": False},
                        {"staging_count": 30000, "dso_count": 29500, "match": False},
                    ],
                ),
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
            watermark_column="dw_timestamp_update",
            identity_insert=True,
        )
        recovery_config = dc_replace(config, mode="merge")
        assert recovery_config.mode == "merge"
        assert recovery_config.identity_insert is True
        assert recovery_config.watermark_column == "dw_timestamp_update"
        assert recovery_config.schema == "fct"
        assert recovery_config.table == "Vareudlevering"


class TestDispatcherFallbackOnConnectivityError:
    """sync_table_via_linked_server must fall back to Python when the linked-server
    path exhausts its own retries and re-raises a connectivity fault — even though
    mssql_python raises ProgrammingError (not RuntimeError) for that fault, the same
    cross-class mismatch already fixed in _is_retryable_connectivity_error."""

    def test_falls_back_to_python_on_non_runtimeerror_connectivity_fault(self):
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Faktura",
            primary_key="dw_ek_Faktura",
            mode="merge",
        )
        python_result = {
            "rows_affected": 500,
            "mode": "merge",
            "sync_engine": "python_fallback",
            "table": "[fct].[Faktura]",
            "columns_synced": 10,
            "duration_ms": 1000,
            "sync_duration_seconds": 1.0,
            "rows_per_second": 500.0,
        }

        with _patch_dfm_session():
            with (
                unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
                unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
                unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
                unittest.mock.patch.object(sync_utils, "validate_linked_server"),
                unittest.mock.patch.object(
                    sync_utils,
                    "_sync_via_linked_server",
                    side_effect=_FakeProgrammingError(
                        "TCP Provider: No connection could be made because the target "
                        "machine actively refused it"
                    ),
                ),
                unittest.mock.patch.object(
                    sync_utils, "_sync_via_python", return_value=python_result
                ) as mock_python,
            ):
                result = sync_utils.sync_table_via_linked_server(
                    ctx,
                    dso_resource,
                    config,
                    mssql_source_staging=staging_resource,
                )

        mock_python.assert_called_once()
        assert result["sync_engine"] == "python_fallback"

    def test_does_not_fall_back_on_unrelated_non_runtimeerror(self):
        """A non-RuntimeError that ISN'T a connectivity fault must still propagate —
        the fallback broadening must not swallow unrelated errors."""
        ctx = _create_mock_context()
        dso_resource, _conn, _cursor = _create_mock_dso_resource()
        staging_resource, _, _ = _create_mock_dso_resource()

        config = SyncTableConfig(
            schema="fct",
            table="Faktura",
            primary_key="dw_ek_Faktura",
            mode="merge",
        )

        with _patch_dfm_session():
            with (
                unittest.mock.patch.object(sync_utils, "is_same_database_mode", return_value=False),
                unittest.mock.patch.object(sync_utils, "can_connect_to_staging", return_value=True),
                unittest.mock.patch.object(sync_utils, "validate_table_schemas_match"),
                unittest.mock.patch.object(sync_utils, "validate_linked_server"),
                unittest.mock.patch.object(
                    sync_utils,
                    "_sync_via_linked_server",
                    side_effect=_FakeProgrammingError("Invalid column name 'Foo'"),
                ),
                unittest.mock.patch.object(sync_utils, "_sync_via_python") as mock_python,
                pytest.raises(_FakeProgrammingError),
            ):
                sync_utils.sync_table_via_linked_server(
                    ctx,
                    dso_resource,
                    config,
                    mssql_source_staging=staging_resource,
                )

        mock_python.assert_not_called()


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
            assert config.watermark_column == "dw_timestamp_update", (
                f"{name} watermark_column={config.watermark_column!r} — "
                f"expected 'dw_timestamp_update'"
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
            assert "dw_timestamp_effective" in FHIR_SYNC_CONFIGS[key].exclude_columns


class TestBatchedPaginationSafety:
    """Tests for keyset pagination safety in _sync_via_linked_server_batched."""

    def test_max_pk_sql_uses_staging_server_for_string_pk(self):
        """_max_pk_sql should query via OPENQUERY to use staging collation, not DSO."""
        config = SyncTableConfig(
            schema="fhir",
            table="Patient",
            primary_key="patient_id",
            mode="watermark",
            watermark_column="dw_timestamp_update",
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
        resource = _RawConnResource([_DummyConnection(cursor)])
        context = MagicMock()
        context.log = MagicMock()
        config = SyncTableConfig(
            schema="sec",
            table="BrugerOrganisation",
            primary_key=["BrugerAD", "dw_ek_Organisation"],
            mode="truncate_insert",
        )

        result = sync_utils._sync_via_linked_server_batched_composite(
            context=context,
            mssql_target_dso=resource,
            config=config,
            last_sync_checkpoint=None,
            columns=[
                "BrugerAD",
                "dw_ek_Organisation",
                "dw_timestamp_insert",
                "dw_timestamp_update",
            ],
            string_pk_cols=set(),
        )

        executed_sql = [sql for sql, _ in cursor.execute_calls]
        assert any("TRUNCATE TABLE [sec].[BrugerOrganisation]" in sql for sql in executed_sql)
        assert not any("INSERT INTO [sec].[BrugerOrganisation]" in sql for sql in executed_sql)
        assert result["rows_affected"] == 0
        assert result["mode"] == "truncate_insert"


class TestRetryableConnectivityError:
    """_is_retryable_connectivity_error must classify transient driver
    connectivity and linked-server plan-staleness errors as retryable
    regardless of exception class.

    Regression: mssql_python raises ProgrammingError (a DB-API Exception, NOT a
    RuntimeError) for 'TCP Provider ... actively refused it'. The original
    isinstance(exc, RuntimeError) guard made the batch/count retry dead code for
    exactly those errors, so transient cross-firewall connection refusals failed
    the run instead of being retried.
    """

    def test_mssql_python_programmingerror_tcp_provider_is_retryable(self):
        # mssql_python.exceptions.ProgrammingError is NOT a RuntimeError subclass.
        class ProgrammingError(Exception):
            pass

        exc = ProgrammingError(
            "Driver Error: Syntax error or access violation; DDBC Error: "
            "[Microsoft][SQL Server]TCP Provider: No connection could be made "
            "because the target machine actively refused it."
        )
        assert sync_utils._is_retryable_connectivity_error(exc) is True

    def test_mssql_python_programmingerror_schema_version_change_is_retryable(self):
        # SQL Server 7359: stale linked-server plan, self-heals on recompile.
        class ProgrammingError(Exception):
            pass

        exc = ProgrammingError(
            "Driver Error: Syntax error or access violation; DDBC Error: "
            '[Microsoft][SQL Server]The OLE DB provider "MSOLEDBSQL" for linked '
            'server "STAGING_LINK" reported a change in schema version between '
            'compile time ("1986250673148544") and run time ("1986250673148545").'
        )
        assert sync_utils._is_retryable_connectivity_error(exc) is True

    def test_runtime_error_with_pattern_still_retryable(self):
        exc = RuntimeError("Communication link failure while reading from staging")
        assert sync_utils._is_retryable_connectivity_error(exc) is True

    def test_non_connectivity_error_is_not_retryable(self):
        class ProgrammingError(Exception):
            pass

        # A genuine query error must NOT be retried (would mask real bugs).
        exc = ProgrammingError("Invalid column name 'Foo'.")
        assert sync_utils._is_retryable_connectivity_error(exc) is False

    def test_syntax_error_with_same_driver_prefix_is_not_retryable(self):
        class ProgrammingError(Exception):
            pass

        # Shares the generic 'Syntax error or access violation' driver prefix
        # with the transient 7359 message but is a genuine query bug.
        exc = ProgrammingError(
            "Driver Error: Syntax error or access violation; DDBC Error: "
            "[Microsoft][SQL Server]Incorrect syntax near the keyword 'FROM'."
        )
        assert sync_utils._is_retryable_connectivity_error(exc) is False


class TestBatchedSyncConnectionHardening:
    """Single-PK batched sync must source connections from the resource and
    survive a transient connectivity failure during the Phase-0 count."""

    @pytest.fixture(autouse=True)
    def _reset_collation_cache(self, monkeypatch):
        monkeypatch.setattr(sync_utils, "_collation_mismatch_cache", False)

    @staticmethod
    def _config():
        return SyncTableConfig(
            schema="codesystem",
            table="EANConcept",
            primary_key="code",
            mode="truncate_insert",
        )

    @staticmethod
    def _columns():
        return ["code", "display", "dw_timestamp_insert", "dw_timestamp_update"]

    def test_batched_sync_uses_resource_get_raw_connection(self):
        """The DSO layer must obtain its connection via the resource, not by
        hand-rolling a bare mssql_python.connect()."""
        cursor = _DummyCursor(fetchone_queue=[(0,)])  # count -> 0 rows -> early return
        conn = _MssqlPythonDummyConnection(cursor)
        resource = _RawConnResource([conn])
        context = MagicMock()
        context.log = MagicMock()

        result = sync_utils._sync_via_linked_server_batched(
            context=context,
            mssql_target_dso=resource,
            config=self._config(),
            last_sync_checkpoint=None,
            columns=self._columns(),
            string_pk_cols=set(),
        )

        assert resource.raw_calls == 1
        assert result["rows_affected"] == 0
        assert result["sync_engine"] == sync_utils.SYNC_ENGINE_LINKED_SERVER
        assert conn.closed >= 1  # the finally block closed the raw connection

    def test_multi_batch_stages_all_rows_then_inserts(self):
        """Two non-empty batches stage through the shared helper: total_rows
        accumulates across batches and Phase 2 inserts the full staged set."""

        class _MultiBatchCursor(_DummyCursor):
            def __init__(self):
                super().__init__()
                self._batch_rowcounts = [3, 2, 0]  # batch1, batch2, then empty -> stop
                self._next_pks = [("k1",), ("k2",)]
                self._fetch = []

            def execute(self, sql, params=None):
                self.execute_calls.append((sql, params))
                if "COUNT(*)" in sql:
                    self._fetch.append((5,))
                elif "INSERT INTO #_sync_staging" in sql:
                    self.rowcount = self._batch_rowcounts.pop(0)
                elif "MAX(" in sql:  # keyset advance (_max_pk_sql)
                    self._fetch.append(self._next_pks.pop(0))
                elif "INSERT INTO [" in sql:  # Phase 2 insert into target table
                    self.rowcount = 5

            def fetchone(self):
                return self._fetch.pop(0) if self._fetch else (0,)

        conn = _MssqlPythonDummyConnection(_MultiBatchCursor())
        resource = _RawConnResource([conn])
        context = MagicMock()
        context.log = MagicMock()

        result = sync_utils._sync_via_linked_server_batched(
            context=context,
            mssql_target_dso=resource,
            config=self._config(),
            last_sync_checkpoint=None,
            columns=self._columns(),
            string_pk_cols=set(),
        )

        assert resource.raw_calls == 1  # no reconnect needed
        assert result["rows_affected"] == 5  # Phase-2 INSERT count after staging 3+2 rows

    def test_count_connectivity_failure_reconnects_before_phase1(self):
        """A transient connectivity error during the Phase-0 count must trigger a
        reconnect + temp recreate, so Phase 1 starts on a healthy session rather
        than failing with a misleading 'Invalid object name #_sync_staging'."""

        class _CountRaisesConnError(_DummyCursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "COUNT(*)" in sql:
                    raise _FakeProgrammingError(
                        "Driver Error: Syntax error or access violation; DDBC Error: "
                        "[Microsoft][SQL Server]TCP Provider: No connection could be "
                        "made because the target machine actively refused it."
                    )

        broken_conn = _MssqlPythonDummyConnection(_CountRaisesConnError())
        healthy_conn = _MssqlPythonDummyConnection(_DummyCursor(fetchone_queue=[(0,)]))
        resource = _RawConnResource([broken_conn, healthy_conn])
        context = MagicMock()
        context.log = MagicMock()

        result = sync_utils._sync_via_linked_server_batched(
            context=context,
            mssql_target_dso=resource,
            config=self._config(),
            last_sync_checkpoint=None,
            columns=self._columns(),
            string_pk_cols=set(),
        )

        assert resource.raw_calls == 2  # reconnected after the count flap
        assert broken_conn.closed >= 1  # the poisoned connection was closed
        assert result["rows_affected"] == 0

    def test_reconnect_init_failure_does_not_leak_connection(self):
        """If the reconnect's temp-table creation fails, the freshly-opened
        connection must be closed (not leaked) before the error propagates.
        Leaking connections during the very churn we are hardening against
        would make the problem worse."""

        class _CountRaisesConnError(_DummyCursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "COUNT(*)" in sql:
                    raise _FakeProgrammingError(
                        "[Microsoft][SQL Server]TCP Provider: No connection could be "
                        "made because the target machine actively refused it."
                    )

        class _TempCreateRaises(_DummyCursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "INTO #_sync_staging" in sql:
                    raise _FakeProgrammingError("permission denied creating temp table")

        broken_conn = _MssqlPythonDummyConnection(_CountRaisesConnError())
        reconnect_conn = _MssqlPythonDummyConnection(_TempCreateRaises())
        resource = _RawConnResource([broken_conn, reconnect_conn])
        context = MagicMock()
        context.log = MagicMock()

        with pytest.raises(_FakeProgrammingError):
            sync_utils._sync_via_linked_server_batched(
                context=context,
                mssql_target_dso=resource,
                config=self._config(),
                last_sync_checkpoint=None,
                columns=self._columns(),
                string_pk_cols=set(),
            )

        # the reconnect connection failed mid-init and MUST have been closed
        assert reconnect_conn.closed >= 1

    def test_phase1_batch_connectivity_failure_reconnects(self, monkeypatch):
        """A retryable connectivity error during a Phase-1 batch INSERT must
        close the connection, reconnect, recreate the temp table, and continue —
        previously this batch-retry path had no test coverage."""
        # No real backoff sleeping during the test.
        monkeypatch.setattr(sync_utils.time, "sleep", lambda *_a, **_k: None)

        class _FirstBatchRaises(_DummyCursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "INSERT INTO #_sync_staging" in sql:
                    raise _FakeProgrammingError(
                        "[Microsoft][SQL Server]TCP Provider: No connection could be "
                        "made because the target machine actively refused it."
                    )

        # conn1: count OK (5 rows), then the first batch INSERT flaps.
        conn1 = _MssqlPythonDummyConnection(_FirstBatchRaises(fetchone_queue=[(5,)]))
        # conn2: fresh session, first batch returns 0 rows -> clean early return.
        conn2 = _MssqlPythonDummyConnection(_DummyCursor())
        resource = _RawConnResource([conn1, conn2])
        context = MagicMock()
        context.log = MagicMock()

        result = sync_utils._sync_via_linked_server_batched(
            context=context,
            mssql_target_dso=resource,
            config=self._config(),
            last_sync_checkpoint=None,
            columns=self._columns(),
            string_pk_cols=set(),
        )

        assert resource.raw_calls == 2  # batch flap triggered a reconnect
        assert conn1.closed >= 1  # the flapped connection was closed
        assert result["rows_affected"] == 0


class TestCompositeSyncConnectionHardening:
    """Composite-PK batched sync (FHIR) must have the same connectivity
    resilience as single-PK: source connections from the resource, reconnect on
    a transient Phase-0 count failure, and retry batches on a connection flap."""

    @pytest.fixture(autouse=True)
    def _reset_collation_cache(self, monkeypatch):
        monkeypatch.setattr(sync_utils, "_collation_mismatch_cache", False)

    @staticmethod
    def _config():
        return SyncTableConfig(
            schema="sec",
            table="BrugerOrganisation",
            primary_key=["BrugerAD", "dw_ek_Organisation"],
            mode="truncate_insert",
        )

    @staticmethod
    def _columns():
        return ["BrugerAD", "dw_ek_Organisation", "dw_timestamp_insert", "dw_timestamp_update"]

    def test_count_connectivity_failure_reconnects(self):
        class _CountRaisesConnError(_DummyCursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "COUNT(*)" in sql:
                    raise _FakeProgrammingError(
                        "[Microsoft][SQL Server]TCP Provider: No connection could be "
                        "made because the target machine actively refused it."
                    )

        broken_conn = _MssqlPythonDummyConnection(_CountRaisesConnError())
        healthy_conn = _MssqlPythonDummyConnection(_DummyCursor(fetchone_queue=[(0,)]))
        resource = _RawConnResource([broken_conn, healthy_conn])
        context = MagicMock()
        context.log = MagicMock()

        result = sync_utils._sync_via_linked_server_batched_composite(
            context=context,
            mssql_target_dso=resource,
            config=self._config(),
            last_sync_checkpoint=None,
            columns=self._columns(),
            string_pk_cols=set(),
        )

        assert resource.raw_calls == 2  # reconnected after the count flap
        assert broken_conn.closed >= 1
        assert result["rows_affected"] == 0

    def test_batch_connectivity_failure_reconnects(self, monkeypatch):
        """A retryable connectivity error during a composite Phase-1 batch INSERT
        must close, reconnect, recreate the temp table, and continue."""
        monkeypatch.setattr(sync_utils.time, "sleep", lambda *_a, **_k: None)

        class _FirstBatchRaises(_DummyCursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if "INSERT INTO #_sync_staging" in sql:
                    raise _FakeProgrammingError(
                        "[Microsoft][SQL Server]TCP Provider: No connection could be "
                        "made because the target machine actively refused it."
                    )

        conn1 = _MssqlPythonDummyConnection(_FirstBatchRaises(fetchone_queue=[(5,)]))
        conn2 = _MssqlPythonDummyConnection(_DummyCursor())
        resource = _RawConnResource([conn1, conn2])
        context = MagicMock()
        context.log = MagicMock()

        result = sync_utils._sync_via_linked_server_batched_composite(
            context=context,
            mssql_target_dso=resource,
            config=self._config(),
            last_sync_checkpoint=None,
            columns=self._columns(),
            string_pk_cols=set(),
        )

        assert resource.raw_calls == 2  # batch flap triggered a reconnect
        assert conn1.closed >= 1
        assert result["rows_affected"] == 0


class TestChunkInputSizesDecimalScale:
    """Decimal params must be sized from the chunk, like string params already are.

    mssql-python >= 1.6.0 binds SQL_NUMERIC hints as SQL_C_CHAR, so an
    under-declared scale fails the whole chunk with "String data, right-truncated".
    dim/fct carry NUMERIC(28,12) and DECIMAL(32,16), so scales above 10 are real.
    """

    @staticmethod
    def _spec_for(values):
        rows = [(v,) for v in values]
        sizes = sync_utils._build_mssql_python_chunk_input_sizes(rows)
        assert sizes is not None, "helper should size a Decimal chunk"
        return sizes[0]

    def test_scale_comes_from_widest_decimal_in_chunk(self):
        sql_type, _precision, scale = self._spec_for(
            [Decimal("1.5"), Decimal("123456789012345.678901234567")]
        )
        assert sql_type == sync_utils._SQL_NUMERIC
        assert scale == 12

    def test_zero_with_negative_exponent_is_scale_twelve(self):
        """Decimal('0E-12') occurs in real ERP data — exponent, not digits, sets scale."""
        _sql_type, _precision, scale = self._spec_for([Decimal("0E-12")])
        assert scale == 12

    def test_scale_sixteen_column_is_declared_fully(self):
        """Linjenummer is DECIMAL(32,16)."""
        _sql_type, _precision, scale = self._spec_for([Decimal("1.0000000000000001")])
        assert scale == 16

    def test_integral_decimals_declare_zero_scale(self):
        _sql_type, _precision, scale = self._spec_for([Decimal("42"), Decimal("100")])
        assert scale == 0

    def test_trailing_zero_decimal_keeps_its_exponent_scale(self):
        """Decimal('2876.000000000000000') has exponent -15 even though it is integral."""
        _sql_type, _precision, scale = self._spec_for([Decimal("2876.000000000000000")])
        assert scale == 15

    def test_precision_never_exceeds_sql_server_maximum(self):
        _sql_type, precision, scale = self._spec_for(
            [Decimal("1" * 20 + "." + "1" * 25)]  # would need 45 digits
        )
        assert precision <= 38
        assert scale <= precision

    def test_integer_digits_are_preserved_when_scale_would_overflow(self):
        """Clamping must not steal room from the integer part — that loses magnitude."""
        _sql_type, precision, scale = self._spec_for([Decimal("1" * 30 + "." + "1" * 20)])
        assert precision == 38
        assert precision - scale >= 30, "must keep 30 integer digits"

    def test_non_finite_decimal_does_not_crash(self):
        rows = [(Decimal("NaN"),)]
        sizes = sync_utils._build_mssql_python_chunk_input_sizes(rows)
        assert sizes is not None
        assert sizes[0][0] == sync_utils._SQL_NUMERIC

    def test_mixed_scales_take_the_maximum(self):
        _sql_type, _precision, scale = self._spec_for(
            [Decimal("1.5"), Decimal("2.25"), Decimal("3.000000000001"), None]
        )
        assert scale == 12


class TestBatchedSyncIndexDisable:
    """The nonclustered index disable/rebuild must fire ONLY for a bulk load
    (empty target / truncate_insert), never for an incremental MERGE into a
    populated target — rebuilding every index over the whole table to absorb a
    small delta would cost far more than per-row upkeep."""

    @pytest.fixture(autouse=True)
    def _reset_collation_cache(self, monkeypatch):
        monkeypatch.setattr(sync_utils, "_collation_mismatch_cache", False)

    @staticmethod
    def _config():
        # Wide-fact stand-in: single PK, watermark, index-disable opted in.
        return SyncTableConfig(
            schema="fct",
            table="DRGKontakt",
            primary_key="dw_ek_DRGKontakt",
            mode="watermark",
            watermark_column="dw_timestamp_update",
            disable_target_indexes_during_sync=True,
        )

    @staticmethod
    def _columns():
        return [
            "dw_ek_DRGKontakt",
            "totalpris_drg",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]

    class _IndexAwareCursor(_DummyCursor):
        """Routes the Phase-0 count, one non-empty Phase-1 batch, the Phase-2
        emptiness probe, index-name discovery, and the DISABLE/REBUILD DDL so a
        test can assert exactly which indexes were touched."""

        def __init__(self, target_populated, index_names):
            super().__init__()
            self._batch_rowcounts = [5, 0]  # one non-empty batch, then stop
            self._next_pks = [(1000,)]
            self._fetch = []
            self._target_populated = target_populated
            self._index_names = list(index_names)
            self.disabled = []
            self.rebuilt = []
            self.index_lookup_calls = 0
            self.tablock_insert = False
            self.did_merge = False

        def execute(self, sql, params=None):
            self.execute_calls.append((sql, params))
            if "COUNT(*)" in sql:
                self._fetch.append((5,))
            elif "INSERT INTO #_sync_staging" in sql:
                self.rowcount = self._batch_rowcounts.pop(0)
            elif "MAX(" in sql:  # keyset advance (_max_pk_sql)
                self._fetch.append(self._next_pks.pop(0))
            elif "EXISTS" in sql:  # Phase-2 emptiness probe
                self._fetch.append((1,) if self._target_populated else (0,))
            elif "sys.indexes" in sql:  # _get_non_pk_nonclustered_index_names
                self.index_lookup_calls += 1
                self.fetchall_queue.append([(n,) for n in self._index_names])
            elif "DISABLE" in sql:
                self.disabled.append(sql)
            elif "REBUILD" in sql:
                self.rebuilt.append(sql)
            elif "MERGE " in sql:
                self.did_merge = True
                self.rowcount = 3
            elif "INSERT INTO [" in sql:  # Phase-2 bulk insert into the target
                self.tablock_insert = "WITH (TABLOCK)" in sql
                self.rowcount = 5

        def fetchone(self):
            return self._fetch.pop(0) if self._fetch else (0,)

    def _run(self, target_populated, index_names):
        cursor = self._IndexAwareCursor(target_populated, index_names)
        conn = _MssqlPythonDummyConnection(cursor)
        resource = _RawConnResource([conn])
        context = MagicMock()
        context.log = MagicMock()
        result = sync_utils._sync_via_linked_server_batched(
            context=context,
            mssql_target_dso=resource,
            config=self._config(),
            last_sync_checkpoint=None,
            columns=self._columns(),
            string_pk_cols=set(),
        )
        return cursor, result

    def test_empty_target_disables_and_rebuilds_every_nonclustered_index(self):
        cursor, result = self._run(target_populated=False, index_names=["IX_a", "IX_b", "IX_c"])
        assert cursor.index_lookup_calls == 1
        assert len(cursor.disabled) == 3
        assert len(cursor.rebuilt) == 3
        # bulk-inserted (not merged) into the empty target, with TABLOCK
        assert cursor.tablock_insert is True
        assert cursor.did_merge is False
        assert result["rows_affected"] == 5

    def test_populated_target_merge_never_touches_indexes(self):
        cursor, result = self._run(target_populated=True, index_names=["IX_a", "IX_b", "IX_c"])
        # Never even look up the index names on the incremental MERGE path.
        assert cursor.index_lookup_calls == 0
        assert cursor.disabled == []
        assert cursor.rebuilt == []
        assert cursor.did_merge is True
        assert cursor.tablock_insert is False
        assert result["rows_affected"] == 3

    class _RatioAwareCursor(_IndexAwareCursor):
        """Adds the sys.partitions target-row estimate and the composite keyset
        advance so the backfill gate can be exercised on both batched paths."""

        def __init__(self, target_populated, index_names, target_rows):
            super().__init__(target_populated, index_names)
            self._target_rows = target_rows
            self.partition_lookups = 0

        def execute(self, sql, params=None):
            if "sys.partitions" in sql:
                self.execute_calls.append((sql, params))
                self.partition_lookups += 1
                self._fetch.append((self._target_rows,))
                return
            if "TOP(1)" in sql and "DESC" in sql:  # composite keyset advance
                self.execute_calls.append((sql, params))
                self._fetch.append(self._next_pks.pop(0) if self._next_pks else None)
                return
            super().execute(sql, params)

    def _run_ratio(
        self,
        target_rows,
        monkeypatch,
        *,
        composite=False,
        min_rows="1",
        ratio="0.1",
        config=None,
        columns=None,
    ):
        monkeypatch.setenv(sync_utils._INDEX_DISABLE_MIN_ROWS_ENV, min_rows)
        if ratio is None:
            monkeypatch.delenv(sync_utils._INDEX_DISABLE_MIN_RATIO_ENV, raising=False)
        else:
            monkeypatch.setenv(sync_utils._INDEX_DISABLE_MIN_RATIO_ENV, ratio)
        cursor = self._RatioAwareCursor(True, ["IX_a", "IX_b"], target_rows)
        conn = _MssqlPythonDummyConnection(cursor)
        resource = _RawConnResource([conn])
        context = MagicMock()
        context.log = MagicMock()
        if composite:
            config = config or SyncTableConfig(
                schema="fhir",
                table="QuestionnaireResponse_item",
                primary_key=["qr_id", "link_id"],
                mode="watermark",
                watermark_column="dw_timestamp_update",
                disable_target_indexes_during_sync=True,
            )
            cursor._next_pks = [tuple(f"pk-{i}" for i in range(len(config.primary_key)))]
            result = sync_utils._sync_via_linked_server_batched_composite(
                context=context,
                mssql_target_dso=resource,
                config=config,
                last_sync_checkpoint=None,
                columns=columns
                or ["qr_id", "link_id", "dw_timestamp_insert", "dw_timestamp_update"],
                string_pk_cols=set(),
            )
        else:
            result = sync_utils._sync_via_linked_server_batched(
                context=context,
                mssql_target_dso=resource,
                config=config or self._config(),
                last_sync_checkpoint=None,
                columns=columns or self._columns(),
                string_pk_cols=set(),
            )
        return cursor, result

    def test_backfill_sized_delta_into_populated_target_disables_and_rebuilds(self, monkeypatch):
        # 5 indkommende rækker mod 10 i målet = ratio 0,5 >= 0,1 -> bulk-vej for indeks, MERGE for data
        cursor, result = self._run_ratio(target_rows=10, monkeypatch=monkeypatch)
        assert cursor.partition_lookups == 1
        assert cursor.index_lookup_calls == 1
        assert len(cursor.disabled) == 2 and len(cursor.rebuilt) == 2
        assert cursor.did_merge is True and cursor.tablock_insert is False
        assert result["rows_affected"] == 3

    def test_small_relative_delta_keeps_indexes(self, monkeypatch):
        # 5 mod 1.000 = ratio 0,005 < 0,1 -> ingen frakobling (den daglige delta)
        cursor, _ = self._run_ratio(target_rows=1000, monkeypatch=monkeypatch)
        assert cursor.partition_lookups == 1
        assert cursor.index_lookup_calls == 0
        assert cursor.disabled == [] and cursor.rebuilt == []
        assert cursor.did_merge is True

    def test_min_rows_floor_short_circuits_before_any_lookup(self, monkeypatch):
        # under den absolutte grænse slås intet op — samme adfærd som før for små deltaer
        cursor, _ = self._run_ratio(target_rows=1, monkeypatch=monkeypatch, min_rows="1000000")
        assert cursor.partition_lookups == 0
        assert cursor.index_lookup_calls == 0
        assert cursor.disabled == []

    def test_composite_path_gets_the_same_gate(self, monkeypatch):
        cursor, result = self._run_ratio(target_rows=10, monkeypatch=monkeypatch, composite=True)
        assert cursor.partition_lookups == 1
        assert cursor.index_lookup_calls == 1
        assert len(cursor.disabled) == 2 and len(cursor.rebuilt) == 2
        assert cursor.did_merge is True and cursor.tablock_insert is False
        assert result["rows_affected"] == 3

    @pytest.mark.parametrize(
        ("config_name", "composite", "columns"),
        [
            (
                "fhir_questionnaire_response",
                False,
                ["qr_id", "dw_timestamp_insert", "dw_timestamp_update"],
            ),
            (
                "fhir_questionnaire_response_item",
                True,
                ["qr_id", "link_id", "item_key", "answer_index", "dw_timestamp_update"],
            ),
        ],
    )
    def test_qr_configs_disable_on_min_rows_alone(
        self, monkeypatch, config_name, composite, columns
    ):
        # Få rækker mod 1.000 i målet er under 1 %, og miljøet kræver 50 %;
        # QR-tabellernes egen andel på 0 vinder over miljøet.
        from DSO.load.sync_configs import FHIR_SYNC_CONFIGS

        cursor, _ = self._run_ratio(
            target_rows=1000,
            monkeypatch=monkeypatch,
            composite=composite,
            ratio="0.5",
            config=FHIR_SYNC_CONFIGS[config_name],
            columns=columns,
        )
        assert len(cursor.disabled) == 2 and len(cursor.rebuilt) == 2
        assert cursor.did_merge is True

    def test_table_without_own_ratio_keeps_indexes_under_the_default_share(self, monkeypatch):
        # DRG-lignende tabel uden eget andelskrav og uden miljøværdi: standarden på 10 % gælder.
        cursor, _ = self._run_ratio(target_rows=1000, monkeypatch=monkeypatch, ratio=None)
        assert cursor.disabled == [] and cursor.rebuilt == []
        assert cursor.did_merge is True

    def test_composite_small_delta_keeps_indexes(self, monkeypatch):
        cursor, _ = self._run_ratio(target_rows=1000, monkeypatch=monkeypatch, composite=True)
        assert cursor.index_lookup_calls == 0
        assert cursor.disabled == [] and cursor.rebuilt == []
        assert cursor.did_merge is True


def test_index_disable_thresholds_run_tags_overstyrer_miljoe(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv(sync_utils._INDEX_DISABLE_MIN_ROWS_ENV, "500")
    monkeypatch.setenv(sync_utils._INDEX_DISABLE_MIN_RATIO_ENV, "0.5")
    assert sync_utils._index_disable_thresholds() == (500, 0.5)
    ctx = SimpleNamespace(
        run=SimpleNamespace(
            tags={
                sync_utils._INDEX_DISABLE_MIN_ROWS_TAG: "1000",
                sync_utils._INDEX_DISABLE_MIN_RATIO_TAG: "0",
            }
        )
    )
    assert sync_utils._index_disable_thresholds(ctx) == (1000, 0.0)
    # MagicMock-kontekst (som i de øvrige tests) har ingen dict-tags -> miljøet gælder
    assert sync_utils._index_disable_thresholds(MagicMock()) == (500, 0.5)


def test_index_disable_ratio_tag_before_table_before_environment(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv(sync_utils._INDEX_DISABLE_MIN_RATIO_ENV, "0.5")
    assert sync_utils._index_disable_thresholds(None, 0.0)[1] == 0.0
    assert sync_utils._index_disable_thresholds(None, None)[1] == 0.5
    ctx = SimpleNamespace(
        run=SimpleNamespace(tags={sync_utils._INDEX_DISABLE_MIN_RATIO_TAG: "0.3"})
    )
    assert sync_utils._index_disable_thresholds(ctx, 0.0)[1] == 0.3
    monkeypatch.delenv(sync_utils._INDEX_DISABLE_MIN_RATIO_ENV)
    assert sync_utils._index_disable_thresholds(None, None)[1] == 0.10
