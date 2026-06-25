# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for erp partition logic.

Tests validate:
- build_partition_where_clause generates correct WHERE clauses for different table types
- build_dependent_table_partition_clause generates correct FK-based filters
- TableConfig fields (table_type, parent_table, is_string_date) work correctly
- Master data tables always return None or custom_where_clause (no partition filtering)
"""

import pytest
from core.partitions import (
    PARTITION_START_KEYS,
    daily_partitions,
    daily_partitions_erp,
    is_first_partition,
    monthly_partitions,
    monthly_partitions_erp,
)
from DSI.connectors.mssql.models import (
    TABLE_TYPE_BRIDGE,
    TABLE_TYPE_DEPENDENT,
    TABLE_TYPE_MASTER_DATA,
    TABLE_TYPE_MASTER_DATA_PARTITIONED,
    TABLE_TYPE_TRANSACTIONAL,
    TableConfig,
)
from DSI.connectors.mssql.sql_builders import (
    build_partition_where_clause,
)
from DSI.connectors.mssql.sql_builders import (
    build_relation_where_clause as build_dependent_table_partition_clause,
)
from DSI.connectors.mssql.sql_builders import (
    get_month_date_range,
    is_monthly_partition,
)


class TestBuildPartitionWhereClause:
    """Tests for build_partition_where_clause function."""

    def test_master_data_returns_none_when_no_custom_where(self):
        """Master data tables should return None when no custom_where_clause."""
        config = TableConfig(
            output_name="test_master",
            source_schema="dbo",
            source_table="MasterTable",
            target_table="master_table",
            description="Test master table",
            table_type=TABLE_TYPE_MASTER_DATA,
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        assert result is None

    def test_master_data_returns_custom_where_when_present(self):
        """Master data tables should return custom_where_clause if present."""
        config = TableConfig(
            output_name="test_master",
            source_schema="dbo",
            source_table="MasterTable",
            target_table="master_table",
            description="Test master table",
            table_type=TABLE_TYPE_MASTER_DATA,
            custom_where_clause="Active = 1",
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        assert result == "Active = 1"

    def test_is_full_load_returns_custom_where(self):
        """Tables with is_full_load=True should return custom_where_clause."""
        config = TableConfig(
            output_name="test_full_load",
            source_schema="dbo",
            source_table="FullLoadTable",
            target_table="full_load_table",
            description="Test full load table",
            is_full_load=True,
            custom_where_clause="Status = 'ACTIVE'",
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        assert result == "Status = 'ACTIVE'"

    def test_transactional_sydax_datetime_partition(self):
        """Transactional SydAX tables with datetime partition column."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        assert result == "CAST([Oprettelsestidspunkt] AS DATE) = '2025-01-15'"

    def test_transactional_mysupply_string_date_partition(self):
        """Transactional MySupply tables with string date partition column."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="dbo",
            source_table="statistic_invoice_header",
            target_table="statistic_invoice_header_mysupply",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="FakturaDato",
            is_string_date=True,
        )
        result = build_partition_where_clause(config, "2025-01-15", "MySupply")
        assert result == "[FakturaDato] = '2025-01-15'"

    def test_transactional_combines_with_custom_where(self):
        """Transactional tables should combine partition filter with custom_where_clause."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
            custom_where_clause="Status = 'POSTED'",
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        assert (
            result
            == "(CAST([Oprettelsestidspunkt] AS DATE) = '2025-01-15') AND (Status = 'POSTED')"
        )

    def test_dependent_delegates_to_dependent_function(self):
        """Dependent tables should delegate to build_dependent_table_partition_clause."""
        config = TableConfig(
            output_name="test_dependent",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test dependent table",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        # Should contain EXISTS-based filter with JOIN
        assert result.startswith("EXISTS (")
        assert "FinansposteringId" in result
        assert "'2025-01-15'" in result

    def test_bridge_delegates_to_dependent_function(self):
        """Bridge tables should delegate to build_dependent_table_partition_clause."""
        config = TableConfig(
            output_name="test_bridge",
            source_schema="Finans",
            source_table="FinansposteringKreditorTransRelation",
            target_table="finansposteringkreditortransrelation_sydax",
            description="Test bridge table",
            table_type=TABLE_TYPE_BRIDGE,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="FinansposteringId",
            fk_cast="VARCHAR(50)",
            fk_path=(),
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        # Should contain FK-based filter
        assert "FinansposteringId" in result
        assert "'2025-01-15'" in result

    def test_no_partition_column_returns_custom_where(self):
        """Tables without partition column should return custom_where_clause."""
        config = TableConfig(
            output_name="test_no_partition",
            source_schema="dbo",
            source_table="SomeTable",
            target_table="some_table",
            description="Test table without partition column",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            # No partition_column
            custom_where_clause="Active = 1",
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        assert result == "Active = 1"

    def test_master_data_partitioned_with_single_column(self):
        """Master data partitioned tables with single partition_column."""
        config = TableConfig(
            output_name="test_partitioned_master",
            source_schema="Finans",
            source_table="Finanskonto",
            target_table="finanskonto_sydax",
            description="Test partitioned master data",
            table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
            partition_column="CreatedOn",
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        assert result == "CAST([CreatedOn] AS DATE) = '2025-01-15'"

    def test_master_data_partitioned_with_multiple_columns_or_logic(self):
        """Master data partitioned with partition_columns uses OR logic."""
        config = TableConfig(
            output_name="test_partitioned_master",
            source_schema="Finans",
            source_table="Finanskonto",
            target_table="finanskonto_sydax",
            description="Test partitioned master data with OR",
            table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
            partition_columns=("CreatedOn", "Modified"),
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        # Should have both columns with OR
        assert "(CAST([CreatedOn] AS DATE) = '2025-01-15')" in result
        assert "(CAST([Modified] AS DATE) = '2025-01-15')" in result
        assert " OR " in result

    def test_master_data_partitioned_monthly_with_multiple_columns(self):
        """Master data partitioned monthly with partition_columns uses OR logic with date ranges."""
        config = TableConfig(
            output_name="test_partitioned_master",
            source_schema="Finans",
            source_table="Finanskonto",
            target_table="finanskonto_sydax",
            description="Test partitioned master data monthly with OR",
            table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
            partition_columns=("CreatedOn", "Modified"),
        )
        result = build_partition_where_clause(config, "2025-01", "SydAX")
        # Should have both columns with OR and date ranges
        assert "CAST([CreatedOn] AS DATE) >= '2025-01-01'" in result
        assert "CAST([CreatedOn] AS DATE) <= '2025-01-31'" in result
        assert "CAST([Modified] AS DATE) >= '2025-01-01'" in result
        assert "CAST([Modified] AS DATE) <= '2025-01-31'" in result
        assert " OR " in result

    def test_master_data_partitioned_with_custom_where(self):
        """Master data partitioned should combine partition filter with custom_where_clause."""
        config = TableConfig(
            output_name="test_partitioned_master",
            source_schema="Finans",
            source_table="Finanskonto",
            target_table="finanskonto_sydax",
            description="Test partitioned master data with custom where",
            table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
            partition_columns=("CreatedOn", "Modified"),
            custom_where_clause="Active = 1",
        )
        result = build_partition_where_clause(config, "2025-01-15", "SydAX")
        # Should have OR filter AND custom where
        assert " OR " in result
        assert "AND (Active = 1)" in result

    def test_ils_partitioned_with_pro2_columns(self):
        """ILS tables partitioned with pro2created/pro2modified columns."""
        config = TableConfig(
            output_name="test_ils_partitioned",
            source_schema="dbo",
            source_table="laftdpris",
            target_table="laftdpris_ils",
            description="Test ILS partitioned table",
            table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
            partition_columns=("pro2created", "pro2modified"),
        )
        result = build_partition_where_clause(config, "2025-01-15", "ILS")
        assert "(CAST([pro2created] AS DATE) = '2025-01-15')" in result
        assert "(CAST([pro2modified] AS DATE) = '2025-01-15')" in result
        assert " OR " in result


class TestBuildDependentTablePartitionClause:
    """Tests for build_dependent_table_partition_clause function."""

    def test_sydax_kreditortransaktion(self):
        """Kreditortransaktion should filter via bridge table to Finanspostering."""
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_dependent_table_partition_clause(config, "2025-01-15", "SydAX")

        assert result.startswith("EXISTS (")
        assert "INNER JOIN" in result
        assert "FinansposteringKreditorTransRelation" in result
        assert "Finanspostering" in result
        assert "CAST(_parent.Oprettelsestidspunkt AS DATE) = '2025-01-15'" in result

    def test_sydax_finansposteringkreditortransrelation(self):
        """FinansposteringKreditorTransRelation should filter via FK to Finanspostering."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="FinansposteringKreditorTransRelation",
            target_table="finansposteringkreditortransrelation_sydax",
            description="Test",
            table_type=TABLE_TYPE_BRIDGE,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="FinansposteringId",
            fk_cast="VARCHAR(50)",
            fk_path=(),
        )
        result = build_dependent_table_partition_clause(config, "2025-01-15", "SydAX")

        assert "FinansposteringId" in result
        assert "CAST(Oprettelsestidspunkt AS DATE) = '2025-01-15'" in result

    def test_mysupply_metadata(self):
        """metadata should filter via metadata_id from statistic_invoice_header."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="metadata",
            target_table="metadata_mysupply",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="statistic_invoice_header",
            parent_partition_col="FakturaDato",
            fk_column="id",
            parent_fk_column="metadata_id",
            fk_path=(),
        )
        result = build_dependent_table_partition_clause(config, "2025-01-15", "MySupply")

        assert "id IN" in result
        assert "metadata_id" in result
        assert "statistic_invoice_header" in result
        assert "FakturaDato = '2025-01-15'" in result

    def test_mysupply_statistic_invoice_line(self):
        """statistic_invoice_line should filter via statistic_invoice_header_id."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="statistic_invoice_line",
            target_table="statistic_invoice_line_mysupply",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="statistic_invoice_header",
            parent_partition_col="FakturaDato",
            fk_column="statistic_invoice_header_id",
            parent_fk_column="id",
            fk_path=(),
        )
        result = build_dependent_table_partition_clause(config, "2025-01-15", "MySupply")

        assert "statistic_invoice_header_id IN" in result
        assert "statistic_invoice_header" in result
        assert "FakturaDato = '2025-01-15'" in result

    def test_full_load_returns_custom_where(self):
        """is_full_load tables should return custom_where_clause."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="SomeTable",
            target_table="some_table",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            is_full_load=True,
            custom_where_clause="Status = 'ACTIVE'",
        )
        result = build_dependent_table_partition_clause(config, "2025-01-15", "SydAX")
        assert result == "Status = 'ACTIVE'"

    def test_unknown_table_returns_custom_where(self):
        """Unknown table should fall back to custom_where_clause."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="UnknownTable",
            target_table="unknown_table",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="SomeParent",
            is_full_load=True,
            custom_where_clause="Active = 1",
        )
        result = build_dependent_table_partition_clause(config, "2025-01-15", "SydAX")
        assert result == "Active = 1"


class TestTableConfigValidation:
    """Tests for TableConfig dataclass field validation."""

    def test_default_table_type_is_transactional(self):
        """Default table_type should be TABLE_TYPE_TRANSACTIONAL."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test",
        )
        assert config.table_type == TABLE_TYPE_TRANSACTIONAL

    def test_default_parent_table_is_none(self):
        """Default parent_table should be None."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test",
        )
        assert config.parent_table is None

    def test_default_is_string_date_is_false(self):
        """Default is_string_date should be False."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test",
        )
        assert config.is_string_date is False

    def test_table_type_values(self):
        """Verify table type constants have expected values."""
        assert TABLE_TYPE_MASTER_DATA == "master_data"
        assert TABLE_TYPE_MASTER_DATA_PARTITIONED == "master_data_partitioned"
        assert TABLE_TYPE_TRANSACTIONAL == "transactional"
        assert TABLE_TYPE_DEPENDENT == "dependent"
        assert TABLE_TYPE_BRIDGE == "bridge"

    def test_default_partition_columns_is_empty(self):
        """Default partition_columns should be empty tuple."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test",
        )
        assert config.partition_columns == ()

    def test_frozen_dataclass(self):
        """TableConfig should be immutable (frozen=True)."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test",
        )
        with pytest.raises(AttributeError):
            config.output_name = "modified"


class TestTableConfigsContent:
    """Tests to verify table configs have correct values."""

    def test_sydax_daily_configs_have_table_types(self):
        """All SYDAX_TABLE_CONFIGS_DAILY entries should have valid table_type."""
        from DSI.mssql import SYDAX_TABLE_CONFIGS_DAILY

        for config in SYDAX_TABLE_CONFIGS_DAILY:
            assert config.table_type in (
                TABLE_TYPE_MASTER_DATA,
                TABLE_TYPE_MASTER_DATA_PARTITIONED,
                TABLE_TYPE_TRANSACTIONAL,
                TABLE_TYPE_DEPENDENT,
                TABLE_TYPE_BRIDGE,
            ), f"Config {config.output_name} has invalid table_type: {config.table_type}"

    def test_sydax_finanspostering_is_transactional(self):
        """Finanspostering should be transactional with Oprettelsestidspunkt partition."""
        from DSI.mssql import SYDAX_TABLE_CONFIGS_DAILY

        fp_config = next(
            c for c in SYDAX_TABLE_CONFIGS_DAILY if c.source_table == "Finanspostering"
        )
        assert fp_config.table_type == TABLE_TYPE_TRANSACTIONAL
        assert fp_config.partition_column == "Oprettelsestidspunkt"
        assert fp_config.parent_table is None

    def test_sydax_kreditortransaktion_is_dependent(self):
        """Kreditortransaktion should be dependent on Finanspostering."""
        from DSI.mssql import SYDAX_TABLE_CONFIGS_DAILY

        kt_config = next(
            c for c in SYDAX_TABLE_CONFIGS_DAILY if c.source_table == "Kreditortransaktion"
        )
        assert kt_config.table_type == TABLE_TYPE_DEPENDENT
        assert kt_config.parent_table == "Finanspostering"

    def test_sydax_master_data_tables(self):
        """SydAX master data tables should have appropriate master data table_type."""
        from DSI.mssql import SYDAX_TABLE_CONFIGS_DAILY

        # Tables that remain full load (TABLE_TYPE_MASTER_DATA)
        full_load_master_tables = [
            "Afdeling",
            "Hovedkonto",
            "Gruppering",
            "GrupperingAutoriseret",
            "Art",
            "Funktion",
            "Dranst",
        ]

        # Tables that are partitioned by CreatedOn OR Modified
        partitioned_master_tables = [
            "Finanskonto",
            "FinanskontoOmkostningssted",
            "OmkostningsstedHierarki",
            "Kreditor",
        ]

        for table_name in full_load_master_tables:
            config = next(
                (c for c in SYDAX_TABLE_CONFIGS_DAILY if c.source_table == table_name),
                None,
            )
            assert config is not None, f"Missing config for {table_name}"
            assert (
                config.table_type == TABLE_TYPE_MASTER_DATA
            ), f"{table_name} should be master_data, got {config.table_type}"

        for table_name in partitioned_master_tables:
            config = next(
                (c for c in SYDAX_TABLE_CONFIGS_DAILY if c.source_table == table_name),
                None,
            )
            assert config is not None, f"Missing config for {table_name}"
            assert (
                config.table_type == TABLE_TYPE_MASTER_DATA_PARTITIONED
            ), f"{table_name} should be master_data_partitioned, got {config.table_type}"
            # Should have partition_columns with CreatedOn and Modified
            assert config.partition_columns == (
                "CreatedOn",
                "Modified",
            ), f"{table_name} should have partition_columns ('CreatedOn', 'Modified'), got {config.partition_columns}"

    def test_mysupply_daily_configs_have_table_types(self):
        """All MYSUPPLY_TABLE_CONFIGS_DAILY entries should have valid table_type."""
        from DSI.mssql import MYSUPPLY_TABLE_CONFIGS_DAILY

        for config in MYSUPPLY_TABLE_CONFIGS_DAILY:
            assert config.table_type in (
                TABLE_TYPE_MASTER_DATA,
                TABLE_TYPE_MASTER_DATA_PARTITIONED,
                TABLE_TYPE_TRANSACTIONAL,
                TABLE_TYPE_DEPENDENT,
                TABLE_TYPE_BRIDGE,
            ), f"Config {config.output_name} has invalid table_type: {config.table_type}"

    def test_mysupply_statistic_invoice_header_is_transactional_with_string_date(self):
        """statistic_invoice_header should be transactional with is_string_date=True."""
        from DSI.mssql import MYSUPPLY_TABLE_CONFIGS_DAILY

        sih_config = next(
            c for c in MYSUPPLY_TABLE_CONFIGS_DAILY if c.source_table == "statistic_invoice_header"
        )
        assert sih_config.table_type == TABLE_TYPE_TRANSACTIONAL
        assert sih_config.partition_column == "FakturaDato"
        assert sih_config.is_string_date is True
        assert sih_config.parent_table is None

    def test_mysupply_dependent_tables(self):
        """MySupply dependent tables should have correct parent_table."""
        from DSI.mssql import MYSUPPLY_TABLE_CONFIGS_DAILY

        dependent_tables = ["metadata", "statistic_invoice_line"]
        for table_name in dependent_tables:
            config = next(c for c in MYSUPPLY_TABLE_CONFIGS_DAILY if c.source_table == table_name)
            assert config.table_type == TABLE_TYPE_DEPENDENT, f"{table_name} should be dependent"
            assert (
                config.parent_table == "statistic_invoice_header"
            ), f"{table_name} should have parent statistic_invoice_header"


class TestNoHardcodedDates:
    """Tests to ensure no hardcoded dates remain in configurations."""

    def test_sydax_configs_no_hardcoded_dates(self):
        """SYDAX_TABLE_CONFIGS_DAILY should have no hardcoded year/date filters."""
        from DSI.mssql import SYDAX_TABLE_CONFIGS_DAILY

        for config in SYDAX_TABLE_CONFIGS_DAILY:
            if config.custom_where_clause:
                assert (
                    "2026" not in config.custom_where_clause
                ), f"Config {config.output_name} has hardcoded 2026"
                assert (
                    "2025" not in config.custom_where_clause
                ), f"Config {config.output_name} has hardcoded 2025"
                assert (
                    "YEAR(" not in config.custom_where_clause
                ), f"Config {config.output_name} has YEAR() filter"

    def test_mysupply_configs_no_hardcoded_dates(self):
        """MYSUPPLY_TABLE_CONFIGS_DAILY should have no hardcoded year/date filters."""
        from DSI.mssql import MYSUPPLY_TABLE_CONFIGS_DAILY

        for config in MYSUPPLY_TABLE_CONFIGS_DAILY:
            if config.custom_where_clause:
                assert (
                    "2026" not in config.custom_where_clause
                ), f"Config {config.output_name} has hardcoded 2026"
                assert (
                    ">= '" not in config.custom_where_clause
                ), f"Config {config.output_name} has >= date filter"


class TestMonthlyPartitionSupport:
    """Tests for monthly partition support."""

    def test_is_monthly_partition_detects_monthly_format(self):
        """is_monthly_partition should return True for YYYY-MM format."""
        assert is_monthly_partition("2025-01") is True
        assert is_monthly_partition("2024-12") is True
        assert is_monthly_partition("2023-06") is True

    def test_is_monthly_partition_rejects_daily_format(self):
        """is_monthly_partition should return False for YYYY-MM-DD format."""
        assert is_monthly_partition("2025-01-15") is False
        assert is_monthly_partition("2024-12-31") is False
        assert is_monthly_partition("2023-06-01") is False

    def test_get_month_date_range_january(self):
        """get_month_date_range should return correct range for January (31 days)."""
        start, end = get_month_date_range("2025-01")
        assert start == "2025-01-01"
        assert end == "2025-01-31"

    def test_get_month_date_range_february_non_leap(self):
        """get_month_date_range should return correct range for February non-leap year."""
        start, end = get_month_date_range("2025-02")
        assert start == "2025-02-01"
        assert end == "2025-02-28"

    def test_get_month_date_range_february_leap(self):
        """get_month_date_range should return correct range for February leap year."""
        start, end = get_month_date_range("2024-02")
        assert start == "2024-02-01"
        assert end == "2024-02-29"

    def test_get_month_date_range_april(self):
        """get_month_date_range should return correct range for April (30 days)."""
        start, end = get_month_date_range("2025-04")
        assert start == "2025-04-01"
        assert end == "2025-04-30"

    def test_monthly_partition_sydax_datetime_column(self):
        """Monthly partition for SydAX datetime column uses date range."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        result = build_partition_where_clause(config, "2025-01", "SydAX")
        assert "CAST([Oprettelsestidspunkt] AS DATE) >= '2025-01-01'" in result
        assert "CAST([Oprettelsestidspunkt] AS DATE) <= '2025-01-31'" in result

    def test_monthly_partition_mysupply_string_date_column(self):
        """Monthly partition for MySupply string date column uses string comparison."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="dbo",
            source_table="statistic_invoice_header",
            target_table="statistic_invoice_header_mysupply",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="FakturaDato",
            is_string_date=True,
        )
        result = build_partition_where_clause(config, "2025-01", "MySupply")
        assert "[FakturaDato] >= '2025-01-01'" in result
        assert "[FakturaDato] <= '2025-01-31'" in result

    def test_monthly_partition_sydax_dependent_kreditortransaktion(self):
        """Monthly partition for dependent Kreditortransaktion uses date range in subquery."""
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_dependent_table_partition_clause(config, "2025-01", "SydAX")
        assert "CAST(_parent.Oprettelsestidspunkt AS DATE) >= '2025-01-01'" in result
        assert "CAST(_parent.Oprettelsestidspunkt AS DATE) <= '2025-01-31'" in result

    def test_monthly_partition_mysupply_dependent_metadata(self):
        """Monthly partition for dependent metadata uses date range in subquery."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="metadata",
            target_table="metadata_mysupply",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="statistic_invoice_header",
            parent_partition_col="FakturaDato",
            fk_column="id",
            parent_fk_column="metadata_id",
            fk_path=(),
        )
        result = build_dependent_table_partition_clause(config, "2025-01", "MySupply")
        assert "FakturaDato >= '2025-01-01'" in result
        assert "FakturaDato <= '2025-01-31'" in result


class TestMonthlyTableConfigs:
    """Tests for monthly table configurations."""

    def test_sydax_monthly_configs_exist(self):
        """SYDAX_TABLE_CONFIGS_MONTHLY should have 17 tables.

        Drift detector: bump this count deliberately when adding/removing
        SYDAX base table configs (DSI/mssql/sydax.py).
        """
        from DSI.mssql import SYDAX_TABLE_CONFIGS_MONTHLY

        assert len(SYDAX_TABLE_CONFIGS_MONTHLY) == 17

    def test_mysupply_monthly_configs_exist(self):
        """MYSUPPLY_TABLE_CONFIGS_MONTHLY should have 5 tables.

        header, metadata, line + the two allowance/charge tables
        (statistic_invoice_header_allowance_charge, statistic_invoice_line_allowance_charge).
        """
        from DSI.mssql import MYSUPPLY_TABLE_CONFIGS_MONTHLY

        assert len(MYSUPPLY_TABLE_CONFIGS_MONTHLY) == 5

    def test_monthly_configs_have_monthly_suffix(self):
        """All monthly configs should have _monthly suffix in output_name."""
        from DSI.mssql import (
            MYSUPPLY_TABLE_CONFIGS_MONTHLY,
            SYDAX_TABLE_CONFIGS_MONTHLY,
        )

        for config in SYDAX_TABLE_CONFIGS_MONTHLY:
            assert config.output_name.endswith(
                "_monthly"
            ), f"Config {config.output_name} should end with _monthly"

        for config in MYSUPPLY_TABLE_CONFIGS_MONTHLY:
            assert config.output_name.endswith(
                "_monthly"
            ), f"Config {config.output_name} should end with _monthly"

    def test_monthly_configs_same_table_types_as_daily(self):
        """Monthly configs should have same table_type distribution as daily."""
        from DSI.mssql import (
            MYSUPPLY_TABLE_CONFIGS_DAILY,
            MYSUPPLY_TABLE_CONFIGS_MONTHLY,
            SYDAX_TABLE_CONFIGS_DAILY,
            SYDAX_TABLE_CONFIGS_MONTHLY,
        )

        # Check SydAX
        daily_types = [c.table_type for c in SYDAX_TABLE_CONFIGS_DAILY]
        monthly_types = [c.table_type for c in SYDAX_TABLE_CONFIGS_MONTHLY]
        assert sorted(daily_types) == sorted(monthly_types)

        # Check MySupply
        daily_types = [c.table_type for c in MYSUPPLY_TABLE_CONFIGS_DAILY]
        monthly_types = [c.table_type for c in MYSUPPLY_TABLE_CONFIGS_MONTHLY]
        assert sorted(daily_types) == sorted(monthly_types)


class TestILSPartitionedConfigs:
    """Tests for ILS partitioned table configurations."""

    def test_ils_full_load_configs_exist(self):
        """ILS_TABLE_CONFIGS should have tables that are always fully loaded."""
        from DSI.mssql import ILS_TABLE_CONFIGS

        assert len(ILS_TABLE_CONFIGS) == 9
        table_names = [c.source_table for c in ILS_TABLE_CONFIGS]
        assert "vareart" in table_names  # 191 rows
        assert "varegrp" in table_names  # 34 rows
        assert "lafth" in table_names  # 750 rows
        assert "vare" in table_names  # 261K rows
        assert "lev" in table_names  # 38K rows
        assert "sgh" in table_names  # Hospital master data
        assert "lgr" in table_names  # Warehouse master data
        assert "rekv" in table_names  # Requisitioner master data
        assert "rekvgrp" in table_names  # Requisitioner group master data

    def test_ils_daily_configs_exist(self):
        """ILS_TABLE_CONFIGS_DAILY should have large partitioned tables."""
        from DSI.mssql import ILS_TABLE_CONFIGS_DAILY

        assert len(ILS_TABLE_CONFIGS_DAILY) == 7
        table_names = [c.source_table for c in ILS_TABLE_CONFIGS_DAILY]
        assert "laftdpris" in table_names  # 187K rows
        assert "laftdstam" in table_names  # 420K rows
        assert "laftd" in table_names  # 62K rows
        assert "lgrvare" in table_names  # 188K rows
        assert "pmdm" in table_names  # 96K rows
        assert "indkfak" in table_names  # 434K rows
        assert "udlev" in table_names  # 1M rows

    def test_ils_daily_configs_use_pro2_columns(self):
        """ILS daily configs should use pro2created-based partitioning."""
        from DSI.mssql import ILS_TABLE_CONFIGS_DAILY

        valid_partition_columns = {
            ("pro2created", "pro2modified"),
            ("pro2created", "sysdato"),
            ("sysdato",),
        }
        for config in ILS_TABLE_CONFIGS_DAILY:
            assert (
                config.table_type == TABLE_TYPE_MASTER_DATA_PARTITIONED
            ), f"{config.source_table} should be master_data_partitioned"
            assert config.partition_columns in valid_partition_columns, (
                f"{config.source_table} should have partition_columns in {valid_partition_columns}, "
                f"got {config.partition_columns}"
            )
            assert config.is_full_load is False, f"{config.source_table} should not be full_load"

    def test_ils_full_load_configs_are_full_load(self):
        """ILS full load configs should have is_full_load=True."""
        from DSI.mssql import ILS_TABLE_CONFIGS

        for config in ILS_TABLE_CONFIGS:
            assert config.is_full_load is True, f"{config.source_table} should be is_full_load=True"
            assert (
                config.table_type != TABLE_TYPE_MASTER_DATA_PARTITIONED
            ), f"{config.source_table} should not be partitioned"

    def test_ils_daily_configs_have_daily_suffix(self):
        """ILS daily configs should have _daily suffix in output_name."""
        from DSI.mssql import ILS_TABLE_CONFIGS_DAILY

        for config in ILS_TABLE_CONFIGS_DAILY:
            assert config.output_name.endswith(
                "_daily"
            ), f"Config {config.output_name} should end with _daily"


class TestScheduledCatchupMode:
    """Tests for scheduled catchup mode (is_scheduled_catchup=True).

    Scheduled catchup mode is used when the daily schedule runs to load
    the last 25 hours of data instead of exact date matching.
    """

    def test_scheduled_catchup_datetime_column(self):
        """Datetime columns should use >= with timestamp for scheduled catchup."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        result = build_partition_where_clause(
            config, "2025-01-15", "SydAX", is_scheduled_catchup=True
        )
        # Should use >= with yesterday's date at 01:00
        assert result == "[Oprettelsestidspunkt] >= '2025-01-14 01:00:00'"

    def test_scheduled_catchup_string_date_column(self):
        """String date columns should use >= with yesterday's date for scheduled catchup."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="dbo",
            source_table="statistic_invoice_header",
            target_table="statistic_invoice_header_mysupply",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="FakturaDato",
            is_string_date=True,
        )
        result = build_partition_where_clause(
            config, "2025-01-15", "MySupply", is_scheduled_catchup=True
        )
        # String dates don't have time component, use >= yesterday
        assert result == "[FakturaDato] >= '2025-01-14'"

    def test_scheduled_catchup_with_multiple_columns_or_logic(self):
        """Multiple partition columns should use OR logic with time window."""
        config = TableConfig(
            output_name="test_partitioned_master",
            source_schema="Finans",
            source_table="Finanskonto",
            target_table="finanskonto_sydax",
            description="Test partitioned master data with OR",
            table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
            partition_columns=("CreatedOn", "Modified"),
        )
        result = build_partition_where_clause(
            config, "2025-01-15", "SydAX", is_scheduled_catchup=True
        )
        # Should have both columns with OR and time window
        assert "([CreatedOn] >= '2025-01-14 01:00:00')" in result
        assert "([Modified] >= '2025-01-14 01:00:00')" in result
        assert " OR " in result

    def test_scheduled_catchup_with_custom_where(self):
        """Scheduled catchup should combine time window filter with custom_where_clause."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
            custom_where_clause="Status = 'POSTED'",
        )
        result = build_partition_where_clause(
            config, "2025-01-15", "SydAX", is_scheduled_catchup=True
        )
        # Should have time window AND custom where
        assert "[Oprettelsestidspunkt] >= '2025-01-14 01:00:00'" in result
        assert "AND (Status = 'POSTED')" in result

    def test_scheduled_catchup_dependent_table_sydax(self):
        """Dependent SydAX tables should use time window in subquery filter."""
        config = TableConfig(
            output_name="test_dependent",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test dependent table",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_dependent_table_partition_clause(
            config, "2025-01-15", "SydAX", is_scheduled_catchup=True
        )
        # Should contain time window filter in EXISTS subquery
        assert "_parent.[Oprettelsestidspunkt] >= '2025-01-14 01:00:00'" in result
        assert result.startswith("EXISTS (")

    def test_scheduled_catchup_dependent_table_mysupply(self):
        """Dependent MySupply tables should use >= yesterday in subquery filter."""
        config = TableConfig(
            output_name="test_dependent",
            source_schema="dbo",
            source_table="statistic_invoice_line",
            target_table="statistic_invoice_line_mysupply",
            description="Test dependent table",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="statistic_invoice_header",
            parent_partition_col="FakturaDato",
            fk_column="statistic_invoice_header_id",
            parent_fk_column="id",
            fk_path=(),
        )
        result = build_dependent_table_partition_clause(
            config, "2025-01-15", "MySupply", is_scheduled_catchup=True
        )
        # String dates use >= yesterday
        assert "FakturaDato >= '2025-01-14'" in result
        assert "statistic_invoice_header_id IN" in result

    def test_manual_run_unchanged_default_is_false(self):
        """Default behavior (is_scheduled_catchup=False) should use exact date match."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        # Explicit False
        result_explicit = build_partition_where_clause(
            config, "2025-01-15", "SydAX", is_scheduled_catchup=False
        )
        # Default (no argument)
        result_default = build_partition_where_clause(config, "2025-01-15", "SydAX")
        # Both should be exact date match
        assert result_explicit == "CAST([Oprettelsestidspunkt] AS DATE) = '2025-01-15'"
        assert result_default == "CAST([Oprettelsestidspunkt] AS DATE) = '2025-01-15'"

    def test_monthly_partition_ignores_scheduled_catchup(self):
        """Monthly partitions should ignore scheduled_catchup and use date range."""
        config = TableConfig(
            output_name="test_transactional",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test transactional table",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        # With scheduled catchup True but monthly partition
        result_catchup = build_partition_where_clause(
            config, "2025-01", "SydAX", is_scheduled_catchup=True
        )
        # Without scheduled catchup
        result_normal = build_partition_where_clause(
            config, "2025-01", "SydAX", is_scheduled_catchup=False
        )
        # Both should use date range for monthly
        expected = (
            "CAST([Oprettelsestidspunkt] AS DATE) >= '2025-01-01' AND "
            "CAST([Oprettelsestidspunkt] AS DATE) <= '2025-01-31'"
        )
        assert result_catchup == expected
        assert result_normal == expected

    def test_scheduled_catchup_bridge_table_sydax(self):
        """Bridge SydAX tables should use time window in subquery filter."""
        config = TableConfig(
            output_name="test_bridge",
            source_schema="Finans",
            source_table="FinansposteringKreditorTransRelation",
            target_table="finansposteringkreditortransrelation_sydax",
            description="Test bridge table",
            table_type=TABLE_TYPE_BRIDGE,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="FinansposteringId",
            fk_cast="VARCHAR(50)",
            fk_path=(),
        )
        result = build_partition_where_clause(
            config, "2025-01-15", "SydAX", is_scheduled_catchup=True
        )
        # Should contain time window filter in subquery
        assert "[Oprettelsestidspunkt] >= '2025-01-14 01:00:00'" in result
        assert "FinansposteringId" in result


class TestGeneratePartitionConfigs:
    """Tests for the generate_partition_configs factory function."""

    def test_factory_appends_daily_suffix(self):
        """Factory should append _daily suffix to output_name."""
        from DSI.connectors.mssql.models import generate_partition_configs

        base_configs = (
            TableConfig(
                output_name="stg_test_table",
                source_schema="dbo",
                source_table="TestTable",
                target_table="test_table",
                description="Test table",
            ),
        )
        result = generate_partition_configs(base_configs, "daily")

        assert len(result) == 1
        assert result[0].output_name == "stg_test_table_daily"

    def test_factory_appends_monthly_suffix(self):
        """Factory should append _monthly suffix to output_name."""
        from DSI.connectors.mssql.models import generate_partition_configs

        base_configs = (
            TableConfig(
                output_name="stg_test_table",
                source_schema="dbo",
                source_table="TestTable",
                target_table="test_table",
                description="Test table",
            ),
        )
        result = generate_partition_configs(base_configs, "monthly")

        assert len(result) == 1
        assert result[0].output_name == "stg_test_table_monthly"

    def test_factory_updates_description(self):
        """Factory should append partition frequency to description."""
        from DSI.connectors.mssql.models import generate_partition_configs

        base_configs = (
            TableConfig(
                output_name="stg_test_table",
                source_schema="dbo",
                source_table="TestTable",
                target_table="test_table",
                description="Test table",
            ),
        )
        daily_result = generate_partition_configs(base_configs, "daily")
        monthly_result = generate_partition_configs(base_configs, "monthly")

        assert "(daily partition)" in daily_result[0].description
        assert "(monthly partition)" in monthly_result[0].description

    def test_factory_preserves_all_config_fields(self):
        """Factory should preserve all TableConfig fields from base config."""
        from DSI.connectors.mssql.models import generate_partition_configs

        base_configs = (
            TableConfig(
                output_name="ext_test_table",
                source_schema="dbo",
                source_table="TestTable",
                target_table="test_table",
                description="Test table",
                exclude_columns=("col1", "col2"),
                custom_where_clause="Active = 1",
                partition_column="CreatedOn",
                partition_columns=("CreatedOn", "Modified"),
                is_full_load=False,
                consistency_columns=("id", "name"),
                table_type=TABLE_TYPE_TRANSACTIONAL,
                parent_table="ParentTable",
                is_string_date=True,
            ),
        )
        result = generate_partition_configs(base_configs, "daily")

        assert len(result) == 1
        cfg = result[0]
        assert cfg.source_schema == "dbo"
        assert cfg.source_table == "TestTable"
        assert cfg.target_table == "test_table"
        assert cfg.exclude_columns == ("col1", "col2")
        assert cfg.custom_where_clause == "Active = 1"
        assert cfg.partition_column == "CreatedOn"
        assert cfg.partition_columns == ("CreatedOn", "Modified")
        assert cfg.is_full_load is False
        assert cfg.consistency_columns == ("id", "name")
        assert cfg.table_type == TABLE_TYPE_TRANSACTIONAL
        assert cfg.parent_table == "ParentTable"
        assert cfg.is_string_date is True

    def test_factory_generates_correct_count_for_sydax(self):
        """Factory-generated SYDAX configs should have same count as base."""
        from DSI.mssql import (
            SYDAX_BASE_TABLE_CONFIGS,
            SYDAX_TABLE_CONFIGS_DAILY,
            SYDAX_TABLE_CONFIGS_MONTHLY,
        )

        assert len(SYDAX_TABLE_CONFIGS_DAILY) == len(SYDAX_BASE_TABLE_CONFIGS)
        assert len(SYDAX_TABLE_CONFIGS_MONTHLY) == len(SYDAX_BASE_TABLE_CONFIGS)

    def test_factory_generates_correct_count_for_mysupply(self):
        """Factory-generated MYSUPPLY configs should have same count as base."""
        from DSI.mssql import (
            MYSUPPLY_BASE_TABLE_CONFIGS,
            MYSUPPLY_TABLE_CONFIGS_DAILY,
            MYSUPPLY_TABLE_CONFIGS_MONTHLY,
        )

        assert len(MYSUPPLY_TABLE_CONFIGS_DAILY) == len(MYSUPPLY_BASE_TABLE_CONFIGS)
        assert len(MYSUPPLY_TABLE_CONFIGS_MONTHLY) == len(MYSUPPLY_BASE_TABLE_CONFIGS)

    def test_factory_generates_correct_count_for_ils(self):
        """Factory-generated ILS configs should have same count as base."""
        from DSI.mssql import (
            ILS_PARTITIONED_BASE_TABLE_CONFIGS,
            ILS_TABLE_CONFIGS_DAILY,
            ILS_TABLE_CONFIGS_MONTHLY,
        )

        assert len(ILS_TABLE_CONFIGS_DAILY) == len(ILS_PARTITIONED_BASE_TABLE_CONFIGS)
        assert len(ILS_TABLE_CONFIGS_MONTHLY) == len(ILS_PARTITIONED_BASE_TABLE_CONFIGS)


class TestILSMonthlyConfigs:
    """Tests for ILS monthly table configurations."""

    def test_ils_monthly_configs_exist(self):
        """ILS_TABLE_CONFIGS_MONTHLY should have 7 large partitioned tables."""
        from DSI.mssql import ILS_TABLE_CONFIGS_MONTHLY

        assert len(ILS_TABLE_CONFIGS_MONTHLY) == 7
        table_names = [c.source_table for c in ILS_TABLE_CONFIGS_MONTHLY]
        assert "laftdpris" in table_names
        assert "laftdstam" in table_names
        assert "laftd" in table_names
        assert "lgrvare" in table_names
        assert "pmdm" in table_names
        assert "indkfak" in table_names
        assert "udlev" in table_names

    def test_ils_monthly_configs_have_monthly_suffix(self):
        """ILS monthly configs should have _monthly suffix in output_name."""
        from DSI.mssql import ILS_TABLE_CONFIGS_MONTHLY

        for config in ILS_TABLE_CONFIGS_MONTHLY:
            assert config.output_name.endswith(
                "_monthly"
            ), f"Config {config.output_name} should end with _monthly"

    def test_ils_monthly_configs_use_pro2_columns(self):
        """ILS monthly configs should use pro2created-based partitioning."""
        from DSI.mssql import ILS_TABLE_CONFIGS_MONTHLY

        valid_partition_columns = {
            ("pro2created", "pro2modified"),
            ("pro2created", "sysdato"),
            ("sysdato",),
        }
        for config in ILS_TABLE_CONFIGS_MONTHLY:
            assert (
                config.table_type == TABLE_TYPE_MASTER_DATA_PARTITIONED
            ), f"{config.source_table} should be master_data_partitioned"
            assert config.partition_columns in valid_partition_columns, (
                f"{config.source_table} should have partition_columns in {valid_partition_columns}, "
                f"got {config.partition_columns}"
            )

    def test_ils_monthly_configs_same_table_types_as_daily(self):
        """ILS monthly configs should have same table_type distribution as daily."""
        from DSI.mssql import (
            ILS_TABLE_CONFIGS_DAILY,
            ILS_TABLE_CONFIGS_MONTHLY,
        )

        daily_types = [c.table_type for c in ILS_TABLE_CONFIGS_DAILY]
        monthly_types = [c.table_type for c in ILS_TABLE_CONFIGS_MONTHLY]
        assert sorted(daily_types) == sorted(monthly_types)

    def test_ils_daily_and_monthly_configs_same_source_tables(self):
        """ILS daily and monthly configs should have same source tables."""
        from DSI.mssql import (
            ILS_TABLE_CONFIGS_DAILY,
            ILS_TABLE_CONFIGS_MONTHLY,
        )

        daily_tables = sorted([c.source_table for c in ILS_TABLE_CONFIGS_DAILY])
        monthly_tables = sorted([c.source_table for c in ILS_TABLE_CONFIGS_MONTHLY])
        assert daily_tables == monthly_tables


class TestBaseTableConfigs:
    """Tests for base table configurations."""

    def test_sydax_base_configs_have_no_frequency_suffix(self):
        """SYDAX_BASE_TABLE_CONFIGS should not have _daily or _monthly suffix."""
        from DSI.mssql import SYDAX_BASE_TABLE_CONFIGS

        for config in SYDAX_BASE_TABLE_CONFIGS:
            assert not config.output_name.endswith(
                "_daily"
            ), f"Base config {config.output_name} should not end with _daily"
            assert not config.output_name.endswith(
                "_monthly"
            ), f"Base config {config.output_name} should not end with _monthly"

    def test_mysupply_base_configs_have_no_frequency_suffix(self):
        """MYSUPPLY_BASE_TABLE_CONFIGS should not have _daily or _monthly suffix."""
        from DSI.mssql import MYSUPPLY_BASE_TABLE_CONFIGS

        for config in MYSUPPLY_BASE_TABLE_CONFIGS:
            assert not config.output_name.endswith(
                "_daily"
            ), f"Base config {config.output_name} should not end with _daily"
            assert not config.output_name.endswith(
                "_monthly"
            ), f"Base config {config.output_name} should not end with _monthly"

    def test_ils_base_configs_have_no_frequency_suffix(self):
        """ILS_PARTITIONED_BASE_TABLE_CONFIGS should not have _daily or _monthly suffix."""
        from DSI.mssql import ILS_PARTITIONED_BASE_TABLE_CONFIGS

        for config in ILS_PARTITIONED_BASE_TABLE_CONFIGS:
            assert not config.output_name.endswith(
                "_daily"
            ), f"Base config {config.output_name} should not end with _daily"
            assert not config.output_name.endswith(
                "_monthly"
            ), f"Base config {config.output_name} should not end with _monthly"


class TestIsFirstPartition:
    """Tests for the is_first_partition helper in core.partitions."""

    def test_daily_start_key_matches(self):
        """Matching the daily start key should return True."""
        assert is_first_partition("2021-01-01", daily_partitions) is True

    def test_daily_erp_start_key_matches(self):
        """Matching the ERP daily start key should return True."""
        assert is_first_partition("2005-01-01", daily_partitions_erp) is True

    def test_monthly_start_key_matches(self):
        """Matching the monthly start key should return True."""
        assert is_first_partition("2021-01", monthly_partitions) is True

    def test_monthly_erp_start_key_matches(self):
        """Matching the ERP monthly start key should return True."""
        assert is_first_partition("2012-01", monthly_partitions_erp) is True

    def test_non_start_key_returns_false(self):
        """A non-start key should return False."""
        assert is_first_partition("2025-06-15", daily_partitions_erp) is False

    def test_fallback_without_partitions_def(self):
        """Without partitions_def, any known start key should return True."""
        assert is_first_partition("2005-01-01") is True
        assert is_first_partition("2021-01-01") is True
        assert is_first_partition("2025-06-15") is False

    def test_all_partition_defs_registered(self):
        """Every time-based partition definition should appear in PARTITION_START_KEYS."""
        assert daily_partitions in PARTITION_START_KEYS
        assert monthly_partitions in PARTITION_START_KEYS
        assert daily_partitions_erp in PARTITION_START_KEYS
        assert monthly_partitions_erp in PARTITION_START_KEYS


class TestFirstPartitionFilterLogic:
    """Tests for first-partition upper-bound-only filtering in ERP extract."""

    def test_first_partition_daily_datetime_column(self):
        """First daily partition should produce upper-bound-only filter."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        result = build_partition_where_clause(
            config, "2005-01-01", "SydAX", is_first_partition=True
        )
        assert result == "CAST([Oprettelsestidspunkt] AS DATE) <= '2005-01-01'"

    def test_first_partition_daily_string_date_column(self):
        """First daily partition with string date should produce upper-bound-only filter."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="statistic_invoice_header",
            target_table="statistic_invoice_header_mysupply",
            description="Test",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="FakturaDato",
            is_string_date=True,
        )
        result = build_partition_where_clause(
            config, "2005-01-01", "MySupply", is_first_partition=True
        )
        assert result == "[FakturaDato] <= '2005-01-01'"

    def test_first_partition_monthly_datetime_column(self):
        """First monthly partition should produce upper-bound-only filter."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        result = build_partition_where_clause(config, "2012-01", "SydAX", is_first_partition=True)
        assert result == "CAST([Oprettelsestidspunkt] AS DATE) <= '2012-01-31'"

    def test_first_partition_monthly_string_date_column(self):
        """First monthly partition with string date should produce upper-bound-only filter."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="statistic_invoice_header",
            target_table="statistic_invoice_header_mysupply",
            description="Test",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="FakturaDato",
            is_string_date=True,
        )
        result = build_partition_where_clause(
            config, "2012-01", "MySupply", is_first_partition=True
        )
        assert result == "[FakturaDato] <= '2012-01-31'"

    def test_first_partition_with_custom_where(self):
        """First partition should combine upper-bound filter with custom_where_clause."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
            custom_where_clause="Status = 'POSTED'",
        )
        result = build_partition_where_clause(
            config, "2005-01-01", "SydAX", is_first_partition=True
        )
        assert "CAST([Oprettelsestidspunkt] AS DATE) <= '2005-01-01'" in result
        assert "AND (Status = 'POSTED')" in result

    def test_first_partition_multiple_columns_or_logic(self):
        """First partition with multiple partition_columns uses OR with upper-bound-only."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="Finanskonto",
            target_table="finanskonto_sydax",
            description="Test",
            table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
            partition_columns=("CreatedOn", "Modified"),
        )
        result = build_partition_where_clause(
            config, "2005-01-01", "SydAX", is_first_partition=True
        )
        assert "(CAST([CreatedOn] AS DATE) <= '2005-01-01')" in result
        assert "(CAST([Modified] AS DATE) <= '2005-01-01')" in result
        assert " OR " in result

    def test_non_first_partition_unchanged(self):
        """Non-first partition should still use exact date match."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        result = build_partition_where_clause(
            config, "2025-06-15", "SydAX", is_first_partition=False
        )
        assert result == "CAST([Oprettelsestidspunkt] AS DATE) = '2025-06-15'"

    def test_scheduled_catchup_overrides_first_partition(self):
        """Scheduled catchup should take precedence over first partition logic."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="Finanspostering",
            target_table="finanspostering_sydax",
            description="Test",
            table_type=TABLE_TYPE_TRANSACTIONAL,
            partition_column="Oprettelsestidspunkt",
        )
        result = build_partition_where_clause(
            config,
            "2005-01-01",
            "SydAX",
            is_scheduled_catchup=True,
            is_first_partition=True,
        )
        assert ">=" in result
        assert "01:00:00" in result


class TestFirstPartitionDependentTables:
    """Tests for first-partition logic in dependent/bridge table clause builders."""

    def test_first_partition_sydax_kreditortransaktion(self):
        """First partition: dependent SydAX table should use upper-bound-only in subquery."""
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_dependent_table_partition_clause(
            config, "2005-01-01", "SydAX", is_first_partition=True
        )
        assert "CAST(_parent.Oprettelsestidspunkt AS DATE) <= '2005-01-01'" in result
        assert result.startswith("EXISTS (")
        assert ">=" not in result

    def test_first_partition_sydax_bridge(self):
        """First partition: bridge SydAX table should use upper-bound-only in subquery."""
        config = TableConfig(
            output_name="test",
            source_schema="Finans",
            source_table="FinansposteringKreditorTransRelation",
            target_table="finansposteringkreditortransrelation_sydax",
            description="Test",
            table_type=TABLE_TYPE_BRIDGE,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="FinansposteringId",
            fk_cast="VARCHAR(50)",
            fk_path=(),
        )
        result = build_partition_where_clause(
            config, "2005-01-01", "SydAX", is_first_partition=True
        )
        assert "CAST(Oprettelsestidspunkt AS DATE) <= '2005-01-01'" in result
        assert "FinansposteringId" in result

    def test_first_partition_mysupply_metadata(self):
        """First partition: dependent MySupply metadata should use upper-bound-only."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="metadata",
            target_table="metadata_mysupply",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="statistic_invoice_header",
            parent_partition_col="FakturaDato",
            fk_column="id",
            parent_fk_column="metadata_id",
            fk_path=(),
        )
        result = build_dependent_table_partition_clause(
            config, "2005-01-01", "MySupply", is_first_partition=True
        )
        assert "FakturaDato <= '2005-01-01'" in result
        assert "id IN" in result
        assert ">=" not in result

    def test_first_partition_mysupply_invoice_line(self):
        """First partition: dependent MySupply invoice_line should use upper-bound-only."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="statistic_invoice_line",
            target_table="statistic_invoice_line_mysupply",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="statistic_invoice_header",
            parent_partition_col="FakturaDato",
            fk_column="statistic_invoice_header_id",
            parent_fk_column="id",
            fk_path=(),
        )
        result = build_dependent_table_partition_clause(
            config, "2005-01-01", "MySupply", is_first_partition=True
        )
        assert "FakturaDato <= '2005-01-01'" in result
        assert "statistic_invoice_header_id IN" in result

    def test_first_partition_monthly_dependent_sydax(self):
        """First monthly partition: dependent SydAX table uses upper-bound-only."""
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_dependent_table_partition_clause(
            config, "2012-01", "SydAX", is_first_partition=True
        )
        assert "CAST(_parent.Oprettelsestidspunkt AS DATE) <= '2012-01-31'" in result
        assert ">=" not in result

    def test_non_first_dependent_unchanged(self):
        """Non-first partition dependent tables should use exact date (unchanged)."""
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_dependent_table_partition_clause(
            config, "2025-06-15", "SydAX", is_first_partition=False
        )
        assert "CAST(_parent.Oprettelsestidspunkt AS DATE) = '2025-06-15'" in result

    def test_scheduled_catchup_overrides_first_partition_dependent(self):
        """Scheduled catchup should take precedence for dependent tables too."""
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Oprettelsestidspunkt",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = build_dependent_table_partition_clause(
            config,
            "2005-01-01",
            "SydAX",
            is_scheduled_catchup=True,
            is_first_partition=True,
        )
        assert ">=" in result
        assert "01:00:00" in result
