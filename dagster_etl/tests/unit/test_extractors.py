# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for SqlExtractor class hierarchy.

Tests validate:
- ExtractResult dataclass fields and properties
- SqlExtractor abstract interface enforcement
- OracleExtractor query building and extraction
- MssqlExtractor extraction, insertion, relation_extract, and consistency checks
- Config-driven FK mapping produces correct SQL for all 4 dependent/bridge tables
"""

from unittest.mock import MagicMock

import pytest
from DSI.connectors.base import ExtractResult, SqlExtractor
from DSI.connectors.mssql.models import (
    TABLE_TYPE_BRIDGE,
    TABLE_TYPE_DEPENDENT,
    TableConfig,
)
from DSI.connectors.mssql.sql_builders import (
    _build_parent_date_filter,
)
from DSI.connectors.mssql.sql_builders import (
    build_relation_where_clause as _build_relation_where_clause,
)
from DSI.connectors.mssql.sql_builders import (
    build_single_column_date_filter as _build_single_column_date_filter,
)
from DSI.connectors.mssql.sql_builders import is_monthly_partition as _is_monthly_partition
from DSI.connectors.oracle.extractor import OracleExtractor

# ============================================================================
# ExtractResult Tests
# ============================================================================


class TestExtractResult:
    """Tests for ExtractResult dataclass."""

    def test_basic_creation(self):
        result = ExtractResult(
            data=[{"a": 1}, {"a": 2}],
            columns_info=[("a", "int", True, None, None, None)],
            column_names=["a"],
            row_count=2,
        )
        assert result.row_count == 2
        assert result.column_names == ["a"]
        assert not result.is_empty

    def test_empty_result(self):
        result = ExtractResult(data=[], columns_info=[], column_names=[], row_count=0)
        assert result.is_empty

    def test_is_empty_property(self):
        empty = ExtractResult(data=[], columns_info=[], column_names=[], row_count=0)
        non_empty = ExtractResult(data=[(1,)], columns_info=[], column_names=["a"], row_count=1)
        assert empty.is_empty is True
        assert non_empty.is_empty is False


# ============================================================================
# SqlExtractor ABC Tests
# ============================================================================


class TestSqlExtractorABC:
    """Tests for SqlExtractor abstract base class."""

    def test_cannot_instantiate_directly(self):
        """SqlExtractor is abstract and cannot be instantiated."""
        ctx = MagicMock()
        with pytest.raises(TypeError):
            SqlExtractor(None, context=ctx, source_system="test")

    def test_default_insert_raises(self):
        """Default insert() should raise NotImplementedError."""
        ctx = MagicMock()
        extractor = OracleExtractor(MagicMock(), context=ctx, source_system="IFDB")
        with pytest.raises(NotImplementedError, match="OracleExtractor.insert"):
            extractor.insert(None, ExtractResult([], [], [], 0), None)

    def test_default_relation_extract_raises(self):
        """Default relation_extract() should raise NotImplementedError."""
        ctx = MagicMock()
        extractor = OracleExtractor(MagicMock(), context=ctx, source_system="IFDB")
        with pytest.raises(NotImplementedError, match="OracleExtractor.relation_extract"):
            extractor.relation_extract(None, None, "daily")

    def test_default_check_consistency_returns_false(self):
        """Default check_consistency() should return False."""
        ctx = MagicMock()
        extractor = OracleExtractor(MagicMock(), context=ctx, source_system="IFDB")
        assert extractor.check_consistency(None) is False


# ============================================================================
# Config-driven FK mapping tests (relation_extract SQL generation)
# ============================================================================


class TestRelationWhereClause:
    """Tests for config-driven FK mapping in _build_relation_where_clause."""

    def test_sydax_kreditortransaktion_daily(self):
        """Kreditortransaktion: FK through bridge table to Finanspostering."""
        config = TableConfig(
            output_name="ext_kreditortransaktion_sydax",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Bogfoeringsdato",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = _build_relation_where_clause(config, "2025-01-15", "SydAX")
        assert result.startswith("EXISTS (")
        assert "FinansposteringKreditorTransRelation" in result
        assert "INNER JOIN" in result
        assert "Finanspostering" in result
        assert "CAST(_parent.Bogfoeringsdato AS DATE) = '2025-01-15'" in result

    def test_sydax_finansposteringkreditortransrelation_daily(self):
        """FinansposteringKreditorTransRelation: bridge with VARCHAR cast."""
        config = TableConfig(
            output_name="ext_finansposteringkreditortransrelation_sydax",
            source_schema="Finans",
            source_table="FinansposteringKreditorTransRelation",
            target_table="finansposteringkreditortransrelation_sydax",
            description="Test",
            table_type=TABLE_TYPE_BRIDGE,
            parent_table="Finanspostering",
            parent_partition_col="Bogfoeringsdato",
            fk_column="FinansposteringId",
            fk_cast="VARCHAR(50)",
            fk_path=(),
        )
        result = _build_relation_where_clause(config, "2025-01-15", "SydAX")
        assert "CAST(FinansposteringId AS VARCHAR(50))" in result
        assert "Finanspostering" in result
        assert "CAST(Bogfoeringsdato AS DATE) = '2025-01-15'" in result

    def test_mysupply_metadata_daily(self):
        """metadata: different FK column names (id → metadata_id)."""
        config = TableConfig(
            output_name="ext_metadata_mysupply",
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
        result = _build_relation_where_clause(config, "2025-01-15", "MySupply")
        assert "id IN" in result
        assert "metadata_id" in result
        assert "statistic_invoice_header" in result
        assert "FakturaDato = '2025-01-15'" in result

    def test_mysupply_statistic_invoice_line_daily(self):
        """statistic_invoice_line: direct FK."""
        config = TableConfig(
            output_name="ext_statistic_invoice_line_mysupply",
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
        result = _build_relation_where_clause(config, "2025-01-15", "MySupply")
        assert "statistic_invoice_header_id IN" in result
        assert "SELECT id FROM" in result
        assert "statistic_invoice_header" in result
        assert "FakturaDato = '2025-01-15'" in result


class TestRelationWhereClauseMonthly:
    """Tests for monthly partition support in relation_extract."""

    def test_sydax_kreditortransaktion_monthly(self):
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Bogfoeringsdato",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = _build_relation_where_clause(config, "2025-01", "SydAX")
        assert "CAST(_parent.Bogfoeringsdato AS DATE) >= '2025-01-01'" in result
        assert "CAST(_parent.Bogfoeringsdato AS DATE) <= '2025-01-31'" in result

    def test_mysupply_metadata_monthly(self):
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
        result = _build_relation_where_clause(config, "2025-01", "MySupply")
        assert "FakturaDato >= '2025-01-01'" in result
        assert "FakturaDato <= '2025-01-31'" in result


class TestRelationWhereClauseFirstPartition:
    """Tests for first-partition upper-bound-only in relation_extract."""

    def test_sydax_first_partition_daily(self):
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Bogfoeringsdato",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = _build_relation_where_clause(
            config, "2005-01-01", "SydAX", is_first_partition=True
        )
        assert "CAST(_parent.Bogfoeringsdato AS DATE) <= '2005-01-01'" in result
        assert ">=" not in result

    def test_mysupply_first_partition_daily(self):
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
        result = _build_relation_where_clause(
            config, "2005-01-01", "MySupply", is_first_partition=True
        )
        assert "FakturaDato <= '2005-01-01'" in result
        assert ">=" not in result


class TestRelationWhereClauseScheduledCatchup:
    """Tests for scheduled catchup in relation_extract."""

    def test_sydax_scheduled_catchup(self):
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
            parent_partition_col="Bogfoeringsdato",
            fk_column="KreditortransaktionId",
            fk_path=(("FinansposteringKreditorTransRelation", "Finans", "KreditortransaktionId"),),
        )
        result = _build_relation_where_clause(
            config, "2025-01-15", "SydAX", is_scheduled_catchup=True
        )
        assert "_parent.[Bogfoeringsdato] >= '2025-01-14 01:00:00'" in result

    def test_mysupply_scheduled_catchup(self):
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
        result = _build_relation_where_clause(
            config, "2025-01-15", "MySupply", is_scheduled_catchup=True
        )
        assert "FakturaDato >= '2025-01-14'" in result


class TestMissingFKFieldsRaisesError:
    """Missing FK fields on dependent/bridge tables must fail fast."""

    def test_missing_fk_column_raises_valueerror(self):
        config = TableConfig(
            output_name="test",
            source_schema="Kreditor",
            source_table="Kreditortransaktion",
            target_table="kreditortransaktion_sydax",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="Finanspostering",
        )
        with pytest.raises(ValueError, match="missing required FK mapping"):
            _build_relation_where_clause(config, "2025-01-15", "SydAX")

    def test_missing_parent_partition_col_raises_valueerror(self):
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="metadata",
            target_table="metadata_mysupply",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="statistic_invoice_header",
            fk_column="id",
        )
        with pytest.raises(ValueError, match="missing required FK mapping"):
            _build_relation_where_clause(config, "2025-01-15", "MySupply")

    def test_full_load_bypasses_fk_check(self):
        """Full load tables should not require FK fields."""
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="SomeTable",
            target_table="sometable",
            description="Test",
            table_type=TABLE_TYPE_DEPENDENT,
            parent_table="ParentTable",
            is_full_load=True,
        )
        result = _build_relation_where_clause(config, "2025-01-15", "SydAX")
        assert result is None


# ============================================================================
# MSSQL helper function tests
# ============================================================================


class TestIsMonthlyPartition:
    def test_monthly_format(self):
        assert _is_monthly_partition("2025-01") is True

    def test_daily_format(self):
        assert _is_monthly_partition("2025-01-15") is False


class TestBuildSingleColumnDateFilter:
    def test_daily_datetime_column(self):
        result = _build_single_column_date_filter("Bogfoeringsdato", "2025-01-15")
        assert result == "CAST([Bogfoeringsdato] AS DATE) = '2025-01-15'"

    def test_daily_string_date_column(self):
        result = _build_single_column_date_filter("FakturaDato", "2025-01-15", is_string_date=True)
        assert result == "[FakturaDato] = '2025-01-15'"

    def test_monthly_datetime_column(self):
        result = _build_single_column_date_filter("Bogfoeringsdato", "2025-01")
        assert "CAST([Bogfoeringsdato] AS DATE) >= '2025-01-01'" in result
        assert "CAST([Bogfoeringsdato] AS DATE) <= '2025-01-31'" in result

    def test_first_partition_upper_bound_only(self):
        result = _build_single_column_date_filter(
            "Bogfoeringsdato", "2005-01-01", is_first_partition=True
        )
        assert result == "CAST([Bogfoeringsdato] AS DATE) <= '2005-01-01'"

    def test_scheduled_catchup(self):
        result = _build_single_column_date_filter(
            "Bogfoeringsdato", "2025-01-15", is_scheduled_catchup=True
        )
        assert result == "[Bogfoeringsdato] >= '2025-01-14 01:00:00'"


class TestBuildParentDateFilter:
    """Tests for _build_parent_date_filter used in FK subqueries."""

    def test_sydax_daily(self):
        result = _build_parent_date_filter("Bogfoeringsdato", "2025-01-15", "SydAX")
        assert result == "CAST(Bogfoeringsdato AS DATE) = '2025-01-15'"

    def test_mysupply_daily(self):
        result = _build_parent_date_filter("FakturaDato", "2025-01-15", "MySupply")
        assert result == "FakturaDato = '2025-01-15'"

    def test_sydax_monthly(self):
        result = _build_parent_date_filter("Bogfoeringsdato", "2025-01", "SydAX")
        assert "CAST(Bogfoeringsdato AS DATE) >= '2025-01-01'" in result
        assert "CAST(Bogfoeringsdato AS DATE) <= '2025-01-31'" in result

    def test_mysupply_monthly(self):
        result = _build_parent_date_filter("FakturaDato", "2025-01", "MySupply")
        assert "FakturaDato >= '2025-01-01'" in result
        assert "FakturaDato <= '2025-01-31'" in result

    def test_sydax_first_partition(self):
        result = _build_parent_date_filter(
            "Bogfoeringsdato", "2005-01-01", "SydAX", is_first_partition=True
        )
        assert result == "CAST(Bogfoeringsdato AS DATE) <= '2005-01-01'"

    def test_mysupply_first_partition(self):
        result = _build_parent_date_filter(
            "FakturaDato", "2005-01-01", "MySupply", is_first_partition=True
        )
        assert result == "FakturaDato <= '2005-01-01'"

    def test_sydax_scheduled_catchup(self):
        result = _build_parent_date_filter(
            "Bogfoeringsdato", "2025-01-15", "SydAX", is_scheduled_catchup=True
        )
        assert result == "[Bogfoeringsdato] >= '2025-01-14 01:00:00'"

    def test_mysupply_scheduled_catchup(self):
        result = _build_parent_date_filter(
            "FakturaDato", "2025-01-15", "MySupply", is_scheduled_catchup=True
        )
        assert result == "FakturaDato >= '2025-01-14'"


# ============================================================================
# TableConfig FK field tests
# ============================================================================


class TestTableConfigFKFields:
    """Tests for the new FK mapping fields on TableConfig."""

    def test_default_fk_fields_are_none_or_empty(self):
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="Test",
            target_table="test",
            description="Test",
        )
        assert config.fk_column is None
        assert config.parent_partition_col is None
        assert config.parent_fk_column is None
        assert config.fk_cast is None
        assert config.fk_path == ()

    def test_fk_fields_preserved_by_generate_partition_configs(self):
        from DSI.connectors.mssql.models import generate_partition_configs

        base = (
            TableConfig(
                output_name="test",
                source_schema="dbo",
                source_table="t",
                target_table="t",
                description="test",
                fk_column="id",
                parent_partition_col="dt",
                parent_fk_column="parent_id",
                fk_cast="VARCHAR(50)",
                fk_path=(("bridge", "dbo", "join_col"),),
            ),
        )
        result = generate_partition_configs(base, "daily")
        cfg = result[0]
        assert cfg.fk_column == "id"
        assert cfg.parent_partition_col == "dt"
        assert cfg.parent_fk_column == "parent_id"
        assert cfg.fk_cast == "VARCHAR(50)"
        assert cfg.fk_path == (("bridge", "dbo", "join_col"),)
        assert cfg.output_name == "test_daily"

    def test_frozen_fk_fields(self):
        config = TableConfig(
            output_name="test",
            source_schema="dbo",
            source_table="t",
            target_table="t",
            description="test",
            fk_column="id",
        )
        with pytest.raises(AttributeError):
            config.fk_column = "other"


@pytest.mark.unit
def test_tableconfig_parallel_workers_defaults_to_one():
    from DSI.connectors.mssql.models import TableConfig

    cfg = TableConfig(
        output_name="o",
        source_schema="dbo",
        source_table="t",
        target_table="t",
        description="d",
    )
    assert cfg.parallel_workers == 1
    cfg2 = TableConfig(
        output_name="o",
        source_schema="dbo",
        source_table="t",
        target_table="t",
        description="d",
        parallel_workers=8,
    )
    assert cfg2.parallel_workers == 8
