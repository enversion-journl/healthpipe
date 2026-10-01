# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for build_fhir_archive_query().

Tests:
  1. Full-load mode — no WHERE filter beyond deleted-row filter
  2. Period mode — bounded date range with >= and < DATEADD
  3. IDs mode — inline values and filter-table subquery
  4. Delta mode — checkpoint-based > comparison
  5. Lookback — DATEADD subtraction on checkpoint
  6. Options — include_deleted, select_clause
  7. SQL injection safety — identifier validation
"""

import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query

pytestmark = pytest.mark.unit

# ===========================================================================
# Helpers
# ===========================================================================


def _cfg(mode: str = "delta", **kwargs) -> RunModeConfig:
    """Shorthand for creating a RunModeConfig with overrides."""
    return RunModeConfig(mode=mode, **kwargs)


_TABLE = "arc.ifdb_encounter"


# ===========================================================================
# full_load mode
# ===========================================================================


class TestFhirQueryFullLoad:
    """full_load returns all rows (no checkpoint filter)."""

    def test_full_load_returns_base_query_only(self):
        result = build_fhir_archive_query(_TABLE, "2025-01-01 00:00:00", _cfg("full_load"))
        assert "SELECT *" in result
        assert "[arc].[ifdb_encounter]" in result
        assert "dw_timestamp_update" not in result

    def test_full_load_excludes_deleted_by_default(self):
        result = build_fhir_archive_query(_TABLE, None, _cfg("full_load"))
        assert "[dw_is_deleted] = 0" in result

    def test_full_load_ignores_checkpoint(self):
        result = build_fhir_archive_query(_TABLE, "2025-06-01 10:00:00", _cfg("full_load"))
        assert "2025-06-01" not in result


# ===========================================================================
# period mode
# ===========================================================================


class TestFhirQueryPeriod:
    """period mode creates a bounded date range."""

    def test_basic_period(self):
        cfg = _cfg("period", date_from="2025-01-01", date_to="2025-01-31")
        result = build_fhir_archive_query(_TABLE, None, cfg)
        assert ">= '2025-01-01'" in result
        assert "DATEADD(day, 1, CAST('2025-01-31' AS DATE))" in result

    def test_period_uses_default_delta_col(self):
        cfg = _cfg("period", date_from="2025-03-01", date_to="2025-03-31")
        result = build_fhir_archive_query(_TABLE, None, cfg)
        assert "[dw_timestamp_update]" in result

    def test_period_ignores_checkpoint(self):
        cfg = _cfg("period", date_from="2025-03-01", date_to="2025-03-31")
        result = build_fhir_archive_query(_TABLE, "should-not-appear", cfg)
        assert "should-not-appear" not in result

    def test_period_without_date_from_raises_validation_error(self):
        with pytest.raises(Exception, match="date_from"):
            _cfg("period")

    def test_period_without_date_to_gives_open_ended(self):
        cfg = _cfg("period", date_from="2025-03-01")
        result = build_fhir_archive_query(_TABLE, None, cfg)
        assert ">= '2025-03-01'" in result
        assert "DATEADD" not in result


# ===========================================================================
# ids mode
# ===========================================================================


class TestFhirQueryIds:
    """ids mode creates IN (...) clauses."""

    def test_inline_values(self):
        cfg = _cfg("ids", id_filter_column="OBJECT_ID", id_filter_values=["A", "B", "C"])
        result = build_fhir_archive_query(_TABLE, None, cfg)
        assert "[OBJECT_ID] IN" in result
        assert "'A'" in result
        assert "'B'" in result
        assert "'C'" in result

    def test_filter_table(self):
        cfg = _cfg("ids", id_filter_column="CPR", id_filter_table="tmp.my_ids")
        result = build_fhir_archive_query(_TABLE, None, cfg)
        assert "[CPR] IN" in result
        assert "SELECT [flt].[CPR] FROM [tmp].[my_ids] AS [flt]" in result

    def test_filter_table_takes_precedence_over_values(self):
        cfg = _cfg(
            "ids",
            id_filter_column="CPR",
            id_filter_table="tmp.ids",
            id_filter_values=["should_not_appear"],
        )
        result = build_fhir_archive_query(_TABLE, None, cfg)
        assert "[tmp].[ids]" in result
        assert "should_not_appear" not in result

    def test_ids_without_id_filter_column_raises_validation_error(self):
        with pytest.raises(Exception, match="id_filter_column"):
            _cfg("ids", id_filter_values=["X"])

    def test_ids_with_single_quote_in_value(self):
        cfg = _cfg("ids", id_filter_column="name", id_filter_values=["O'Brien"])
        result = build_fhir_archive_query(_TABLE, None, cfg)
        assert "O''Brien" in result


# ===========================================================================
# delta mode (checkpoint-based)
# ===========================================================================


class TestFhirQueryDelta:
    """delta mode filters by checkpoint using > comparison."""

    def test_basic_delta(self):
        result = build_fhir_archive_query(_TABLE, "2025-05-08 10:30:00", _cfg("delta"))
        assert "[dw_timestamp_update] > '2025-05-08 10:30:00'" in result

    def test_no_checkpoint_returns_no_filter(self):
        result = build_fhir_archive_query(_TABLE, None, _cfg("delta"))
        assert "dw_timestamp_update" not in result

    def test_checkpoint_with_single_quote_raises_validation_error(self):
        with pytest.raises(ValueError, match="Invalid checkpoint value"):
            build_fhir_archive_query(_TABLE, "2025-01-01'injected", _cfg("delta"))

    def test_custom_delta_column(self):
        cfg = _cfg("delta", delta_column="IFDB_MODIFICATION_DATETIME")
        result = build_fhir_archive_query(_TABLE, "2025-01-01 00:00:00", cfg)
        assert "[IFDB_MODIFICATION_DATETIME] >" in result
        assert "dw_timestamp_update" not in result


# ===========================================================================
# lookback
# ===========================================================================


class TestFhirQueryLookback:
    """lookback_days adjusts checkpoint via DATEADD."""

    def test_lookback_adds_dateadd(self):
        cfg = _cfg("delta", lookback_days=7)
        result = build_fhir_archive_query(_TABLE, "2025-05-08 10:00:00", cfg)
        assert "DATEADD(day, -7," in result
        assert "2025-05-08 10:00:00" in result

    def test_zero_lookback_no_dateadd(self):
        cfg = _cfg("delta", lookback_days=0)
        result = build_fhir_archive_query(_TABLE, "2025-01-01 00:00:00", cfg)
        assert "DATEADD" not in result


# ===========================================================================
# Options — include_deleted, select_clause
# ===========================================================================


class TestFhirQueryOptions:
    """Optional parameters: include_deleted, select_clause."""

    def test_include_deleted_removes_filter(self):
        result = build_fhir_archive_query(_TABLE, None, _cfg("full_load"), include_deleted=True)
        assert "dw_is_deleted" not in result

    def test_default_excludes_deleted(self):
        result = build_fhir_archive_query(_TABLE, None, _cfg("full_load"))
        assert "[dw_is_deleted] = 0" in result

    def test_custom_select_clause(self):
        result = build_fhir_archive_query(
            _TABLE,
            None,
            _cfg("full_load"),
            select_clause="SELECT DISTINCT [LOCATION_SORCODE]",
        )
        assert "SELECT DISTINCT [LOCATION_SORCODE]" in result
        assert "SELECT *" not in result


# ===========================================================================
# SQL injection safety
# ===========================================================================


class TestFhirQueryOrderBy:
    """order_by parameter appends ORDER BY to the query."""

    def test_order_by_appended(self):
        result = build_fhir_archive_query(_TABLE, None, _cfg("full_load"), order_by="[OBJECT_ID]")
        assert result.endswith(" ORDER BY [OBJECT_ID]")

    def test_order_by_none_no_clause(self):
        result = build_fhir_archive_query(_TABLE, None, _cfg("full_load"))
        assert "ORDER BY" not in result

    def test_order_by_with_delta_filter(self):
        result = build_fhir_archive_query(
            _TABLE, "2025-01-01 00:00:00", _cfg("delta"), order_by="[OBJECT_ID]"
        )
        assert "[dw_timestamp_update] > '2025-01-01 00:00:00'" in result
        assert result.endswith(" ORDER BY [OBJECT_ID]")

    def test_order_by_with_period_filter(self):
        cfg = _cfg("period", date_from="2025-01-01", date_to="2025-01-31")
        result = build_fhir_archive_query(_TABLE, None, cfg, order_by="[OBJECT_ID]")
        assert ">= '2025-01-01'" in result
        assert result.endswith(" ORDER BY [OBJECT_ID]")

    def test_order_by_multi_column(self):
        result = build_fhir_archive_query(
            _TABLE, None, _cfg("full_load"), order_by="[OBJECT_ID], [dw_timestamp_update]"
        )
        assert "ORDER BY [OBJECT_ID], [dw_timestamp_update]" in result


# ===========================================================================
# SQL injection safety
# ===========================================================================


class TestFhirQueryInjectionSafety:
    """Identifier validation prevents SQL injection."""

    def test_malicious_delta_column_raises(self):
        cfg = _cfg("delta", delta_column="]; DROP TABLE arc.ifdb_patient; --")
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            build_fhir_archive_query(_TABLE, "2025-01-01", cfg)

    def test_malicious_id_filter_column_raises(self):
        cfg = _cfg("ids", id_filter_column="id]; DROP TABLE x; --", id_filter_values=["A"])
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            build_fhir_archive_query(_TABLE, None, cfg)

    def test_malicious_filter_table_raises(self):
        cfg = _cfg("ids", id_filter_column="id", id_filter_table="tmp]; DROP TABLE x; --")
        with pytest.raises(ValueError, match="Invalid"):
            build_fhir_archive_query(_TABLE, None, cfg)

    def test_valid_bracket_quoted_column_accepted(self):
        cfg = _cfg("delta", delta_column="[dw_timestamp_update]")
        result = build_fhir_archive_query(_TABLE, "2025-01-01", cfg)
        assert "[dw_timestamp_update]" in result
