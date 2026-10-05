# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for DSI extraction layer.
"""

import pytest
from DSI.connectors.oracle.models import (
    TableExtractionConfig,
    build_partition_query,
)

pytestmark = pytest.mark.integration


class TestBuildPartitionQuery:
    """Tests for build_partition_query function."""

    def test_build_daily_partition_query(self):
        """Test building a query with daily partition filter."""
        config = TableExtractionConfig(
            table="IFDB.PATIENT_OPLYSNINGER_COLUMNA",
            columns=["PATIENT_ID", "CPR", "FORNAVNE"],
            partition_column="MODTIDSPUNKT",
            description="Test config",
        )

        query = build_partition_query(config, "2025-01-15", "daily")

        assert "SELECT PATIENT_ID, CPR, FORNAVNE FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA" in query
        assert "MODTIDSPUNKT >= TO_DATE('2025-01-15', 'YYYY-MM-DD')" in query
        assert "MODTIDSPUNKT < TO_DATE('2025-01-16', 'YYYY-MM-DD')" in query

    def test_build_monthly_partition_query(self):
        """Test building a query with monthly partition filter."""
        config = TableExtractionConfig(
            table="IFDB.HEALTHISSUE",
            columns=["OBJECT_ID", "PATIENT_ID"],
            partition_column="IFDB_MODIFICATION_DATETIME",
            description="Test config",
        )

        query = build_partition_query(config, "2025-01", "monthly")

        assert "SELECT OBJECT_ID, PATIENT_ID FROM IFDB.HEALTHISSUE" in query
        assert "IFDB_MODIFICATION_DATETIME >= TO_DATE('2025-01-01', 'YYYY-MM-DD')" in query
        assert "IFDB_MODIFICATION_DATETIME < TO_DATE('2025-02-01', 'YYYY-MM-DD')" in query

    def test_build_full_load_partition_query(self):
        """Test building a query without partition filter for full load."""
        config = TableExtractionConfig(
            table="IFDB.ENCOUNTER",
            columns=["OBJECT_ID", "PATIENT_ID", "EPISODE_OF_CARE_ID"],
            partition_column="EPJI_MODIFICATION_DATETIME",
            description="Test config",
        )

        query = build_partition_query(config, "full_load", "full_load")

        assert "SELECT OBJECT_ID, PATIENT_ID, EPISODE_OF_CARE_ID FROM IFDB.ENCOUNTER" in query
        assert "WHERE" not in query

    def test_build_query_with_additional_where(self):
        """Test building a query with additional WHERE conditions."""
        config = TableExtractionConfig(
            table="IFDB.EVENTMARKER",
            columns=["OBJECT_ID", "PATIENT_ID"],
            partition_column="IFDB_MODIFICATION_DATETIME",
            additional_where="DELETED = 0",
            description="Test config",
        )

        query = build_partition_query(config, "2025-01-15", "daily")

        assert "DELETED = 0" in query
        assert "IFDB_MODIFICATION_DATETIME" in query

    def test_invalid_partition_type_raises_error(self):
        """Test that invalid partition type raises ValueError."""
        config = TableExtractionConfig(
            table="IFDB.PATIENT_OPLYSNINGER_COLUMNA",
            columns=["PATIENT_ID"],
            partition_column="MODTIDSPUNKT",
            description="Test config",
        )

        with pytest.raises(ValueError, match="Invalid partition type"):
            build_partition_query(config, "2025-01-15", "invalid")


class TestTableExtractionConfig:
    """Tests for TableExtractionConfig dataclass."""

    def test_config_is_frozen(self):
        """Test that config is immutable (frozen dataclass)."""
        config = TableExtractionConfig(
            table="IFDB.PATIENT_OPLYSNINGER_COLUMNA",
            columns=["PATIENT_ID"],
            partition_column="MODTIDSPUNKT",
        )

        with pytest.raises(AttributeError):
            config.table = "IFDB.OTHER_TABLE"

    def test_config_with_none_partition_column(self):
        """Test config without partition column (for non-temporal tables)."""
        config = TableExtractionConfig(
            table="IFDB.HOSPITAL",
            columns=["OBJECT_ID", "NAME"],
            partition_column=None,
            description="Static organization data",
        )

        assert config.partition_column is None
        query = build_partition_query(config, "full_load", "full_load")
        assert "WHERE" not in query


class TestFirstPartitionQueryOracle:
    """Tests for first-partition upper-bound-only filtering in IFDB Oracle queries."""

    def _make_config(self) -> TableExtractionConfig:
        return TableExtractionConfig(
            table="IFDB.ENCOUNTER",
            columns=["OBJECT_ID", "PATIENT_ID"],
            partition_column="EPJI_MODIFICATION_DATETIME",
            description="Test config",
        )

    def test_first_partition_daily_upper_bound_only(self):
        """First daily partition should produce an upper-bound-only filter."""
        config = self._make_config()
        query = build_partition_query(config, "2021-01-01", "daily", is_first_partition=True)
        assert "EPJI_MODIFICATION_DATETIME < TO_DATE('2021-01-02', 'YYYY-MM-DD')" in query
        assert ">=" not in query

    def test_first_partition_monthly_upper_bound_only(self):
        """First monthly partition should produce an upper-bound-only filter."""
        config = self._make_config()
        query = build_partition_query(config, "2021-01", "monthly", is_first_partition=True)
        assert "EPJI_MODIFICATION_DATETIME < TO_DATE('2021-02-01', 'YYYY-MM-DD')" in query
        assert ">=" not in query

    def test_non_first_daily_range(self):
        """Non-first daily partition should produce a sargable range filter."""
        config = self._make_config()
        query = build_partition_query(config, "2025-06-15", "daily", is_first_partition=False)
        assert "EPJI_MODIFICATION_DATETIME >= TO_DATE('2025-06-15', 'YYYY-MM-DD')" in query
        assert "EPJI_MODIFICATION_DATETIME < TO_DATE('2025-06-16', 'YYYY-MM-DD')" in query

    def test_non_first_monthly_range(self):
        """Non-first monthly partition should produce a sargable range filter."""
        config = self._make_config()
        query = build_partition_query(config, "2025-06", "monthly", is_first_partition=False)
        assert "EPJI_MODIFICATION_DATETIME >= TO_DATE('2025-06-01', 'YYYY-MM-DD')" in query
        assert "EPJI_MODIFICATION_DATETIME < TO_DATE('2025-07-01', 'YYYY-MM-DD')" in query

    def test_scheduled_catchup_overrides_first_partition(self):
        """Scheduled catchup should take precedence over first-partition logic."""
        config = self._make_config()
        query = build_partition_query(
            config,
            "2021-01-01",
            "daily",
            is_scheduled_catchup=True,
            is_first_partition=True,
        )
        assert "TO_DATE(" in query
        assert "05:00:00" in query
        assert "<=" not in query

    def test_first_partition_with_additional_where(self):
        """First partition should still include additional_where conditions."""
        config = TableExtractionConfig(
            table="IFDB.EVENTMARKER",
            columns=["OBJECT_ID", "PATIENT_ID"],
            partition_column="IFDB_MODIFICATION_DATETIME",
            additional_where="DELETED = 0",
            description="Test config",
        )
        query = build_partition_query(config, "2021-01-01", "daily", is_first_partition=True)
        assert "IFDB_MODIFICATION_DATETIME < TO_DATE('2021-01-02', 'YYYY-MM-DD')" in query
        assert "DELETED = 0" in query

    def test_full_load_ignores_first_partition(self):
        """Full load should never apply date filtering, even if is_first_partition is True."""
        config = self._make_config()
        query = build_partition_query(config, "full_load", "full_load", is_first_partition=True)
        assert "WHERE" not in query or "EPJI_MODIFICATION_DATETIME" not in query


class TestSargableQuerySyntax:
    """Verify queries never use TRUNC(CAST(...)) which prevents index usage."""

    def _make_config(self) -> TableExtractionConfig:
        return TableExtractionConfig(
            table="IFDB.AKTIVITETSRESULTAT",
            columns=["OBJEKT_ID", "PATIENT_ID"],
            partition_column="OPDATERET_TID",
            description="Test config",
        )

    def test_daily_no_trunc_cast(self):
        query = build_partition_query(self._make_config(), "2025-03-20", "daily")
        assert "TRUNC" not in query
        assert "CAST" not in query

    def test_monthly_no_trunc_cast(self):
        query = build_partition_query(self._make_config(), "2025-03", "monthly")
        assert "TRUNC" not in query
        assert "CAST" not in query

    def test_first_partition_daily_no_trunc_cast(self):
        query = build_partition_query(
            self._make_config(), "2021-01-01", "daily", is_first_partition=True
        )
        assert "TRUNC" not in query
        assert "CAST" not in query

    def test_first_partition_monthly_no_trunc_cast(self):
        query = build_partition_query(
            self._make_config(), "2021-01", "monthly", is_first_partition=True
        )
        assert "TRUNC" not in query
        assert "CAST" not in query

    def test_no_fetch_first_limit(self):
        query = build_partition_query(self._make_config(), "2025-03-20", "daily")
        assert "FETCH FIRST" not in query
        assert "TOP " not in query
