# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for DSI extraction layer configurations.

These tests verify that the extraction configurations for IFDB entities
use valid column names that match the actual Oracle database schema.
"""

import pytest
from DSI.connectors.oracle.models import (
    TableExtractionConfig,
    build_partition_query,
)
from DSI.oracle import (
    ENCOUNTER_CONFIG,
    EPISODE_OF_CARE_CONFIG,
    LOCATION_CONFIG,
    PATIENT_CONFIG,
)

pytestmark = pytest.mark.integration


class TestLocationConfig:
    """Tests for LOCATION extraction configuration."""

    def test_location_config_table_name(self):
        """Verify location config uses correct table."""
        assert LOCATION_CONFIG.table == "IFDB.LOCATION"

    def test_location_config_valid_columns(self):
        """Verify location config uses valid columns (no STATUS, PERIOD_*, LOCATION_TYPE)."""
        invalid_columns = ["STATUS", "PERIOD_START", "PERIOD_END", "LOCATION_TYPE"]
        for col in invalid_columns:
            assert col not in LOCATION_CONFIG.columns, f"Invalid column present: {col}"

    def test_location_config_has_required_columns(self):
        """Verify location config has columns used by FHIR transform."""
        required_columns = [
            "OBJECT_ID",
            "LOCATION_CODE",
            "LOCATION_NAME",
            "LOCATION_SORCODE",
        ]
        for col in required_columns:
            assert col in LOCATION_CONFIG.columns, f"Missing required column: {col}"

    def test_location_config_no_partition_column(self):
        """Verify location config has no partition column (static data)."""
        assert LOCATION_CONFIG.partition_column is None

    def test_location_full_load_query_builds(self):
        """Verify full load query can be built for location."""
        query = build_partition_query(LOCATION_CONFIG, "full_load", "full_load")
        assert "SELECT" in query
        assert "IFDB.LOCATION" in query
        assert "WHERE" not in query


class TestPatientConfig:
    """Tests for Patient extraction configuration."""

    def test_patient_config_table_name(self):
        """Verify patient config uses correct table."""
        assert PATIENT_CONFIG.table == "IFDB.PATIENT_OPLYSNINGER_COLUMNA"

    def test_patient_config_partition_column(self):
        """Verify patient config uses valid partition column."""
        assert PATIENT_CONFIG.partition_column == "MODTIDSPUNKT"

    def test_patient_daily_query_builds(self):
        """Verify daily partition query can be built."""
        query = build_partition_query(PATIENT_CONFIG, "2025-01-15", "daily")
        assert "SELECT" in query
        assert "IFDB.PATIENT_OPLYSNINGER_COLUMNA" in query


class TestEpisodeOfCareConfig:
    """Tests for Episode of Care extraction configuration."""

    def test_episode_of_care_config_table_name(self):
        """Verify episode of care config uses correct table."""
        assert EPISODE_OF_CARE_CONFIG.table == "IFDB.EPISODE_OF_CARE"

    def test_episode_of_care_config_partition_column(self):
        """Verify episode of care config uses valid partition column."""
        assert EPISODE_OF_CARE_CONFIG.partition_column == "IFDB_MODIFICATION_DATETIME"

    def test_episode_of_care_daily_query_builds(self):
        """Verify daily partition query can be built."""
        query = build_partition_query(EPISODE_OF_CARE_CONFIG, "2025-01-15", "daily")
        assert "SELECT" in query
        assert "IFDB.EPISODE_OF_CARE" in query


class TestEncounterConfig:
    """Tests for Encounter extraction configuration."""

    def test_encounter_config_table_name(self):
        """Verify encounter config uses correct table."""
        assert ENCOUNTER_CONFIG.table == "IFDB.ENCOUNTER"

    def test_encounter_config_partition_column(self):
        """Verify encounter config uses valid partition column."""
        assert ENCOUNTER_CONFIG.partition_column == "EPJI_MODIFICATION_DATETIME"


# The ServiceRequest source configs (HENVISNING, HENVISNING_HISTORIK,
# REFERRALINFORMATION) moved to the DFM archive layer; their
# extract contract is now covered by test_servicerequest_pipeline_registration
# and the DfmExtractConfig column/type checks.


class TestAllConfigsQueryGeneration:
    """Tests verifying all configs can generate valid queries."""

    @pytest.mark.parametrize(
        "config,config_name",
        [
            (PATIENT_CONFIG, "patient"),
            (LOCATION_CONFIG, "location"),
            (EPISODE_OF_CARE_CONFIG, "episode_of_care"),
            (ENCOUNTER_CONFIG, "encounter"),
        ],
    )
    def test_full_load_query_generates(self, config: TableExtractionConfig, config_name: str):
        """Verify full load query generation works for all configs."""
        query = build_partition_query(config, "full_load", "full_load")
        assert "SELECT" in query, f"{config_name}: Query should contain SELECT"
        assert config.table in query, f"{config_name}: Query should contain table name"

    @pytest.mark.parametrize(
        "config,config_name",
        [
            (PATIENT_CONFIG, "patient"),
            (EPISODE_OF_CARE_CONFIG, "episode_of_care"),
            (ENCOUNTER_CONFIG, "encounter"),
        ],
    )
    def test_daily_query_generates_for_partitioned_configs(
        self, config: TableExtractionConfig, config_name: str
    ):
        """Verify daily query generation works for partitioned configs."""
        if config.partition_column is None:
            pytest.skip(f"{config_name} has no partition column")

        query = build_partition_query(config, "2025-01-15", "daily")
        assert "WHERE" in query, f"{config_name}: Daily query should have WHERE clause"
        assert config.partition_column in query

    @pytest.mark.parametrize(
        "config,config_name",
        [
            (PATIENT_CONFIG, "patient"),
            (EPISODE_OF_CARE_CONFIG, "episode_of_care"),
            (ENCOUNTER_CONFIG, "encounter"),
        ],
    )
    def test_monthly_query_generates_for_partitioned_configs(
        self, config: TableExtractionConfig, config_name: str
    ):
        """Verify monthly query generation works for partitioned configs."""
        if config.partition_column is None:
            pytest.skip(f"{config_name} has no partition column")

        query = build_partition_query(config, "2025-01", "monthly")
        assert "WHERE" in query, f"{config_name}: Monthly query should have WHERE clause"
        assert "2025-01-01" in query
        assert "2025-02-01" in query


class TestConfigColumnConsistency:
    """Tests verifying column consistency between extraction and transform layers."""

    def test_location_columns_match_transform_expectations(self):
        """Verify location extraction columns match what transform expects."""
        transform_expected = {
            "LOCATION_CODE",
            "LOCATION_NAME",
            "LOCATION_SORCODE",
        }
        extraction_columns = set(LOCATION_CONFIG.columns)
        missing = transform_expected - extraction_columns
        assert not missing, f"Transform expects columns not in extraction: {missing}"
