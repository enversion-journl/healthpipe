# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for SQL on FHIR data extraction.

Tests the end-to-end flow: create ViewDefinition -> query sof view -> verify data.
"""

import pytest
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_RESOURCE_CONFIGS,
    ensure_view_definition,
    extract_sof_view,
    load_view_definitions,
)


@pytest.mark.integration
class TestSofExtraction:
    """Test data extraction from sof views."""

    @pytest.fixture(autouse=True)
    def _ensure_views(self, fhir_server, aidbox_available):
        """Ensure all ViewDefinitions are materialized before extraction tests."""
        if not aidbox_available:
            pytest.skip("Aidbox not available")
        for view_def in load_view_definitions():
            ensure_view_definition(fhir_server, view_def)

    @pytest.mark.parametrize(
        "config_name",
        list(SOF_RESOURCE_CONFIGS.keys()),
        ids=list(SOF_RESOURCE_CONFIGS.keys()),
    )
    def test_extract_sof_view_runs_without_error(self, fhir_database, config_name):
        """Each sof view should be queryable without errors (no partition filter)."""
        config = SOF_RESOURCE_CONFIGS[config_name]
        rows = extract_sof_view(
            fhir_database,
            view_name=config["view_name"],
            resource_table=config["resource_table"],
            id_column=config["id_column"],
        )
        # Should return a list (possibly empty if no data)
        assert isinstance(rows, list)

    def test_patient_view_returns_expected_columns(self, fhir_database):
        """Patient sof view should return the expected column set."""
        config = SOF_RESOURCE_CONFIGS["patient_view"]
        rows = extract_sof_view(
            fhir_database,
            view_name=config["view_name"],
            resource_table=config["resource_table"],
            id_column=config["id_column"],
        )
        if rows:
            row_keys = set(rows[0].keys())
            expected = {
                "patient_id",
                "active",
                "gender",
                "dob",
                "patient_ssn",
                "given",
                "family",
                "last_updated",
                "created_at",
            }
            missing = expected - row_keys
            assert not missing, f"Missing columns: {missing}"

    def test_extract_with_partition_filter(self, fhir_database):
        """Extraction with partition filter should succeed (even if 0 rows)."""
        config = SOF_RESOURCE_CONFIGS["patient_view"]
        rows = extract_sof_view(
            fhir_database,
            view_name=config["view_name"],
            resource_table=config["resource_table"],
            id_column=config["id_column"],
            partition_start="2020-01-01T00:00:00+00:00",
            partition_end="2020-01-02T00:00:00+00:00",
        )
        assert isinstance(rows, list)

    def test_encounter_location_view_returns_expected_columns(self, fhir_database):
        """Encounter location sof view should return the expected column set."""
        config = SOF_RESOURCE_CONFIGS["encounter_location_view"]
        rows = extract_sof_view(
            fhir_database,
            view_name=config["view_name"],
            resource_table=config["resource_table"],
            id_column=config["id_column"],
        )
        if rows:
            row_keys = set(rows[0].keys())
            expected = {
                "encounter_id",
                "location_id",
                "location_status",
                "location_period_start",
                "location_period_end",
                "last_updated",
                "created_at",
            }
            missing = expected - row_keys
            assert not missing, f"Missing columns: {missing}"
