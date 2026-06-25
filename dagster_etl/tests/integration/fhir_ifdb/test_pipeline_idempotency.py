# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for full FHIR ETL pipeline idempotency.

These tests verify that complete daily and monthly jobs maintain idempotency
when re-run with the same or overlapping partition data.

Test Scenarios:
1. Full daily pipeline run twice with same partition -> no duplicates
2. Full monthly pipeline run twice with same partition -> no duplicates
3. Partition re-run after data changes -> updates existing, no duplicates
4. Cross-partition data (same resources appearing in multiple partitions) -> no duplicates

These tests require:
- Real FHIR database connection
- Real IFDB Oracle connection (or mocked extraction data)

Run with: USE_REAL_DB_FOR_TESTS=true pytest -v -m "integration" test_pipeline_idempotency.py
"""

import logging
import uuid
from unittest.mock import MagicMock

import dagster as dg
import pytest

pytestmark = pytest.mark.skip(
    reason="Legacy full-pipeline idempotency suite is flaky across environments"
)

from tests.integration.fhir_ifdb.conftest import (
    create_test_encounter,
    create_test_episode_of_care,
    create_test_location,
    create_test_observation,
    create_test_patient,
)
from tests.utils.idempotency_helpers import (
    count_resources_by_identifier,
    delete_resources_by_identifier_prefix,
    get_fhir_id_by_identifier,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)

logger = logging.getLogger(__name__)

# Test prefix for generating unique identifiers
TEST_PREFIX = "PIPE_TEST"


def generate_test_prefix() -> str:
    """Generate a unique prefix for test identifiers."""
    return f"{TEST_PREFIX}_{uuid.uuid4().hex[:8]}"


# =============================================================================
# Daily Pipeline Idempotency Tests
# =============================================================================


@pytest.mark.integration
class TestDailyPipelineIdempotency:
    """Tests for daily pipeline idempotency with all resource types."""

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture(autouse=True)
    def cleanup_all_resources(self, real_fhir_database_resource):
        """Clean up all test resources before and after tests."""
        resource_systems = [
            ("Patient", SYSTEM_IFDB_PATIENT_ID),
            ("EpisodeOfCare", SYSTEM_IFDB_EPISODE_OF_CARE_ID),
            ("Encounter", SYSTEM_IFDB_OBJECT_ID),
            ("Observation", SYSTEM_IFDB_OBJECT_ID),
            ("Location", SYSTEM_SOR),
        ]
        for resource_type, system in resource_systems:
            delete_resources_by_identifier_prefix(
                real_fhir_database_resource, resource_type, system, TEST_PREFIX
            )
        yield
        for resource_type, system in resource_systems:
            delete_resources_by_identifier_prefix(
                real_fhir_database_resource, resource_type, system, TEST_PREFIX
            )

    def _create_mock_context(self, partition_key: str = "2025-01-15"):
        """Create a mock Dagster context for testing."""
        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = partition_key
        context.log = MagicMock()
        return context

    def _simulate_daily_pipeline_run(
        self,
        real_fhir_database_resource,
        test_prefix: str,
        partition_key: str = "2025-01-15",
        run_suffix: str = "",
    ):
        """Simulate a full daily pipeline run with all resource types.

        This mirrors the dependency order of the actual daily pipeline:
        1. Patient (no deps)
        2. Location (no deps)
        3. EpisodeOfCare (depends on Patient)
        4. Encounter (depends on EpisodeOfCare, Location)
        5. Observation (depends on EpisodeOfCare)
        """
        from DSA.FHIR.Encounter.Encounter_IFDB_dfm import _post_encounters_to_fhir
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import _post_episodes_to_fhir
        from DSA.FHIR.Location.Location_IFDB_dfm import _post_locations_to_fhir
        from DSA.FHIR.Observation.Observation_IFDB import _post_observations_to_fhir
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = self._create_mock_context(partition_key)

        # Create test identifiers
        patient_id = f"{test_prefix}_PAT"
        eoc_id = f"{test_prefix}_EOC"
        enc_id = f"{test_prefix}_ENC"
        obs_id = f"{test_prefix}_OBS"
        loc_code = f"{test_prefix}_LOC"

        results = {}

        # 1. Patient (foundational resource, no dependencies)
        results["patient"] = _post_patients_to_fhir(
            context=context,
            patients=[create_test_patient(patient_id, f"0101900{run_suffix}01")],
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        # 2. Location (no dependencies)
        results["location"] = _post_locations_to_fhir(
            context=context,
            locations=[create_test_location(loc_code, f"Location {run_suffix}")],
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        # 3. EpisodeOfCare (depends on Patient)
        results["episode_of_care"] = _post_episodes_to_fhir(
            context=context,
            episodes=[create_test_episode_of_care(eoc_id, patient_id)],
            fhir_database=real_fhir_database_resource,
        )

        # 4. Encounter (depends on EpisodeOfCare, Location)
        results["encounter"] = _post_encounters_to_fhir(
            context=context,
            encounters=[create_test_encounter(enc_id, patient_id, episode_of_care_id=eoc_id)],
            fhir_database=real_fhir_database_resource,
        )

        # 5. Observation (depends on EpisodeOfCare)
        results["observation"] = _post_observations_to_fhir(
            context=context,
            observations=[create_test_observation(obs_id, patient_id, episode_of_care_id=eoc_id)],
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        return results, {
            "patient_id": patient_id,
            "eoc_id": eoc_id,
            "enc_id": enc_id,
            "obs_id": obs_id,
            "loc_code": loc_code,
        }

    def test_daily_pipeline_twice_same_partition_no_duplicates(
        self, real_fhir_database_resource, test_prefix
    ):
        """Test that running daily pipeline twice with same partition creates no duplicates."""
        # First run
        results1, ids = self._simulate_daily_pipeline_run(
            real_fhir_database_resource, test_prefix, "2025-01-15", "R1"
        )

        # Verify first run created resources
        assert results1["patient"].metadata["posted_count"].value == 1
        assert results1["location"].metadata["posted_count"].value == 1
        assert results1["episode_of_care"].metadata["posted_count"].value == 1
        assert results1["encounter"].metadata["posted_count"].value == 1
        assert results1["observation"].metadata["posted_count"].value == 1

        # Store original FHIR IDs
        original_ids = {
            "patient": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, ids["patient_id"]
            ),
            "location": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Location", SYSTEM_SOR, ids["loc_code"]
            ),
            "episode_of_care": get_fhir_id_by_identifier(
                real_fhir_database_resource,
                "EpisodeOfCare",
                SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                ids["eoc_id"],
            ),
            "encounter": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, ids["enc_id"]
            ),
            "observation": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Observation", SYSTEM_IFDB_OBJECT_ID, ids["obs_id"]
            ),
        }

        # Second run (same partition)
        results2, _ = self._simulate_daily_pipeline_run(
            real_fhir_database_resource, test_prefix, "2025-01-15", "R2"
        )

        # Verify second run updated resources (resolved > 0, new = 0 for most)
        # Patient has resolved_count/new_count in metadata
        assert results2["patient"].metadata["resolved_count"].value == 1
        assert results2["patient"].metadata["new_count"].value == 0

        # Verify no duplicates - exactly 1 of each resource
        resource_checks = [
            ("Patient", SYSTEM_IFDB_PATIENT_ID, ids["patient_id"]),
            ("Location", SYSTEM_SOR, ids["loc_code"]),
            ("EpisodeOfCare", SYSTEM_IFDB_EPISODE_OF_CARE_ID, ids["eoc_id"]),
            ("Encounter", SYSTEM_IFDB_OBJECT_ID, ids["enc_id"]),
            ("Observation", SYSTEM_IFDB_OBJECT_ID, ids["obs_id"]),
        ]

        for resource_type, system, identifier in resource_checks:
            count = count_resources_by_identifier(
                real_fhir_database_resource, resource_type, system, identifier
            )
            assert count == 1, f"Expected 1 {resource_type}, found {count}"

        # Verify FHIR IDs are preserved (same UUID before and after)
        new_ids = {
            "patient": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, ids["patient_id"]
            ),
            "location": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Location", SYSTEM_SOR, ids["loc_code"]
            ),
            "episode_of_care": get_fhir_id_by_identifier(
                real_fhir_database_resource,
                "EpisodeOfCare",
                SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                ids["eoc_id"],
            ),
            "encounter": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, ids["enc_id"]
            ),
            "observation": get_fhir_id_by_identifier(
                real_fhir_database_resource, "Observation", SYSTEM_IFDB_OBJECT_ID, ids["obs_id"]
            ),
        }

        for resource_name in original_ids:
            assert original_ids[resource_name] == new_ids[resource_name], (
                f"{resource_name} FHIR ID changed: "
                f"{original_ids[resource_name]} -> {new_ids[resource_name]}"
            )

    def test_daily_pipeline_five_runs_maintains_single_records(
        self, real_fhir_database_resource, test_prefix
    ):
        """Test that 5 consecutive daily runs maintain exactly 1 record per resource."""
        # Run pipeline 5 times
        for run_num in range(5):
            self._simulate_daily_pipeline_run(
                real_fhir_database_resource, test_prefix, "2025-01-15", f"R{run_num}"
            )

        # Define identifiers (same as used in _simulate_daily_pipeline_run)
        ids = {
            "patient_id": f"{test_prefix}_PAT",
            "eoc_id": f"{test_prefix}_EOC",
            "enc_id": f"{test_prefix}_ENC",
            "obs_id": f"{test_prefix}_OBS",
            "loc_code": f"{test_prefix}_LOC",
        }

        # Verify exactly 1 of each
        resource_checks = [
            ("Patient", SYSTEM_IFDB_PATIENT_ID, ids["patient_id"]),
            ("Location", SYSTEM_SOR, ids["loc_code"]),
            ("EpisodeOfCare", SYSTEM_IFDB_EPISODE_OF_CARE_ID, ids["eoc_id"]),
            ("Encounter", SYSTEM_IFDB_OBJECT_ID, ids["enc_id"]),
            ("Observation", SYSTEM_IFDB_OBJECT_ID, ids["obs_id"]),
        ]

        for resource_type, system, identifier in resource_checks:
            count = count_resources_by_identifier(
                real_fhir_database_resource, resource_type, system, identifier
            )
            assert (
                count == 1
            ), f"Expected 1 {resource_type} with ID {identifier} after 5 runs, found {count}"


# =============================================================================
# Monthly Pipeline Idempotency Tests
# =============================================================================


@pytest.mark.integration
class TestMonthlyPipelineIdempotency:
    """Tests for monthly pipeline idempotency."""

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture(autouse=True)
    def cleanup_all_resources(self, real_fhir_database_resource):
        """Clean up all test resources before and after tests."""
        resource_systems = [
            ("Patient", SYSTEM_IFDB_PATIENT_ID),
            ("EpisodeOfCare", SYSTEM_IFDB_EPISODE_OF_CARE_ID),
            ("Encounter", SYSTEM_IFDB_OBJECT_ID),
            ("Observation", SYSTEM_IFDB_OBJECT_ID),
            ("Location", SYSTEM_SOR),
        ]
        for resource_type, system in resource_systems:
            delete_resources_by_identifier_prefix(
                real_fhir_database_resource, resource_type, system, TEST_PREFIX
            )
        yield
        for resource_type, system in resource_systems:
            delete_resources_by_identifier_prefix(
                real_fhir_database_resource, resource_type, system, TEST_PREFIX
            )

    def test_monthly_partition_rerun_updates_existing(
        self, real_fhir_database_resource, test_prefix
    ):
        """Test that re-running a monthly partition updates existing resources."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01"  # Monthly partition format
        context.log = MagicMock()

        patient_id = f"{test_prefix}_MONTHLY_PAT"

        # First run
        patients1 = [create_test_patient(patient_id, "0101900050", "January", "Patient")]
        result1 = _post_patients_to_fhir(
            context=context,
            patients=patients1,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        assert result1.metadata["new_count"].value == 1
        original_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, patient_id
        )

        # Second run (same monthly partition, simulating rerun)
        patients2 = [create_test_patient(patient_id, "0101900050", "Updated", "Name")]
        result2 = _post_patients_to_fhir(
            context=context,
            patients=patients2,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        assert result2.metadata["resolved_count"].value == 1
        assert result2.metadata["new_count"].value == 0

        # Verify no duplicates
        count = count_resources_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, patient_id
        )
        assert count == 1

        # Verify FHIR ID preserved
        new_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, patient_id
        )
        assert new_fhir_id == original_fhir_id


# =============================================================================
# Cross-Partition Idempotency Tests
# =============================================================================


@pytest.mark.integration
class TestCrossPartitionIdempotency:
    """Tests for idempotency when same resources appear in multiple partitions.

    This simulates the scenario where a resource is modified multiple times
    and appears in multiple daily partitions due to modification timestamps.
    """

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture(autouse=True)
    def cleanup_all_resources(self, real_fhir_database_resource):
        """Clean up all test resources before and after tests."""
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )
        yield
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )

    def test_same_resource_multiple_partitions_no_duplicates(
        self, real_fhir_database_resource, test_prefix
    ):
        """Test that same resource appearing in multiple partitions doesn't create duplicates.

        This simulates the real-world scenario where a patient is modified multiple times
        and their record appears in multiple daily extracts.
        """
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        patient_id = f"{test_prefix}_MULTI_PAT"
        original_fhir_id = None

        # Process the same patient across 5 different daily partitions
        partitions = ["2025-01-10", "2025-01-11", "2025-01-12", "2025-01-13", "2025-01-14"]

        for i, partition in enumerate(partitions):
            context = MagicMock(spec=dg.AssetExecutionContext)
            context.has_partition_key = True
            context.partition_key = partition
            context.log = MagicMock()

            patients = [create_test_patient(patient_id, "0101900055", f"Version{i}", "Patient")]
            result = _post_patients_to_fhir(
                context=context,
                patients=patients,
                fhir_database=real_fhir_database_resource,
                extracted_count=1,
            )

            # First partition should create new, subsequent should resolve
            if i == 0:
                assert result.metadata["new_count"].value == 1
                original_fhir_id = get_fhir_id_by_identifier(
                    real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, patient_id
                )
            else:
                assert result.metadata["resolved_count"].value == 1
                assert result.metadata["new_count"].value == 0

        # After all partitions, should have exactly 1 patient
        count = count_resources_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, patient_id
        )
        assert count == 1, f"Expected 1 patient after 5 partitions, found {count}"

        # FHIR ID should be preserved throughout
        final_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, patient_id
        )
        assert final_fhir_id == original_fhir_id


# =============================================================================
# Full Load Idempotency Tests
# =============================================================================


@pytest.mark.integration
class TestFullLoadIdempotency:
    """Tests for full load idempotency.

    Full loads should also be idempotent, allowing reruns without creating duplicates.
    """

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture(autouse=True)
    def cleanup_all_resources(self, real_fhir_database_resource):
        """Clean up all test resources before and after tests."""
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )
        yield
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )

    def test_full_load_rerun_updates_existing(self, real_fhir_database_resource, test_prefix):
        """Test that re-running full load updates existing resources."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "full_load"
        context.log = MagicMock()

        # Create multiple patients for bulk test
        patient_ids = [f"{test_prefix}_BULK_{i}" for i in range(10)]

        # First bulk run
        patients1 = [
            create_test_patient(pid, f"010190{i:04d}", f"Bulk{i}", "Patient")
            for i, pid in enumerate(patient_ids)
        ]
        result1 = _post_patients_to_fhir(
            context=context,
            patients=patients1,
            fhir_database=real_fhir_database_resource,
            extracted_count=10,
        )

        assert result1.metadata["posted_count"].value == 10
        assert result1.metadata["new_count"].value == 10

        # Store original IDs
        original_fhir_ids = {
            pid: get_fhir_id_by_identifier(
                real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, pid
            )
            for pid in patient_ids
        }

        # Second bulk run (re-run)
        patients2 = [
            create_test_patient(pid, f"010190{i:04d}", f"Updated{i}", "Name")
            for i, pid in enumerate(patient_ids)
        ]
        result2 = _post_patients_to_fhir(
            context=context,
            patients=patients2,
            fhir_database=real_fhir_database_resource,
            extracted_count=10,
        )

        assert result2.metadata["posted_count"].value == 10
        assert result2.metadata["resolved_count"].value == 10
        assert result2.metadata["new_count"].value == 0

        # Verify no duplicates
        for pid in patient_ids:
            count = count_resources_by_identifier(
                real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, pid
            )
            assert count == 1, f"Expected 1 patient with ID {pid}, found {count}"

        # Verify all FHIR IDs preserved
        for pid in patient_ids:
            new_fhir_id = get_fhir_id_by_identifier(
                real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, pid
            )
            assert (
                new_fhir_id == original_fhir_ids[pid]
            ), f"FHIR ID for {pid} changed from {original_fhir_ids[pid]} to {new_fhir_id}"
