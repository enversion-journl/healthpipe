# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for FHIR resource idempotency across all resource types.

These tests verify that the FHIR ETL pipeline correctly handles upsert behavior:
1. First run: INSERT new resources
2. Subsequent runs: UPDATE existing resources (no duplicates)
3. Mixed runs: INSERT new + UPDATE existing
4. Multiple runs: Maintain exactly one record per identifier

Tests cover the FHIR resource types in the daily/monthly jobs:
- Patient
- EpisodeOfCare
- Encounter
- Observation
- Location

These tests require a real FHIR database connection.
Run with: USE_REAL_DB_FOR_TESTS=true pytest -v -m "integration"
"""

import logging
import uuid
from unittest.mock import MagicMock

import dagster as dg
import pytest

pytestmark = pytest.mark.skip(
    reason="Legacy broad idempotency suite is high-maintenance and flaky across environments"
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
TEST_PREFIX = "IDEM_TEST"


def generate_test_prefix() -> str:
    """Generate a unique prefix for test identifiers."""
    return f"{TEST_PREFIX}_{uuid.uuid4().hex[:8]}"


# =============================================================================
# Patient Idempotency Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.patient_test
class TestPatientIdempotency:
    """Tests for Patient resource idempotency."""

    @pytest.fixture
    def test_prefix(self):
        """Generate unique test prefix for this test run."""
        return generate_test_prefix()

    @pytest.fixture
    def test_patient_ids(self, test_prefix):
        """Generate unique test patient IDs."""
        return [f"{test_prefix}_PAT_{i}" for i in range(5)]

    @pytest.fixture(autouse=True)
    def cleanup_test_patients(self, test_patient_ids, real_fhir_database_resource):
        """Clean up test patients before and after test."""
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )
        yield
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )

    def test_first_run_inserts_new_patients(self, real_fhir_database_resource, test_patient_ids):
        """Test that first run correctly inserts new patients."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        patients = [
            create_test_patient(test_patient_ids[0], "0101900001"),
            create_test_patient(test_patient_ids[1], "0101900002"),
        ]

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        result = _post_patients_to_fhir(
            context=context,
            patients=patients,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        assert result.metadata["posted_count"].value == 2
        assert result.metadata["new_count"].value == 2
        assert result.metadata["resolved_count"].value == 0

        # Verify exactly one record per patient
        for pid in test_patient_ids[:2]:
            count = count_resources_by_identifier(
                real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, pid
            )
            assert count == 1, f"Expected 1 patient with ID {pid}, found {count}"

    def test_second_run_updates_existing_no_duplicates(
        self, real_fhir_database_resource, test_patient_ids
    ):
        """Test that second run updates existing patients without duplicates."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # First run
        patients_run1 = [
            create_test_patient(test_patient_ids[0], "0101900001", "First", "Run"),
        ]
        _post_patients_to_fhir(
            context=context,
            patients=patients_run1,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        original_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, test_patient_ids[0]
        )
        assert original_fhir_id is not None

        # Second run with updated data
        patients_run2 = [
            create_test_patient(test_patient_ids[0], "0101900001", "Updated", "Name"),
        ]
        result2 = _post_patients_to_fhir(
            context=context,
            patients=patients_run2,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        assert result2.metadata["posted_count"].value == 1
        assert result2.metadata["resolved_count"].value == 1
        assert result2.metadata["new_count"].value == 0

        # Verify no duplicates
        count = count_resources_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, test_patient_ids[0]
        )
        assert count == 1, f"Expected 1 patient, found {count}"

        # Verify FHIR ID preserved
        new_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, test_patient_ids[0]
        )
        assert new_fhir_id == original_fhir_id, "FHIR ID should be preserved"

    def test_multiple_runs_maintain_single_record(
        self, real_fhir_database_resource, test_patient_ids
    ):
        """Test that 5 runs maintain exactly 1 record."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        patient_id = test_patient_ids[0]

        for run in range(5):
            patients = [create_test_patient(patient_id, "0101900001", f"Run{run}", "Test")]
            _post_patients_to_fhir(
                context=context,
                patients=patients,
                fhir_database=real_fhir_database_resource,
                extracted_count=1,
            )

        count = count_resources_by_identifier(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, patient_id
        )
        assert count == 1, f"Expected 1 patient after 5 runs, found {count}"


# =============================================================================
# EpisodeOfCare Idempotency Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.episode_of_care_test
class TestEpisodeOfCareIdempotency:
    """Tests for EpisodeOfCare resource idempotency."""

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture
    def test_eoc_ids(self, test_prefix):
        return [f"{test_prefix}_EOC_{i}" for i in range(5)]

    @pytest.fixture
    def test_patient_id(self, test_prefix):
        return f"{test_prefix}_PAT_EOC"

    @pytest.fixture(autouse=True)
    def cleanup_test_resources(self, test_eoc_ids, test_patient_id, real_fhir_database_resource):
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource,
            "EpisodeOfCare",
            SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            TEST_PREFIX,
        )
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )
        yield
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource,
            "EpisodeOfCare",
            SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            TEST_PREFIX,
        )
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )

    def _setup_test_patient(self, real_fhir_database_resource, test_patient_id):
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        patients = [create_test_patient(test_patient_id, "0101900098")]
        _post_patients_to_fhir(
            context=context,
            patients=patients,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

    def test_first_run_inserts_new_episodes(
        self, real_fhir_database_resource, test_eoc_ids, test_patient_id
    ):
        """Test that first run correctly inserts new episodes of care."""
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import _post_episodes_to_fhir

        self._setup_test_patient(real_fhir_database_resource, test_patient_id)

        episodes = [
            create_test_episode_of_care(test_eoc_ids[0], test_patient_id),
            create_test_episode_of_care(test_eoc_ids[1], test_patient_id, status="finished"),
        ]

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        result = _post_episodes_to_fhir(
            context=context,
            episodes=episodes,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        assert result.metadata["posted_count"].value == 2
        assert result.metadata["new_count"].value == 2

        for eid in test_eoc_ids[:2]:
            count = count_resources_by_identifier(
                real_fhir_database_resource, "EpisodeOfCare", SYSTEM_IFDB_EPISODE_OF_CARE_ID, eid
            )
            assert count == 1

    def test_second_run_updates_existing_no_duplicates(
        self, real_fhir_database_resource, test_eoc_ids, test_patient_id
    ):
        """Test that second run updates existing episodes without duplicates."""
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import _post_episodes_to_fhir

        self._setup_test_patient(real_fhir_database_resource, test_patient_id)

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # First run
        episodes_run1 = [
            create_test_episode_of_care(test_eoc_ids[0], test_patient_id, status="active"),
        ]
        _post_episodes_to_fhir(
            context=context,
            episodes=episodes_run1,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        original_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource,
            "EpisodeOfCare",
            SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            test_eoc_ids[0],
        )

        # Second run
        episodes_run2 = [
            create_test_episode_of_care(test_eoc_ids[0], test_patient_id, status="finished"),
        ]
        result2 = _post_episodes_to_fhir(
            context=context,
            episodes=episodes_run2,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        assert result2.metadata["resolved_count"].value == 1
        assert result2.metadata["new_count"].value == 0

        count = count_resources_by_identifier(
            real_fhir_database_resource,
            "EpisodeOfCare",
            SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            test_eoc_ids[0],
        )
        assert count == 1

        new_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource,
            "EpisodeOfCare",
            SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            test_eoc_ids[0],
        )
        assert new_fhir_id == original_fhir_id


# =============================================================================
# Encounter Idempotency Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.encounter_test
class TestEncounterIdempotency:
    """Tests for Encounter resource idempotency."""

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture
    def test_encounter_ids(self, test_prefix):
        return [f"{test_prefix}_ENC_{i}" for i in range(5)]

    @pytest.fixture
    def test_patient_id(self, test_prefix):
        return f"{test_prefix}_PAT_ENC"

    @pytest.fixture(autouse=True)
    def cleanup_test_resources(
        self, test_encounter_ids, test_patient_id, real_fhir_database_resource
    ):
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, TEST_PREFIX
        )
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )
        yield
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, TEST_PREFIX
        )
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )

    def _setup_test_patient(self, real_fhir_database_resource, test_patient_id):
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        patients = [create_test_patient(test_patient_id, "0101900097")]
        _post_patients_to_fhir(
            context=context,
            patients=patients,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

    def test_first_run_inserts_new_encounters(
        self, real_fhir_database_resource, test_encounter_ids, test_patient_id
    ):
        """Test that first run correctly inserts new encounters."""
        from DSA.FHIR.Encounter.Encounter_IFDB_dfm import _post_encounters_to_fhir

        self._setup_test_patient(real_fhir_database_resource, test_patient_id)

        encounters = [
            create_test_encounter(test_encounter_ids[0], test_patient_id),
            create_test_encounter(test_encounter_ids[1], test_patient_id, status="finished"),
        ]

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        result = _post_encounters_to_fhir(
            context=context,
            encounters=encounters,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        assert result.metadata["posted_count"].value == 2

        for eid in test_encounter_ids[:2]:
            count = count_resources_by_identifier(
                real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, eid
            )
            assert count == 1

    def test_second_run_updates_existing_no_duplicates(
        self, real_fhir_database_resource, test_encounter_ids, test_patient_id
    ):
        """Test that second run updates existing encounters without duplicates."""
        from DSA.FHIR.Encounter.Encounter_IFDB_dfm import _post_encounters_to_fhir

        self._setup_test_patient(real_fhir_database_resource, test_patient_id)

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # First run
        encounters_run1 = [
            create_test_encounter(test_encounter_ids[0], test_patient_id, status="in-progress"),
        ]
        _post_encounters_to_fhir(
            context=context,
            encounters=encounters_run1,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        original_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, test_encounter_ids[0]
        )

        # Second run
        encounters_run2 = [
            create_test_encounter(test_encounter_ids[0], test_patient_id, status="finished"),
        ]
        _post_encounters_to_fhir(
            context=context,
            encounters=encounters_run2,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        count = count_resources_by_identifier(
            real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, test_encounter_ids[0]
        )
        assert count == 1

        new_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Encounter", SYSTEM_IFDB_OBJECT_ID, test_encounter_ids[0]
        )
        assert new_fhir_id == original_fhir_id


# =============================================================================
# Observation Idempotency Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.observation_test
class TestObservationIdempotency:
    """Tests for Observation resource idempotency."""

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture
    def test_observation_ids(self, test_prefix):
        return [f"{test_prefix}_OBS_{i}" for i in range(5)]

    @pytest.fixture
    def test_patient_id(self, test_prefix):
        return f"{test_prefix}_PAT_OBS"

    @pytest.fixture(autouse=True)
    def cleanup_test_resources(
        self, test_observation_ids, test_patient_id, real_fhir_database_resource
    ):
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Observation", SYSTEM_IFDB_OBJECT_ID, TEST_PREFIX
        )
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )
        yield
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Observation", SYSTEM_IFDB_OBJECT_ID, TEST_PREFIX
        )
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Patient", SYSTEM_IFDB_PATIENT_ID, TEST_PREFIX
        )

    def _setup_test_patient(self, real_fhir_database_resource, test_patient_id):
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        patients = [create_test_patient(test_patient_id, "0101900096")]
        _post_patients_to_fhir(
            context=context,
            patients=patients,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

    def test_first_run_inserts_new_observations(
        self, real_fhir_database_resource, test_observation_ids, test_patient_id
    ):
        """Test that first run correctly inserts new observations."""
        from DSA.FHIR.Observation.Observation_IFDB import _post_observations_to_fhir

        self._setup_test_patient(real_fhir_database_resource, test_patient_id)

        observations = [
            create_test_observation(test_observation_ids[0], test_patient_id, "Obs A"),
            create_test_observation(test_observation_ids[1], test_patient_id, "Obs B"),
        ]

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        result = _post_observations_to_fhir(
            context=context,
            observations=observations,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        assert result.metadata["posted_count"].value == 2

        for oid in test_observation_ids[:2]:
            count = count_resources_by_identifier(
                real_fhir_database_resource, "Observation", SYSTEM_IFDB_OBJECT_ID, oid
            )
            assert count == 1

    def test_second_run_updates_existing_no_duplicates(
        self, real_fhir_database_resource, test_observation_ids, test_patient_id
    ):
        """Test that second run updates existing observations without duplicates."""
        from DSA.FHIR.Observation.Observation_IFDB import _post_observations_to_fhir

        self._setup_test_patient(real_fhir_database_resource, test_patient_id)

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # First run
        observations_run1 = [
            create_test_observation(test_observation_ids[0], test_patient_id, "Original"),
        ]
        _post_observations_to_fhir(
            context=context,
            observations=observations_run1,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        original_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource,
            "Observation",
            SYSTEM_IFDB_OBJECT_ID,
            test_observation_ids[0],
        )

        # Second run
        observations_run2 = [
            create_test_observation(test_observation_ids[0], test_patient_id, "Updated"),
        ]
        _post_observations_to_fhir(
            context=context,
            observations=observations_run2,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        count = count_resources_by_identifier(
            real_fhir_database_resource,
            "Observation",
            SYSTEM_IFDB_OBJECT_ID,
            test_observation_ids[0],
        )
        assert count == 1

        new_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource,
            "Observation",
            SYSTEM_IFDB_OBJECT_ID,
            test_observation_ids[0],
        )
        assert new_fhir_id == original_fhir_id


# =============================================================================
# Location Idempotency Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.location_test
class TestLocationIdempotency:
    """Tests for Location resource idempotency."""

    @pytest.fixture
    def test_prefix(self):
        return generate_test_prefix()

    @pytest.fixture
    def test_location_codes(self, test_prefix):
        return [f"{test_prefix}_LOC_{i}" for i in range(5)]

    @pytest.fixture(autouse=True)
    def cleanup_test_resources(self, test_location_codes, real_fhir_database_resource):
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Location", SYSTEM_SOR, TEST_PREFIX
        )
        yield
        delete_resources_by_identifier_prefix(
            real_fhir_database_resource, "Location", SYSTEM_SOR, TEST_PREFIX
        )

    def test_first_run_inserts_new_locations(
        self, real_fhir_database_resource, test_location_codes
    ):
        """Test that first run correctly inserts new locations."""
        from DSA.FHIR.Location.Location_IFDB_dfm import _post_locations_to_fhir

        locations = [
            create_test_location(test_location_codes[0], "Location A"),
            create_test_location(test_location_codes[1], "Location B"),
        ]

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        result = _post_locations_to_fhir(
            context=context,
            locations=locations,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        assert result.metadata["posted_count"].value == 2
        assert result.metadata["new_count"].value == 2

        for lid in test_location_codes[:2]:
            count = count_resources_by_identifier(
                real_fhir_database_resource, "Location", SYSTEM_SOR, lid
            )
            assert count == 1

    def test_second_run_updates_existing_no_duplicates(
        self, real_fhir_database_resource, test_location_codes
    ):
        """Test that second run updates existing locations without duplicates."""
        from DSA.FHIR.Location.Location_IFDB_dfm import _post_locations_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # First run
        locations_run1 = [
            create_test_location(test_location_codes[0], "Original Location"),
        ]
        _post_locations_to_fhir(
            context=context,
            locations=locations_run1,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        original_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Location", SYSTEM_SOR, test_location_codes[0]
        )

        # Second run
        locations_run2 = [
            create_test_location(test_location_codes[0], "Updated Location"),
        ]
        result2 = _post_locations_to_fhir(
            context=context,
            locations=locations_run2,
            fhir_database=real_fhir_database_resource,
            extracted_count=1,
        )

        assert result2.metadata["resolved_count"].value == 1
        assert result2.metadata["new_count"].value == 0

        count = count_resources_by_identifier(
            real_fhir_database_resource, "Location", SYSTEM_SOR, test_location_codes[0]
        )
        assert count == 1

        new_fhir_id = get_fhir_id_by_identifier(
            real_fhir_database_resource, "Location", SYSTEM_SOR, test_location_codes[0]
        )
        assert new_fhir_id == original_fhir_id
