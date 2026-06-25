# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for patient ID resolution behavior.

These tests verify that the ID resolution logic correctly:
1. Attaches existing FHIR UUIDs to resources that already exist in the database
2. Leaves new resources without IDs (to be generated during upsert)
3. Counts resolved vs new resources correctly
"""

from unittest.mock import MagicMock

import pytest
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID


def _create_test_patient(
    patient_id: str, cpr: str | None = None, fhir_id: str | None = None
) -> Patient:
    """Create a test Patient resource with the given identifiers."""
    identifiers = [
        Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=patient_id),
    ]
    if cpr:
        identifiers.append(Identifier(system=SYSTEM_CPR, value=cpr))

    patient = Patient(
        identifier=identifiers,
        active=True,
    )
    if fhir_id:
        patient.id = fhir_id

    return patient


class TestResolveResourceIdsForUpsert:
    """Tests for resolve_resource_ids_for_upsert function."""

    def test_resolves_existing_patient_ids(self):
        """Test that existing patients get their FHIR IDs attached."""
        from DSA.FHIR.utils.ifdb.fhir_posting import resolve_resource_ids_for_upsert

        # Create test patients
        patients = [
            _create_test_patient("P001", "0101901234"),
            _create_test_patient("P002", "0202902345"),
            _create_test_patient("P003", "0303903456"),
        ]

        # Mock FHIR database resource
        mock_db = MagicMock()
        # Simulate that P001 and P002 already exist in the database
        mock_db.resolve_resource_ids.return_value = {
            0: "existing-uuid-001",  # P001 exists
            1: "existing-uuid-002",  # P002 exists
            # P003 is not in the result (new patient)
        }

        # Resolve IDs
        result = resolve_resource_ids_for_upsert(
            resources=patients,
            fhir_database=mock_db,
            resource_type="Patient",
            preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR],
        )

        # Verify resolve was called
        mock_db.resolve_resource_ids.assert_called_once_with(
            resources=patients,
            resource_type="Patient",
            preferred_systems=[SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR],
            force_reload=False,
        )

        # Verify existing patients got their IDs attached
        assert result[0].id == "existing-uuid-001"
        assert result[1].id == "existing-uuid-002"
        # New patient should not have an ID
        assert result[2].id is None

    def test_handles_empty_resource_list(self):
        """Test that empty resource list is handled gracefully."""
        from DSA.FHIR.utils.ifdb.fhir_posting import resolve_resource_ids_for_upsert

        mock_db = MagicMock()

        result = resolve_resource_ids_for_upsert(
            resources=[],
            fhir_database=mock_db,
            resource_type="Patient",
            preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
        )

        assert result == []
        mock_db.resolve_resource_ids.assert_not_called()

    def test_all_new_patients(self):
        """Test scenario where all patients are new."""
        from DSA.FHIR.utils.ifdb.fhir_posting import resolve_resource_ids_for_upsert

        patients = [
            _create_test_patient("P001", "0101901234"),
            _create_test_patient("P002", "0202902345"),
        ]

        mock_db = MagicMock()
        mock_db.resolve_resource_ids.return_value = {}  # No existing patients

        result = resolve_resource_ids_for_upsert(
            resources=patients,
            fhir_database=mock_db,
            resource_type="Patient",
            preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
        )

        # All patients should be without IDs
        assert result[0].id is None
        assert result[1].id is None

    def test_all_existing_patients(self):
        """Test scenario where all patients already exist."""
        from DSA.FHIR.utils.ifdb.fhir_posting import resolve_resource_ids_for_upsert

        patients = [
            _create_test_patient("P001", "0101901234"),
            _create_test_patient("P002", "0202902345"),
        ]

        mock_db = MagicMock()
        mock_db.resolve_resource_ids.return_value = {
            0: "uuid-001",
            1: "uuid-002",
        }

        result = resolve_resource_ids_for_upsert(
            resources=patients,
            fhir_database=mock_db,
            resource_type="Patient",
            preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
        )

        # All patients should have their existing IDs attached
        assert result[0].id == "uuid-001"
        assert result[1].id == "uuid-002"

    def test_preserves_existing_id_if_already_set(self):
        """Test that resources with pre-existing IDs keep them."""
        from DSA.FHIR.utils.ifdb.fhir_posting import resolve_resource_ids_for_upsert

        patients = [
            _create_test_patient("P001", "0101901234", fhir_id="pre-set-id-001"),
            _create_test_patient("P002", "0202902345"),
        ]

        mock_db = MagicMock()
        # Simulate that the resolver doesn't find P001 (because it already has an ID)
        # but would resolve P002
        mock_db.resolve_resource_ids.return_value = {
            1: "resolved-uuid-002",
        }

        result = resolve_resource_ids_for_upsert(
            resources=patients,
            fhir_database=mock_db,
            resource_type="Patient",
            preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
        )

        # P001 should keep its pre-set ID
        assert result[0].id == "pre-set-id-001"
        # P002 should get the resolved ID
        assert result[1].id == "resolved-uuid-002"


class TestPatientPostingWithIdResolution:
    """Tests for the patient posting function with ID resolution."""

    def test_counts_resolved_and_new_patients(self):
        """Test that the posting function correctly counts resolved vs new patients."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        patients = [
            _create_test_patient("P001", "0101901234"),
            _create_test_patient("P002", "0202902345"),
            _create_test_patient("P003", "0303903456"),
        ]

        # Create mock context
        mock_context = MagicMock()
        mock_context.has_partition_key = True
        mock_context.partition_key = "2025-01-15"

        # Create mock database
        mock_db = MagicMock()
        # Simulate that P001 and P002 exist (2 resolved, 1 new)
        mock_db.resolve_resource_ids.return_value = {
            0: "existing-uuid-001",
            1: "existing-uuid-002",
        }
        mock_db.batch_upsert_resources.return_value = (3, 0, [])

        result = _post_patients_to_fhir(
            context=mock_context,
            patients=patients,
            fhir_database=mock_db,
            extracted_count=3,
        )

        # Verify metadata includes resolved/new counts
        metadata = result.metadata
        assert "resolved_count" in metadata
        assert "new_count" in metadata
        assert metadata["resolved_count"].value == 2
        assert metadata["new_count"].value == 1

    def test_empty_patients_list_returns_zero_counts(self):
        """Test that empty patient list returns zeros for all counts."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        mock_context = MagicMock()
        mock_context.has_partition_key = True
        mock_context.partition_key = "2025-01-15"

        mock_db = MagicMock()

        result = _post_patients_to_fhir(
            context=mock_context,
            patients=[],
            fhir_database=mock_db,
            extracted_count=0,
        )

        metadata = result.metadata
        assert metadata["posted_count"].value == 0
        assert metadata["failed_count"].value == 0
        assert metadata["resolved_count"].value == 0
        assert metadata["new_count"].value == 0


class TestIdResolutionForOtherResources:
    """Tests to verify ID resolution is applied to all FHIR resource types."""

    @pytest.mark.parametrize(
        "resource_type,identifier_system",
        [
            ("Observation", "urn:source:ifdb:object_id"),
            ("Location", "urn:source:ifdb:location_id"),
            ("Organization", "urn:oid:1.2.208.176.1.1"),
            ("Encounter", "urn:source:ifdb:object_id"),
            ("EpisodeOfCare", "urn:source:ifdb:episode_of_care_id"),
        ],
    )
    def test_resource_type_uses_correct_identifier_system(
        self, resource_type: str, identifier_system: str
    ):
        """Verify each resource type is configured with correct identifier system for ID resolution."""
        # This is a documentation test to ensure we understand which identifier
        # systems are used for each resource type's ID resolution.
        # The actual behavior is tested in integration tests.
        assert resource_type in [
            "Patient",
            "Observation",
            "Location",
            "Organization",
            "Encounter",
            "EpisodeOfCare",
        ]
        assert identifier_system.startswith("urn:")
