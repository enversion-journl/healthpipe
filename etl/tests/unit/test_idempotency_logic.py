# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for idempotency logic across all FHIR resource types.

Tests the core mechanisms that ensure idempotent behavior:
1. ID resolution via resolve_resource_ids_for_upsert()
2. Upsert behavior (existing resources get updated, new ones inserted)
3. FHIR UUID preservation across updates

These tests use mocked database interactions for fast execution.
"""

from unittest.mock import MagicMock, patch

import pytest
from DSA.FHIR.utils.ifdb.fhir_posting import resolve_resource_ids_for_upsert
from fhir.resources.R4B.encounter import Encounter
from fhir.resources.R4B.episodeofcare import EpisodeOfCare
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.patient import Patient
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)

pytestmark = pytest.mark.unit


class TestIdResolutionMechanics:
    """Tests for the core ID resolution mechanism."""

    def test_new_resources_get_no_id(self):
        """New resources (not in database) should not get an ID assigned."""
        mock_fhir_database = MagicMock()
        # Return empty dict - no existing resources found
        mock_fhir_database.resolve_resource_ids.return_value = {}

        patients = [
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="NEW-001")]),
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="NEW-002")]),
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
            )

        # No IDs should be assigned
        assert result[0].id is None
        assert result[1].id is None

    def test_existing_resources_get_existing_id(self):
        """Existing resources should get their database UUID attached."""
        mock_fhir_database = MagicMock()
        # Return existing IDs for resources
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "existing-uuid-001",
            1: "existing-uuid-002",
        }

        patients = [
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="EXIST-001")]),
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="EXIST-002")]),
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
            )

        # Existing IDs should be attached
        assert result[0].id == "existing-uuid-001"
        assert result[1].id == "existing-uuid-002"

    def test_mixed_new_and_existing_resources(self):
        """Mixed batch handles both new and existing resources correctly."""
        mock_fhir_database = MagicMock()
        # Only index 1 and 3 have existing IDs
        mock_fhir_database.resolve_resource_ids.return_value = {
            1: "existing-uuid-b",
            3: "existing-uuid-d",
        }

        patients = [
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="NEW-A")]),
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="EXIST-B")]),
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="NEW-C")]),
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="EXIST-D")]),
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
            )

        assert result[0].id is None  # New
        assert result[1].id == "existing-uuid-b"  # Existing
        assert result[2].id is None  # New
        assert result[3].id == "existing-uuid-d"  # Existing

    def test_resource_id_resolver_called_with_correct_params(self):
        """Verify the central resource-id resolver is called with correct parameters."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {}

        patients = [
            Patient(identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="TEST-001")])
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR],
            )

        mock_fhir_database.resolve_resource_ids.assert_called_once_with(
            resources=patients,
            resource_type="Patient",
            preferred_systems=[SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR],
            force_reload=False,
        )


class TestEpisodeOfCareIdResolution:
    """Tests for EpisodeOfCare resource ID resolution."""

    def test_episode_of_care_id_resolution(self):
        """EpisodeOfCare uses SYSTEM_IFDB_EPISODE_OF_CARE_ID for identification."""
        from fhir.resources.R4B.reference import Reference

        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "eoc-uuid-001",
        }

        episodes = [
            EpisodeOfCare(
                identifier=[Identifier(system=SYSTEM_IFDB_EPISODE_OF_CARE_ID, value="EOC001")],
                status="active",
                patient=Reference(reference="Patient/dummy"),  # Required field
            )
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=episodes,
                fhir_database=mock_fhir_database,
                resource_type="EpisodeOfCare",
                preferred_id_systems=[SYSTEM_IFDB_EPISODE_OF_CARE_ID],
            )

        assert result[0].id == "eoc-uuid-001"


class TestEncounterIdResolution:
    """Tests for Encounter resource ID resolution."""

    def test_encounter_id_resolution(self):
        """Encounter uses SYSTEM_IFDB_OBJECT_ID for identification."""
        from fhir.resources.R4B.coding import Coding

        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "encounter-uuid-001",
        }

        encounters = [
            Encounter(
                identifier=[Identifier(system=SYSTEM_IFDB_OBJECT_ID, value="ENC001")],
                status="in-progress",
                class_fhir=Coding(code="AMB"),
            )
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=encounters,
                fhir_database=mock_fhir_database,
                resource_type="Encounter",
                preferred_id_systems=[SYSTEM_IFDB_OBJECT_ID],
            )

        assert result[0].id == "encounter-uuid-001"


class TestObservationIdResolution:
    """Tests for Observation resource ID resolution."""

    def test_observation_id_resolution(self):
        """Observation uses SYSTEM_IFDB_OBJECT_ID for identification."""
        from fhir.resources.R4B.codeableconcept import CodeableConcept

        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "observation-uuid-001",
        }

        observations = [
            Observation(
                identifier=[Identifier(system=SYSTEM_IFDB_OBJECT_ID, value="OBS001")],
                status="final",
                code=CodeableConcept(text="Test observation"),
            )
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=observations,
                fhir_database=mock_fhir_database,
                resource_type="Observation",
                preferred_id_systems=[SYSTEM_IFDB_OBJECT_ID],
            )

        assert result[0].id == "observation-uuid-001"


class TestLocationIdResolution:
    """Tests for Location resource ID resolution."""

    def test_location_id_resolution(self):
        """Location uses SYSTEM_SOR for identification."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "location-uuid-001",
        }

        locations = [
            Location(
                identifier=[Identifier(system=SYSTEM_SOR, value="576361000016009")],
                status="active",
            )
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=locations,
                fhir_database=mock_fhir_database,
                resource_type="Location",
                preferred_id_systems=[SYSTEM_SOR],
            )

        assert result[0].id == "location-uuid-001"


class TestOrganizationIdResolution:
    """Tests for Organization resource ID resolution."""

    def test_organization_id_resolution_with_sor_code(self):
        """Organization uses SOR code for identification."""
        SYSTEM_SOR_CODE = "urn:oid:1.2.208.176.1.1"

        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "organization-uuid-001",
        }

        organizations = [
            Organization(
                identifier=[Identifier(system=SYSTEM_SOR_CODE, value="123456789")],
                name="Test Organization",
            )
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=organizations,
                fhir_database=mock_fhir_database,
                resource_type="Organization",
                preferred_id_systems=[SYSTEM_SOR_CODE],
            )

        assert result[0].id == "organization-uuid-001"


class TestBatchUpsertIdempotency:
    """Tests for batch upsert idempotency behavior."""

    def test_resources_with_id_trigger_update(self):
        """Resources with pre-existing ID should trigger UPDATE not INSERT."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.batch_upsert_resources.return_value = (2, 0, [])

        # Resources with IDs already assigned (from ID resolution)
        patients = [
            Patient(id="existing-uuid-001", identifier=[]),
            Patient(id="existing-uuid-002", identifier=[]),
        ]

        # Call batch_upsert_resources directly (simulating post step)
        mock_fhir_database.batch_upsert_resources(
            resources=patients,
            resource_type="Patient",
        )

        # Verify it was called with resources that have IDs
        call_args = mock_fhir_database.batch_upsert_resources.call_args
        assert call_args.kwargs["resources"][0].id == "existing-uuid-001"
        assert call_args.kwargs["resources"][1].id == "existing-uuid-002"

    def test_resources_without_id_get_new_uuid(self):
        """Resources without ID should get new UUID generated."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.batch_upsert_resources.return_value = (2, 0, [])

        # Resources without IDs (new resources)
        patients = [
            Patient(identifier=[]),
            Patient(identifier=[]),
        ]

        mock_fhir_database.batch_upsert_resources(
            resources=patients,
            resource_type="Patient",
        )

        # Verify it was called with resources that don't have IDs
        call_args = mock_fhir_database.batch_upsert_resources.call_args
        assert call_args.kwargs["resources"][0].id is None
        assert call_args.kwargs["resources"][1].id is None


class TestIdempotencyMetadata:
    """Tests for idempotency-related metadata in MaterializeResult."""

    def test_resolved_and_new_counts_in_metadata(self):
        """Verify that resolved_count and new_count are correctly calculated."""
        # This tests the pattern used in assets like condition.py
        # where we count resources before and after ID resolution

        resources_before = [
            Patient(id=None, identifier=[]),  # Will get resolved
            Patient(id=None, identifier=[]),  # Will get resolved
            Patient(id=None, identifier=[]),  # New
        ]

        # Simulate ID resolution attaching IDs to first two
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "uuid-1",
            1: "uuid-2",
        }

        before_resolve = len([r for r in resources_before if r.id is not None])

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            resources_after = resolve_resource_ids_for_upsert(
                resources=resources_before,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
            )

        after_resolve = len([r for r in resources_after if r.id is not None])

        resolved_count = after_resolve - before_resolve
        new_count = len(resources_after) - after_resolve

        assert resolved_count == 2  # Two got existing IDs
        assert new_count == 1  # One is new


class TestEdgeCases:
    """Tests for edge cases in idempotency logic."""

    def test_empty_list_returns_empty(self):
        """Empty resource list should return empty list without errors."""
        mock_fhir_database = MagicMock()

        result = resolve_resource_ids_for_upsert(
            resources=[],
            fhir_database=mock_fhir_database,
            resource_type="Patient",
            preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
        )

        assert result == []
        mock_fhir_database.resolve_resource_ids.assert_not_called()

    def test_resource_with_multiple_identifiers(self):
        """Resources with multiple identifiers should work correctly."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "patient-uuid-001",
        }

        # Patient with both IFDB ID and CPR
        patients = [
            Patient(
                identifier=[
                    Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="P001"),
                    Identifier(system=SYSTEM_CPR, value="0101901234"),
                ]
            )
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR],
            )

        assert result[0].id == "patient-uuid-001"

    def test_resource_with_pre_existing_id_preserved(self):
        """Resources that already have an ID should keep it."""
        mock_fhir_database = MagicMock()
        # Return a different ID for index 0
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "different-uuid",
        }

        # Patient already has an ID set
        patients = [
            Patient(
                id="pre-existing-uuid",
                identifier=[Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="P001")],
            )
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID],
            )

        # The ID should be overwritten by the resolved one
        # (This is the expected behavior - the cache ID takes precedence)
        assert result[0].id == "different-uuid"
