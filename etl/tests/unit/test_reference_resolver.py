# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the reference_resolver module.

Tests the declarative configuration-based reference resolution including:
- ReferenceFieldConfig for top-level references
- NestedReferenceFieldConfig for extension-based references
- collect_reference_identifiers and apply_reference_fhir_ids
- collect_nested_reference_identifiers and apply_nested_reference_fhir_ids
- resolve_and_apply_all_references main entry point
"""

from unittest.mock import MagicMock, patch

import pytest
from DSA.FHIR.utils.reference_resolver import (
    AllReferencesResolutionResult,
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
    _apply_fhir_id_to_reference,
    _extract_identifier_from_reference,
    _get_all_references_from_nested_path,
    _get_reference_from_resource,
    apply_nested_reference_fhir_ids,
    apply_reference_fhir_ids,
    collect_nested_reference_identifiers,
    collect_reference_identifiers,
    resolve_and_apply_all_references,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.encounter import Encounter, EncounterLocation
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference

pytestmark = pytest.mark.unit

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def patient_config():
    """ReferenceFieldConfig for Patient references."""
    return ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system="urn:source:ifdb:patient_id",
        is_required=True,
        description="Patient reference",
    )


@pytest.fixture
def optional_eoc_config():
    """ReferenceFieldConfig for optional EpisodeOfCare references."""
    return ReferenceFieldConfig(
        field_name="episodeOfCare",
        target_resource_type="EpisodeOfCare",
        identifier_system="urn:source:ifdb:episode_of_care_id",
        is_required=False,
        description="Episode of care reference",
    )


@pytest.fixture
def location_config():
    """ReferenceFieldConfig for Location references (handles EncounterLocation)."""
    return ReferenceFieldConfig(
        field_name="location",
        target_resource_type="Location",
        identifier_system="urn:source:ifdb:location_id",
        is_required=False,
        description="Location reference",
    )


@pytest.fixture
def nested_eoc_config():
    """NestedReferenceFieldConfig for EpisodeOfCare in extensions."""
    return NestedReferenceFieldConfig(
        field_path="extension.valueReference",
        identifier_system_to_type={
            "urn:source:ifdb:episode_of_care_id": "EpisodeOfCare",
        },
        extension_url="http://example.org/fhir/StructureDefinition/episodeOfCare",
        is_required=False,
        description="Episode of care extension reference",
    )


@pytest.fixture
def sample_encounters():
    """Sample Encounter resources with multiple reference types."""
    return [
        Encounter(
            identifier=[Identifier(system="urn:source:ifdb:object_id", value="enc-1")],
            status="finished",
            class_fhir=Coding(code="AMB"),
            subject=Reference(
                identifier=Identifier(system="urn:source:ifdb:patient_id", value="pat-123")
            ),
            episodeOfCare=[
                Reference(
                    identifier=Identifier(
                        system="urn:source:ifdb:episode_of_care_id", value="eoc-100"
                    )
                )
            ],
            location=[
                EncounterLocation(
                    location=Reference(
                        identifier=Identifier(system="urn:source:ifdb:location_id", value="loc-A")
                    ),
                    period=Period(start="2024-01-01"),
                )
            ],
        ),
        Encounter(
            identifier=[Identifier(system="urn:source:ifdb:object_id", value="enc-2")],
            status="in-progress",
            class_fhir=Coding(code="IMP"),
            subject=Reference(
                identifier=Identifier(system="urn:source:ifdb:patient_id", value="pat-456")
            ),
            # No episodeOfCare
            location=[
                EncounterLocation(
                    location=Reference(
                        identifier=Identifier(system="urn:source:ifdb:location_id", value="loc-B")
                    ),
                ),
                EncounterLocation(
                    location=Reference(
                        identifier=Identifier(system="urn:source:ifdb:location_id", value="loc-C")
                    ),
                ),
            ],
        ),
    ]


@pytest.fixture
def sample_observations_with_extensions():
    """Sample Observation resources with extension-based references."""
    return [
        Observation(
            identifier=[Identifier(system="urn:source:ifdb:object_id", value="obs-1")],
            status="final",
            code=CodeableConcept(coding=[Coding(code="test-code")]),
            subject=Reference(
                identifier=Identifier(system="urn:source:ifdb:patient_id", value="pat-123")
            ),
            extension=[
                Extension(
                    url="http://example.org/fhir/StructureDefinition/episodeOfCare",
                    valueReference=Reference(
                        identifier=Identifier(
                            system="urn:source:ifdb:episode_of_care_id", value="eoc-100"
                        )
                    ),
                )
            ],
        ),
        Observation(
            identifier=[Identifier(system="urn:source:ifdb:object_id", value="obs-2")],
            status="final",
            code=CodeableConcept(coding=[Coding(code="test-code-2")]),
            subject=Reference(
                identifier=Identifier(system="urn:source:ifdb:patient_id", value="pat-456")
            ),
            extension=[
                Extension(
                    url="http://example.org/fhir/StructureDefinition/episodeOfCare",
                    valueReference=Reference(
                        identifier=Identifier(
                            system="urn:source:ifdb:episode_of_care_id", value="eoc-200"
                        )
                    ),
                ),
                # A different extension (should be ignored)
                Extension(
                    url="http://example.org/fhir/StructureDefinition/other",
                    valueString="some-value",
                ),
            ],
        ),
    ]


# =============================================================================
# Test ReferenceFieldConfig
# =============================================================================


class TestReferenceFieldConfig:
    """Tests for ReferenceFieldConfig dataclass."""

    def test_config_creation(self, patient_config):
        """Test config creation with all fields."""
        assert patient_config.field_name == "subject"
        assert patient_config.target_resource_type == "Patient"
        assert patient_config.identifier_system == "urn:source:ifdb:patient_id"
        assert patient_config.is_required is True
        assert patient_config.description == "Patient reference"

    def test_optional_config(self, optional_eoc_config):
        """Test optional reference config."""
        assert optional_eoc_config.is_required is False


# =============================================================================
# Test NestedReferenceFieldConfig
# =============================================================================


class TestNestedReferenceFieldConfig:
    """Tests for NestedReferenceFieldConfig dataclass."""

    def test_config_creation(self, nested_eoc_config):
        """Test nested config creation."""
        assert nested_eoc_config.field_path == "extension.valueReference"
        assert (
            nested_eoc_config.extension_url
            == "http://example.org/fhir/StructureDefinition/episodeOfCare"
        )
        assert "urn:source:ifdb:episode_of_care_id" in nested_eoc_config.identifier_system_to_type

    def test_get_target_type_for_system(self, nested_eoc_config):
        """Test type lookup by identifier system."""
        assert (
            nested_eoc_config.get_target_type_for_system("urn:source:ifdb:episode_of_care_id")
            == "EpisodeOfCare"
        )
        assert nested_eoc_config.get_target_type_for_system("unknown:system") is None

    def test_get_all_target_types(self):
        """Test getting all target types."""
        config = NestedReferenceFieldConfig(
            field_path="test",
            identifier_system_to_type={
                "system:org": "Organization",
                "system:pract": "Practitioner",
            },
        )
        target_types = config.get_all_target_types()
        assert target_types == {"Organization", "Practitioner"}

    def test_get_systems_for_type(self):
        """Test getting systems for a specific type."""
        config = NestedReferenceFieldConfig(
            field_path="test",
            identifier_system_to_type={
                "system:org1": "Organization",
                "system:org2": "Organization",
                "system:pract": "Practitioner",
            },
        )
        org_systems = config.get_systems_for_type("Organization")
        assert set(org_systems) == {"system:org1", "system:org2"}


# =============================================================================
# Test Helper Functions
# =============================================================================


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_reference_from_resource_direct(self, sample_encounters):
        """Test getting direct reference field."""
        enc = sample_encounters[0]
        ref = _get_reference_from_resource(enc, "subject")
        assert ref is not None
        assert isinstance(ref, Reference)
        assert ref.identifier.value == "pat-123"

    def test_get_reference_from_resource_list(self, sample_encounters):
        """Test getting list of references."""
        enc = sample_encounters[0]
        refs = _get_reference_from_resource(enc, "episodeOfCare")
        assert refs is not None
        assert isinstance(refs, list)
        assert len(refs) == 1

    def test_get_reference_from_resource_none(self, sample_encounters):
        """Test getting nonexistent field."""
        enc = sample_encounters[0]
        ref = _get_reference_from_resource(enc, "nonexistent")
        assert ref is None

    def test_extract_identifier_from_reference(self):
        """Test extracting identifier value from reference."""
        ref = Reference(identifier=Identifier(system="urn:test:system", value="12345"))
        value = _extract_identifier_from_reference(ref, "urn:test:system")
        assert value == "12345"

    def test_extract_identifier_wrong_system(self):
        """Test extracting identifier with wrong system."""
        ref = Reference(identifier=Identifier(system="urn:test:system", value="12345"))
        value = _extract_identifier_from_reference(ref, "urn:different:system")
        assert value is None

    def test_extract_identifier_no_identifier(self):
        """Test extracting from reference without identifier."""
        ref = Reference(reference="Patient/123")
        value = _extract_identifier_from_reference(ref, "urn:test:system")
        assert value is None

    def test_apply_fhir_id_to_reference(self):
        """Test applying FHIR ID to reference."""
        ref = Reference(identifier=Identifier(system="urn:test:system", value="12345"))
        _apply_fhir_id_to_reference(ref, "uuid-abc-123", "Patient")

        assert ref.id == "uuid-abc-123"
        assert ref.type == "Patient"
        assert ref.identifier is None  # Cleared after resolution


# =============================================================================
# Test collect_reference_identifiers
# =============================================================================


class TestCollectReferenceIdentifiers:
    """Tests for collect_reference_identifiers function."""

    def test_collect_from_encounters(self, sample_encounters, patient_config):
        """Test collecting patient identifiers from encounters."""
        identifiers = collect_reference_identifiers(sample_encounters, patient_config)

        # Should have unique identifiers
        assert len(identifiers) == 2  # pat-123 and pat-456
        assert "pat-123" in identifiers
        assert "pat-456" in identifiers

    def test_collect_from_list_reference(self, sample_encounters, optional_eoc_config):
        """Test collecting from list references (episodeOfCare)."""
        identifiers = collect_reference_identifiers(sample_encounters, optional_eoc_config)

        # Only first encounter has episodeOfCare
        assert len(identifiers) == 1
        assert "eoc-100" in identifiers

    def test_collect_from_encounter_location(self, sample_encounters, location_config):
        """Test collecting from EncounterLocation.location structure."""
        identifiers = collect_reference_identifiers(sample_encounters, location_config)

        # Should collect all location codes: loc-A, loc-B, loc-C
        assert len(identifiers) == 3
        assert "loc-A" in identifiers
        assert "loc-B" in identifiers
        assert "loc-C" in identifiers

    def test_collect_empty_resources(self, patient_config):
        """Test collecting from empty list."""
        identifiers = collect_reference_identifiers([], patient_config)
        assert identifiers == {}


# =============================================================================
# Test apply_reference_fhir_ids
# =============================================================================


class TestApplyReferenceFhirIds:
    """Tests for apply_reference_fhir_ids function."""

    def test_apply_to_encounters(self, sample_encounters, patient_config):
        """Test applying resolved IDs to encounters."""
        resolved_ids = {
            "pat-123": "fhir-uuid-123",
            "pat-456": "fhir-uuid-456",
        }

        result = apply_reference_fhir_ids(sample_encounters, patient_config, resolved_ids)

        assert result.resolved_count == 2
        assert result.unresolved_count == 0

        # Check that references were updated
        assert sample_encounters[0].subject.id == "fhir-uuid-123"
        assert sample_encounters[0].subject.type == "Patient"
        assert sample_encounters[0].subject.identifier is None

    def test_apply_with_missing_required(self, sample_encounters, patient_config):
        """Test that missing required references raise ValueError."""
        resolved_ids = {
            "pat-123": "fhir-uuid-123",
            # pat-456 is missing
        }

        with pytest.raises(ValueError) as exc_info:
            apply_reference_fhir_ids(sample_encounters, patient_config, resolved_ids)

        assert "Patient not found" in str(exc_info.value)
        assert "pat-456" in str(exc_info.value)

    def test_apply_with_missing_optional(self, sample_encounters):
        """Test that missing optional references don't raise."""
        optional_config = ReferenceFieldConfig(
            field_name="subject",
            target_resource_type="Patient",
            identifier_system="urn:source:ifdb:patient_id",
            is_required=False,  # Optional
        )
        resolved_ids = {
            "pat-123": "fhir-uuid-123",
            # pat-456 is missing
        }

        result = apply_reference_fhir_ids(sample_encounters, optional_config, resolved_ids)

        assert result.resolved_count == 1
        assert result.unresolved_count == 1
        assert "pat-456" in result.unresolved_identifiers

    def test_apply_to_encounter_location(self, sample_encounters, location_config):
        """Test applying resolved IDs to Encounter_Location.location."""
        resolved_ids = {
            "loc-A": "fhir-loc-A",
            "loc-B": "fhir-loc-B",
            "loc-C": "fhir-loc-C",
        }

        result = apply_reference_fhir_ids(sample_encounters, location_config, resolved_ids)

        assert result.resolved_count == 3
        assert result.unresolved_count == 0

        # Check that nested location references were updated
        assert sample_encounters[0].location[0].location.id == "fhir-loc-A"
        assert sample_encounters[0].location[0].location.type == "Location"


# =============================================================================
# Test Nested Reference Collection and Application
# =============================================================================


class TestNestedReferenceResolution:
    """Tests for nested reference resolution functions."""

    def test_get_all_references_from_extension_path(self, sample_observations_with_extensions):
        """Test extracting references from extension path."""
        obs = sample_observations_with_extensions[0]
        refs = _get_all_references_from_nested_path(
            obs,
            "extension.valueReference",
            extension_url="http://example.org/fhir/StructureDefinition/episodeOfCare",
        )

        assert len(refs) == 1
        assert refs[0].identifier.value == "eoc-100"

    def test_get_all_references_with_url_filter(self, sample_observations_with_extensions):
        """Test that extension URL filter works."""
        obs = sample_observations_with_extensions[1]  # Has multiple extensions
        refs = _get_all_references_from_nested_path(
            obs,
            "extension.valueReference",
            extension_url="http://example.org/fhir/StructureDefinition/episodeOfCare",
        )

        # Should only get the episodeOfCare extension, not the "other" one
        assert len(refs) == 1
        assert refs[0].identifier.value == "eoc-200"

    def test_collect_nested_reference_identifiers(
        self, sample_observations_with_extensions, nested_eoc_config
    ):
        """Test collecting identifiers from nested paths."""
        identifiers_by_type = collect_nested_reference_identifiers(
            sample_observations_with_extensions,
            nested_eoc_config,
        )

        assert "EpisodeOfCare" in identifiers_by_type
        eoc_identifiers = identifiers_by_type["EpisodeOfCare"]
        assert len(eoc_identifiers) == 2
        assert "eoc-100" in eoc_identifiers
        assert "eoc-200" in eoc_identifiers

    def test_apply_nested_reference_fhir_ids(
        self, sample_observations_with_extensions, nested_eoc_config
    ):
        """Test applying resolved IDs to nested references."""
        resolved_ids_by_type = {
            "EpisodeOfCare": {
                "eoc-100": "fhir-eoc-100",
                "eoc-200": "fhir-eoc-200",
            }
        }

        result = apply_nested_reference_fhir_ids(
            sample_observations_with_extensions,
            nested_eoc_config,
            resolved_ids_by_type,
        )

        assert result.total_resolved == 2
        assert result.total_unresolved == 0

        # Check that extension references were updated
        obs1_ext = sample_observations_with_extensions[0].extension[0]
        assert obs1_ext.valueReference.id == "fhir-eoc-100"
        assert obs1_ext.valueReference.type == "EpisodeOfCare"


# =============================================================================
# Test resolve_and_apply_all_references
# =============================================================================


class TestResolveAndApplyAllReferences:
    """Tests for the main entry point function."""

    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_resolve_all_references(self, mock_batch_resolve, sample_encounters, patient_config):
        """Test resolving all references with mocked database."""
        # Mock the batch resolution
        mock_batch_resolve.return_value = {
            "pat-123": "fhir-uuid-123",
            "pat-456": "fhir-uuid-456",
        }

        mock_fhir_database = MagicMock()

        result = resolve_and_apply_all_references(
            resources=sample_encounters,
            reference_configs=[patient_config],
            fhir_database=mock_fhir_database,
        )

        assert isinstance(result, AllReferencesResolutionResult)
        assert result.total_resolved == 2
        assert result.total_unresolved == 0
        assert result.required_unresolved == 0

        # Verify batch_resolve was called
        mock_batch_resolve.assert_called_once()

    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_resolve_multiple_reference_types(
        self, mock_batch_resolve, sample_encounters, patient_config, optional_eoc_config
    ):
        """Test resolving multiple reference types."""

        # Set up mock to return different results based on resource_type
        def mock_resolve(identifiers, resource_type, **kwargs):
            if resource_type == "Patient":
                return {
                    "pat-123": "fhir-pat-123",
                    "pat-456": "fhir-pat-456",
                }
            elif resource_type == "EpisodeOfCare":
                return {
                    "eoc-100": "fhir-eoc-100",
                }
            return {}

        mock_batch_resolve.side_effect = mock_resolve
        mock_fhir_database = MagicMock()

        result = resolve_and_apply_all_references(
            resources=sample_encounters,
            reference_configs=[patient_config, optional_eoc_config],
            fhir_database=mock_fhir_database,
        )

        # Should have results for both field types
        assert ("subject", "urn:source:ifdb:patient_id") in result.field_results
        assert ("episodeOfCare", "urn:source:ifdb:episode_of_care_id") in result.field_results

        # 2 patients + 1 episode = 3 total resolved
        assert result.total_resolved == 3

    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_two_systems_on_one_field_keep_separate_results(
        self, mock_batch_resolve, sample_encounters
    ):
        """One field, two identifier systems -> two entries, not one.

        Encounter.location carries both a SOR department and a bed Location.
        Keyed on the field name alone, the second config's result replaced the
        first one's, and the per-field counts silently reported half the work.
        """
        sor_config = ReferenceFieldConfig(
            field_name="location",
            target_resource_type="Location",
            identifier_system="https://sundhedsdatastyrelsen.dk/sor",
            is_required=False,
        )
        bed_config = ReferenceFieldConfig(
            field_name="location",
            target_resource_type="Location",
            identifier_system="urn:source:ifdb:bed_id",
            is_required=False,
        )
        mock_batch_resolve.return_value = {}

        result = resolve_and_apply_all_references(
            resources=sample_encounters,
            reference_configs=[sor_config, bed_config],
            fhir_database=MagicMock(),
        )

        assert ("location", "https://sundhedsdatastyrelsen.dk/sor") in result.field_results
        assert ("location", "urn:source:ifdb:bed_id") in result.field_results
        assert len(result.field_results) == 2

    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_resolve_with_nested_configs(
        self,
        mock_batch_resolve,
        sample_observations_with_extensions,
        patient_config,
        nested_eoc_config,
    ):
        """Test resolving both top-level and nested references."""

        def mock_resolve(identifiers, resource_type, **kwargs):
            if resource_type == "Patient":
                return {
                    "pat-123": "fhir-pat-123",
                    "pat-456": "fhir-pat-456",
                }
            elif resource_type == "EpisodeOfCare":
                return {
                    "eoc-100": "fhir-eoc-100",
                    "eoc-200": "fhir-eoc-200",
                }
            return {}

        mock_batch_resolve.side_effect = mock_resolve
        mock_fhir_database = MagicMock()

        result = resolve_and_apply_all_references(
            resources=sample_observations_with_extensions,
            reference_configs=[patient_config],
            fhir_database=mock_fhir_database,
            nested_reference_configs=[nested_eoc_config],
        )

        # Should have top-level and nested results
        assert ("subject", "urn:source:ifdb:patient_id") in result.field_results
        assert "extension.valueReference" in result.nested_results

        # 2 patients + 2 episodes = 4 total
        assert result.total_resolved == 4

    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_required_unresolved_tracking(
        self, mock_batch_resolve, sample_encounters, patient_config
    ):
        """Test that required unresolved references are tracked."""
        # Missing one patient
        mock_batch_resolve.return_value = {
            "pat-123": "fhir-uuid-123",
            # pat-456 is missing
        }

        mock_fhir_database = MagicMock()

        # This should raise because patient is required
        with pytest.raises(ValueError):
            resolve_and_apply_all_references(
                resources=sample_encounters,
                reference_configs=[patient_config],
                fhir_database=mock_fhir_database,
            )

    def test_resolve_fn_callable_replaces_db_path(self, sample_encounters, patient_config):
        """Partition resolver should replace direct DB identifier lookup."""
        mock_fhir_database = MagicMock()
        captured_calls: list[tuple[str, str, list[str]]] = []

        def fake_resolve(*, resource_type, identifier_system, values):
            captured_calls.append((resource_type, identifier_system, list(values)))
            return {
                "pat-123": "fhir-uuid-123",
                "pat-456": "fhir-uuid-456",
            }

        result = resolve_and_apply_all_references(
            resources=sample_encounters,
            reference_configs=[patient_config],
            fhir_database=mock_fhir_database,
            resolve_fn=fake_resolve,
        )

        assert result.total_resolved == 2
        mock_fhir_database.resolve_identifier_values.assert_not_called()
        assert captured_calls[0][0] == "Patient"
        assert sorted(captured_calls[0][2]) == ["pat-123", "pat-456"]

    def test_resolve_fn_threaded_to_nested_configs(
        self,
        sample_observations_with_extensions,
        patient_config,
        nested_eoc_config,
    ):
        """Nested references must use the same resolver hook as top-level refs."""
        mock_fhir_database = MagicMock()
        calls: list[tuple[str, list[str]]] = []

        def fake_resolve(*, resource_type, identifier_system, values):
            calls.append((resource_type, list(values)))
            if resource_type == "Patient":
                return {"pat-123": "fhir-pat-123", "pat-456": "fhir-pat-456"}
            if resource_type == "EpisodeOfCare":
                return {"eoc-100": "fhir-eoc-100", "eoc-200": "fhir-eoc-200"}
            return {}

        result = resolve_and_apply_all_references(
            resources=sample_observations_with_extensions,
            reference_configs=[patient_config],
            fhir_database=mock_fhir_database,
            nested_reference_configs=[nested_eoc_config],
            resolve_fn=fake_resolve,
        )

        assert result.total_resolved == 4
        mock_fhir_database.resolve_identifier_values.assert_not_called()
        assert {resource_type for resource_type, _ in calls} == {
            "Patient",
            "EpisodeOfCare",
        }


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_resources_list(self, patient_config):
        """Test with empty resources list."""
        identifiers = collect_reference_identifiers([], patient_config)
        assert identifiers == {}

    def test_resources_without_references(self):
        """Test resources that don't have the reference field."""
        # Use Observation which allows null subject, and a config for a different field
        config = ReferenceFieldConfig(
            field_name="encounter",  # Observation has encounter field but we don't set it
            target_resource_type="Encounter",
            identifier_system="urn:test:system",
            is_required=False,
        )

        obs = Observation(
            identifier=[Identifier(system="urn:test", value="123")],
            status="final",
            code=CodeableConcept(coding=[Coding(code="test")]),
            # No encounter field set
        )

        identifiers = collect_reference_identifiers([obs], config)
        assert identifiers == {}

    def test_reference_without_identifier(self):
        """Test reference without identifier (uses direct reference string)."""
        config = ReferenceFieldConfig(
            field_name="subject",
            target_resource_type="Patient",
            identifier_system="urn:source:ifdb:patient_id",
            is_required=False,
        )

        # Create observation with subject that has reference string but no identifier
        obs = Observation(
            identifier=[Identifier(system="urn:test", value="123")],
            status="final",
            code=CodeableConcept(coding=[Coding(code="test")]),
            subject=Reference(reference="Patient/123"),  # Direct reference, no identifier
        )

        identifiers = collect_reference_identifiers([obs], config)
        # Reference without identifier should not be collected
        assert identifiers == {}

    def test_nested_config_without_extension_url(self):
        """Test nested config that matches all extensions."""
        config = NestedReferenceFieldConfig(
            field_path="extension.valueReference",
            identifier_system_to_type={
                "urn:test:system": "TestResource",
            },
            # No extension_url specified - matches all
        )

        obs = Observation(
            status="final",
            code=CodeableConcept(coding=[Coding(code="test")]),
            extension=[
                Extension(
                    url="http://any/url",
                    valueReference=Reference(
                        identifier=Identifier(system="urn:test:system", value="val-1")
                    ),
                ),
            ],
        )

        refs = _get_all_references_from_nested_path(
            obs,
            config.field_path,
            extension_url=None,  # Match all
        )

        assert len(refs) == 1
        assert refs[0].identifier.value == "val-1"
