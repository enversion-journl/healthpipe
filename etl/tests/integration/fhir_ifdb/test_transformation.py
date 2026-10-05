# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for DSA transformation layer.
"""

import pytest
from fhir.resources.R4B.episodeofcare import EpisodeOfCare
from fhir.resources.R4B.patient import Patient

pytestmark = pytest.mark.integration


class TestPatientTransformation:
    """Tests for Patient transformation."""

    def test_transform_patient_rows_to_fhir(self, sample_patient_rows, mock_dagster_context):
        """Test transformation of IFDB patient rows to FHIR Patient resources."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=sample_patient_rows,
        )

        assert len(patients) == 2
        assert all(isinstance(p, Patient) for p in patients)

        # Check first patient
        patient1 = patients[0]
        assert patient1.gender == "male"
        assert patient1.name is not None
        assert patient1.name[0].family == "Jensen"
        assert patient1.name[0].given == ["Anders"]
        assert patient1.address is not None
        assert patient1.address[0].city == "Odense"

        # Check second patient
        patient2 = patients[1]
        assert patient2.gender == "female"
        assert patient2.name[0].family == "Nielsen"
        assert patient2.name[0].given == ["Marie", "Sofie"]

    def test_patient_identifier_systems(self, sample_patient_rows, mock_dagster_context):
        """Test that correct identifier systems are used."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb
        from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=sample_patient_rows,
        )

        patient = patients[0]
        identifier_systems = [id.system for id in patient.identifier]

        assert SYSTEM_CPR in identifier_systems
        assert SYSTEM_IFDB_PATIENT_ID in identifier_systems

    def test_patient_deceased_status_mapping(
        self, sample_deceased_patient_row, mock_dagster_context
    ):
        """Test that deceased status is correctly mapped from PERSON_STATUSKODE=90."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=[sample_deceased_patient_row],
        )

        assert len(patients) == 1
        patient = patients[0]

        # Deceased flag should be True for status code 90
        assert patient.deceasedBoolean is True
        # Active should be False for deceased patients
        assert patient.active is False

    def test_patient_annulled_status_mapping(
        self, sample_annulled_patient_row, mock_dagster_context
    ):
        """Test that annulled status is correctly mapped from PERSON_STATUSKODE=80."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=[sample_annulled_patient_row],
        )

        assert len(patients) == 1
        patient = patients[0]

        # Annulled patients are inactive but not deceased
        assert patient.deceasedBoolean is None
        assert patient.active is False

    def test_patient_general_practitioner_mapping(self, sample_patient_rows, mock_dagster_context):
        """Test that general practitioner is correctly mapped from doctor fields."""
        from DSA.FHIR.utils.ifdb.transforms import (
            SYSTEM_DANISH_GP_NUMBER,
            transform_fhir_patient_ifdb,
        )

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=sample_patient_rows,
        )

        patient = patients[0]

        # Check generalPractitioner is populated
        assert patient.generalPractitioner is not None
        assert len(patient.generalPractitioner) == 1

        gp_ref = patient.generalPractitioner[0]
        assert gp_ref.type == "Practitioner"
        assert gp_ref.identifier is not None
        assert gp_ref.identifier.system == SYSTEM_DANISH_GP_NUMBER
        assert gp_ref.identifier.value == "123456"
        assert gp_ref.display == "Dr. Hans Petersen"

    def test_patient_address_protection_handling(
        self, sample_protected_address_patient_row, mock_dagster_context
    ):
        """Protected citizens still carry the address; the flag travels alongside."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb
        from utils.fhir.fhir_extensions import EXTENSION_ADDRESS_PROTECTION

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=[sample_protected_address_patient_row],
        )

        assert len(patients) == 1
        patient = patients[0]

        assert patient.address is not None
        assert len(patient.address) == 1
        assert patient.address[0].line == ["Hemmelig Vej 1"]
        assert patient.address[0].postalCode == "7000"
        assert patient.address[0].city == "Fredericia"

        assert patient.extension is not None
        protection_ext = None
        for ext in patient.extension:
            if ext.url == EXTENSION_ADDRESS_PROTECTION:
                protection_ext = ext
                break

        assert protection_ext is not None
        assert protection_ext.valueBoolean is True

    def test_patient_municipality_in_address(self, sample_patient_rows, mock_dagster_context):
        """Test that municipality is correctly mapped to address.district."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=sample_patient_rows,
        )

        patient = patients[0]
        assert patient.address is not None
        assert len(patient.address) == 1

        address = patient.address[0]
        # Municipality code should be in district field
        assert address.district == "461"
        assert address.city == "Odense"
        assert address.postalCode == "5000"

    def test_patient_district_extensions(self, sample_patient_rows, mock_dagster_context):
        """Test that district and social district extensions are correctly created."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb
        from utils.fhir.fhir_extensions import (
            EXTENSION_DISTRICT,
            EXTENSION_SOCIAL_DISTRICT,
        )

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=sample_patient_rows,
        )

        patient = patients[0]
        assert patient.extension is not None

        # Find district extension
        district_ext = None
        social_district_ext = None
        for ext in patient.extension:
            if ext.url == EXTENSION_DISTRICT:
                district_ext = ext
            elif ext.url == EXTENSION_SOCIAL_DISTRICT:
                social_district_ext = ext

        # Check district extension
        assert district_ext is not None
        assert district_ext.extension is not None
        district_values = {e.url: e.valueString for e in district_ext.extension}
        assert district_values.get("code") == "D01"
        assert district_values.get("text") == "Odense Centrum"

        # Check social district extension
        assert social_district_ext is not None
        assert social_district_ext.extension is not None
        social_values = {e.url: e.valueString for e in social_district_ext.extension}
        assert social_values.get("code") == "SD01"
        assert social_values.get("text") == "Odense Social District"

    def test_patient_active_status_default(self, sample_patient_rows, mock_dagster_context):
        """Test that active patients have active=True and no deceased flag."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=sample_patient_rows,
        )

        for patient in patients:
            assert patient.active is True
            assert patient.deceasedBoolean is None

    def test_patient_without_general_practitioner(
        self, sample_deceased_patient_row, mock_dagster_context
    ):
        """Test that patients without GP info have no generalPractitioner."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb

        # The deceased patient fixture has no GP info
        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=[sample_deceased_patient_row],
        )

        patient = patients[0]
        assert patient.generalPractitioner is None

    def test_patient_is_deleted_extension(self, sample_patient_rows, mock_dagster_context):
        """Flat urn:source:ifdb:patient_slettet extension (same style as healthissue_id)."""
        from DSA.FHIR.utils.ifdb.transforms import transform_fhir_patient_ifdb
        from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=sample_patient_rows,
        )

        assert len(patients) == 2

        patient1 = patients[0]
        assert patient1.extension is not None
        d1 = next(e for e in patient1.extension if e.url == EXTENSION_FHIR_IS_DELETED)
        assert d1.valueInteger == 0

        patient2 = patients[1]
        assert patient2.extension is not None
        d2 = next(e for e in patient2.extension if e.url == EXTENSION_FHIR_IS_DELETED)
        assert d2.valueInteger == 1


class TestEpisodeOfCareTransformation:
    """Tests for EpisodeOfCare transformation."""

    def test_transform_episode_of_care(self, sample_episode_of_care_rows, mock_dagster_context):
        """Test transformation of IFDB episode of care rows to FHIR EpisodeOfCare resources."""
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import _transform_episodes_to_fhir

        episodes = _transform_episodes_to_fhir(
            rows=sample_episode_of_care_rows,
            context=mock_dagster_context,
        )

        assert len(episodes) == 2
        assert all(isinstance(e, EpisodeOfCare) for e in episodes)

        episode = episodes[0]
        assert episode.status is not None
        assert episode.patient is not None
        assert episode.period is not None

    def test_episode_of_care_is_deleted_extension(
        self, sample_episode_of_care_rows, mock_dagster_context
    ):
        """Flat urn:source:ifdb:episode_of_care_deleted extension."""
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import _transform_episodes_to_fhir
        from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

        episodes = _transform_episodes_to_fhir(
            rows=sample_episode_of_care_rows,
            context=mock_dagster_context,
        )

        assert len(episodes) == 2

        episode1 = episodes[0]
        assert episode1.extension is not None
        d1 = next(e for e in episode1.extension if e.url == EXTENSION_FHIR_IS_DELETED)
        assert d1.valueInteger == 0

        episode2 = episodes[1]
        assert episode2.extension is not None
        d2 = next(e for e in episode2.extension if e.url == EXTENSION_FHIR_IS_DELETED)
        assert d2.valueInteger == 1
