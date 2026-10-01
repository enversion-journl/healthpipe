# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for FHIR Appointment transform from BOM_IFDB.AFTALE.

Verifies:
- Field mapping (description, created, minutesDuration)
- Patient participant with Reference.type="Patient" and CPR identifier
- Location participant with Reference.type="Location" and SOR identifier
- Participant status derived from appointment status (not hard-coded)
- Proper None handling for nullable fields
- Rows skipped when PK or CPR is missing
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.FHIR.Appointment.Appointment_IFDB_dfm import (
    APPOINTMENT_NESTED_REFERENCE_CONFIGS,
    _transform_appointments_to_fhir,
)
from utils.fhir.identifier_systems import (
    SYSTEM_BOM_IFDB_AFTALE_OID,
    SYSTEM_CPR,
    SYSTEM_SOR,
)


@pytest.fixture
def mock_dagster_context() -> MagicMock:
    """Mock Dagster context for transform tests."""
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.partition_key = "2026-01-15"
    context.has_partition_key = True
    context.log = MagicMock()
    return context


@pytest.fixture
def sample_aftale_rows() -> list[dict]:
    """Minimal valid AFTALE rows for Appointment transform, in ARC SHAPE:
    arc.bom_ifdb_aftale stores every column as NVARCHAR, so the transform
    receives str (never datetime/int) — the fixtures must mirror that
    (estimat §4.4: fixtures follow the input form)."""
    return [
        {
            "OID": "1001",
            "GUID": "abc-123-def",
            "PATIENTINFOOID": "501",
            "LASTMODIFIED": "2026-01-15 10:00:00",
            "OPRETTETDATOTID": "2026-01-10 08:30:00",
            "BOOKINGSTATUS": "1",
            "PATIENT_CPR": "0101901234",
            "PATIENT_MOEDEDATOTID": "2026-02-01 09:00:00",
            "RESTRAEK_STARTDATOTID": "2026-02-01 09:00:00",
            "RESTRAEKTOTALMINUTTER": "30",
            "KONTAKTTYPE": "2",
            "UDFOERENDE_SORKODE": "123456789012345",
            "DATOSLETTET": None,
            "ANGAAENDE": "Kontrol efter operation",
            "SKAERMNING_SKAERMET": "1",
            "PATIENTDELTAGER": "0",
        },
        {
            "OID": "1002",
            "GUID": None,
            "PATIENTINFOOID": "502",
            "LASTMODIFIED": "2026-01-15 11:00:00",
            "OPRETTETDATOTID": None,
            "BOOKINGSTATUS": "3",
            "PATIENT_CPR": "0202905678",
            "PATIENT_MOEDEDATOTID": None,
            "RESTRAEK_STARTDATOTID": None,
            "RESTRAEKTOTALMINUTTER": None,
            "KONTAKTTYPE": None,
            "UDFOERENDE_SORKODE": None,
            "DATOSLETTET": "2026-01-14 00:00:00",
            "ANGAAENDE": None,
            "SKAERMNING_SKAERMET": None,
            "PATIENTDELTAGER": None,
        },
    ]


@pytest.fixture
def participants_by_aftale() -> dict[int, list[dict]]:
    return {
        1001: [
            {"AFTALEOID": 1001, "KRAV_PERSONALE_ROLLEKODE": "LAE"},
        ],
    }


@pytest.mark.unit
class TestAppointmentTransform:
    """Tests for _transform_appointments_to_fhir."""

    def test_basic_transform_produces_appointments(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        assert len(appointments) == 2

    def test_identifier_system(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        appt = appointments[0]
        assert appt.identifier[0].system == SYSTEM_BOM_IFDB_AFTALE_OID
        assert appt.identifier[0].value == "1001"

    def test_patient_participant_has_type_and_cpr(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        appt = appointments[0]
        patient_participant = appt.participant[0]

        assert patient_participant.actor.type == "Patient"
        assert patient_participant.actor.identifier.system == SYSTEM_CPR
        assert patient_participant.actor.identifier.value == "0101901234"
        assert patient_participant.status == "accepted"

        type_coding = patient_participant.type[0].coding[0]
        assert type_coding.code == "SBJ"

    def test_location_participant_has_type_and_sor(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        appt = appointments[0]
        loc_participant = appt.participant[-1]

        assert loc_participant.actor.type == "Location"
        assert loc_participant.actor.identifier.system == SYSTEM_SOR
        assert loc_participant.actor.identifier.value == "123456789012345"

    def test_resource_participant_from_aftaleressourcedeltager(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        appt = appointments[0]
        atnd_participant = appt.participant[1]

        type_coding = atnd_participant.type[0].coding[0]
        assert type_coding.code == "ATND"
        assert type_coding.display == "LAE"
        assert atnd_participant.status == "accepted"

    def test_description_mapped(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        assert appointments[0].description == "Kontrol efter operation"
        assert appointments[1].description is None

    def test_created_mapped(self, sample_aftale_rows, participants_by_aftale, mock_dagster_context):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        assert appointments[0].created is not None
        assert appointments[1].created is None

    def test_minutes_duration_mapped(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        assert appointments[0].minutesDuration == 30
        assert appointments[1].minutesDuration is None

    def test_deleted_row_gets_entered_in_error_status(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        deleted_appt = appointments[1]
        assert deleted_appt.status == "entered-in-error"

    def test_is_deleted_extension_always_present(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        for appt in appointments:
            is_deleted_ext = next(e for e in appt.extension if e.url == "urn:fhir:isDeleted")
            assert is_deleted_ext.valueInteger in (0, 1)

    def test_start_and_end_computed_from_arc_string_timestamps(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        """arc delivers NVARCHAR timestamps — start must parse, and end must be
        start + RESTRAEKTOTALMINUTTER (str + timedelta raised TypeError before
        the _to_datetime coercion)."""
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        appt = appointments[0]
        assert appt.start is not None
        assert appt.end is not None
        assert appt.end - appt.start == timedelta(minutes=30)
        assert appointments[1].start is None
        assert appointments[1].end is None

    def test_skaermet_string_flag_sets_restricted_meta(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        """SKAERMNING_SKAERMET arrives as "1" (str) from arc — the restricted
        confidentiality marking must still be applied (== 1 was permanently
        False against the string)."""
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        assert appointments[0].meta is not None
        assert appointments[0].meta.security[0].code == "R"
        assert appointments[1].meta is None

    def test_patientdeltager_zero_string_makes_information_only(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        assert appointments[0].participant[0].required == "information-only"
        assert appointments[1].participant[0].required == "required"

    def test_skips_row_with_null_oid(self, participants_by_aftale, mock_dagster_context):
        rows = [{"OID": None, "PATIENT_CPR": "0101901234", "BOOKINGSTATUS": 1}]
        appointments = _transform_appointments_to_fhir(
            rows, participants_by_aftale, mock_dagster_context
        )
        assert len(appointments) == 0

    def test_skips_row_with_missing_cpr(self, participants_by_aftale, mock_dagster_context):
        rows = [{"OID": 999, "PATIENT_CPR": None, "BOOKINGSTATUS": 1}]
        appointments = _transform_appointments_to_fhir(
            rows, participants_by_aftale, mock_dagster_context
        )
        assert len(appointments) == 0

    def test_no_supporting_information(
        self, sample_aftale_rows, participants_by_aftale, mock_dagster_context
    ):
        """supportingInformation should no longer be set on Appointment."""
        appointments = _transform_appointments_to_fhir(
            sample_aftale_rows, participants_by_aftale, mock_dagster_context
        )
        for appt in appointments:
            assert appt.supportingInformation is None or appt.supportingInformation == []

    @pytest.mark.parametrize(
        "booking_status, expected_participant_status",
        [
            (1, "accepted"),
            (2, "accepted"),
            (3, "accepted"),
            (4, "needs-action"),
            (5, "needs-action"),
            (10, "declined"),
            (11, "needs-action"),
            (13, "needs-action"),
        ],
        ids=[
            "booked->accepted",
            "fulfilled->accepted",
            "entered-in-error->accepted",
            "proposed->needs-action",
            "pending->needs-action",
            "cancelled->declined",
            "proposed(anmodet)->needs-action",
            "proposed(webanmodet)->needs-action",
        ],
    )
    def test_participant_status_derived_from_appointment_status(
        self,
        booking_status,
        expected_participant_status,
        participants_by_aftale,
        mock_dagster_context,
    ):
        rows = [
            {
                "OID": 5000,
                "PATIENT_CPR": "0101901234",
                "BOOKINGSTATUS": booking_status,
                "DATOSLETTET": None,
                "PATIENT_MOEDEDATOTID": datetime(2026, 2, 1, 9, 0, 0),
                "RESTRAEK_STARTDATOTID": None,
                "RESTRAEKTOTALMINUTTER": None,
            }
        ]
        appointments = _transform_appointments_to_fhir(rows, {}, mock_dagster_context)
        assert len(appointments) == 1
        assert appointments[0].participant[0].status == expected_participant_status


@pytest.mark.unit
class TestAppointmentNestedReferenceConfigs:
    """Verify NestedReferenceFieldConfig declarations are correct."""

    def test_participant_actor_config(self):
        config = APPOINTMENT_NESTED_REFERENCE_CONFIGS[0]
        assert config.field_path == "participant.actor"
        assert config.identifier_system_to_type[SYSTEM_CPR] == "Patient"
        assert config.identifier_system_to_type[SYSTEM_SOR] == "Location"

    def test_only_one_nested_config(self):
        assert len(APPOINTMENT_NESTED_REFERENCE_CONFIGS) == 1
