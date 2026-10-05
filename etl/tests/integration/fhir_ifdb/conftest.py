# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pytest fixtures for FHIR IFDB integration tests.

These fixtures provide mocked resources and sample data for testing
the DSI extraction and DSA transformation layers.
"""

import os
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def require_real_fhir_ifdb_environment():
    """
    FHIR IFDB integration tests depend on live IFDB/FHIR infrastructure.

    Skip by default in local/mock environments to avoid deterministic failures.
    """
    run_integration = os.getenv("RUN_FHIR_IFDB_INTEGRATION_TESTS", "false").lower() == "true"
    if not run_integration:
        pytest.skip(
            "FHIR IFDB integration tests are opt-in (set RUN_FHIR_IFDB_INTEGRATION_TESTS=true)"
        )


@pytest.fixture
def sample_patient_rows() -> list[dict[str, Any]]:
    """Sample patient data from IFDB extraction.

    Column names match the aliases used in the transformation layer
    (as defined in transform_fhir_patient_ifdb function).

    Includes all fields defined in fhir.ifdb.yml ifdb_patient canonical:
    - Core identifiers: patient_id, identifier_cpr
    - Name: name_given, name_family
    - Address: address_street, address_postal, address_city, address_municipality
    - Demographics: gender_code, gender_text, birth_date
    - Status: person_status_code, person_status_text
    - GP: doctor_name, doctor_number
    - Protection: address_protection, discretion_flag
    - District: district_code, district_text, social_district_code, social_district_text
    - Metadata: last_updated, modtidspunkt
    """
    return [
        {
            # Core identifiers
            "patient_id": "123456",
            "identifier_cpr": "0101901234",
            # Name
            "name_given": "Anders",
            "name_family": "Jensen",
            # Address
            "address_street": "Hovedgaden 1",
            "address_postal": "5000",
            "address_city": "Odense",
            "address_municipality": "461",
            # Demographics
            "gender_code": "1",
            "gender_text": "Mand",
            "birth_date": datetime(1990, 1, 1),
            # Person status (01 = active)
            "person_status_code": "01",
            "person_status_text": "aktiv",
            # General practitioner
            "doctor_name": "Dr. Hans Petersen",
            "doctor_number": "123456",
            # Protection flags
            "address_protection": False,
            "discretion_flag": False,
            # Remarks
            "remarks": None,
            # District codes
            "district_code": "D01",
            "district_text": "Odense Centrum",
            "social_district_code": "SD01",
            "social_district_text": "Odense Social District",
            # Metadata
            "last_updated": datetime(2025, 1, 15, 10, 30, 0),
            "modtidspunkt": "15JAN2025:10:30:00",
            # Deletion status (0 = not deleted)
            "is_deleted": 0,
        },
        {
            # Core identifiers
            "patient_id": "789012",
            "identifier_cpr": "1502851234",
            # Name
            "name_given": "Marie Sofie",
            "name_family": "Nielsen",
            # Address
            "address_street": "Østergade 42",
            "address_postal": "8000",
            "address_city": "Aarhus",
            "address_municipality": "751",
            # Demographics
            "gender_code": "2",
            "gender_text": "Kvinde",
            "birth_date": datetime(1985, 2, 15),
            # Person status (01 = active)
            "person_status_code": "01",
            "person_status_text": "aktiv",
            # General practitioner
            "doctor_name": "Dr. Mette Hansen",
            "doctor_number": "654321",
            # Protection flags
            "address_protection": False,
            "discretion_flag": False,
            # Remarks
            "remarks": "Preferred contact by phone",
            # District codes
            "district_code": "D02",
            "district_text": "Aarhus Nord",
            "social_district_code": "SD02",
            "social_district_text": "Aarhus Social District",
            # Metadata
            "last_updated": datetime(2025, 1, 15, 11, 0, 0),
            "modtidspunkt": "15JAN2025:11:00:00",
            # Deletion status (1 = deleted)
            "is_deleted": 1,
        },
    ]


@pytest.fixture
def sample_deceased_patient_row() -> dict[str, Any]:
    """Sample deceased patient data for testing deceased status mapping."""
    return {
        "patient_id": "999001",
        "identifier_cpr": "0101501234",
        "name_given": "Erik",
        "name_family": "Andersen",
        "address_street": "Kirkegade 10",
        "address_postal": "6000",
        "address_city": "Kolding",
        "address_municipality": "621",
        "gender_code": "1",
        "gender_text": "Mand",
        "birth_date": datetime(1950, 1, 1),
        # Person status 90 = deceased
        "person_status_code": "90",
        "person_status_text": "Død",
        "doctor_name": None,
        "doctor_number": None,
        "address_protection": False,
        "discretion_flag": False,
        "remarks": None,
        "district_code": None,
        "district_text": None,
        "social_district_code": None,
        "social_district_text": None,
        "last_updated": datetime(2025, 1, 10, 8, 0, 0),
        "modtidspunkt": "10JAN2025:08:00:00",
    }


@pytest.fixture
def sample_protected_address_patient_row() -> dict[str, Any]:
    """Sample patient with address protection for testing privacy handling."""
    return {
        "patient_id": "999002",
        "identifier_cpr": "1506801234",
        "name_given": "Anna",
        "name_family": "Larsen",
        # Address details are carried as fact even when address_protection is True
        "address_street": "Hemmelig Vej 1",
        "address_postal": "7000",
        "address_city": "Fredericia",
        "address_municipality": "607",
        "gender_code": "2",
        "gender_text": "Kvinde",
        "birth_date": datetime(1980, 6, 15),
        "person_status_code": "01",
        "person_status_text": "aktiv",
        "doctor_name": "Dr. Søren Nielsen",
        "doctor_number": "789012",
        # Address protection enabled
        "address_protection": True,
        "discretion_flag": True,
        "remarks": "Address protection due to safety concerns",
        "district_code": "D03",
        "district_text": "Fredericia District",
        "social_district_code": "SD03",
        "social_district_text": "Fredericia Social",
        "last_updated": datetime(2025, 1, 12, 14, 0, 0),
        "modtidspunkt": "12JAN2025:14:00:00",
    }


@pytest.fixture
def sample_annulled_patient_row() -> dict[str, Any]:
    """Sample patient with annulled CPR for testing inactive status mapping."""
    return {
        "patient_id": "999003",
        "identifier_cpr": "0101701234",
        "name_given": "Test",
        "name_family": "Annulleret",
        "address_street": None,
        "address_postal": None,
        "address_city": None,
        "address_municipality": None,
        "gender_code": "1",
        "gender_text": "Mand",
        "birth_date": datetime(1970, 1, 1),
        # Person status 80 = annulled/cancelled
        "person_status_code": "80",
        "person_status_text": "Annulleret",
        "doctor_name": None,
        "doctor_number": None,
        "address_protection": False,
        "discretion_flag": False,
        "remarks": "CPR annulled - duplicate registration",
        "district_code": None,
        "district_text": None,
        "social_district_code": None,
        "social_district_text": None,
        "last_updated": datetime(2024, 6, 1, 10, 0, 0),
        "modtidspunkt": "01JUN2024:10:00:00",
    }


@pytest.fixture
def sample_episode_of_care_rows() -> list[dict[str, Any]]:
    """
    Sample episode of care data from IFDB extraction.

    Aligned with fhir.ifdb.yml model specification.
    """
    return [
        {
            # Required FHIR elements
            "OBJECT_ID": "EOC001",
            "PATIENT_ID": "123456",
            "STATUS": "active",
            # Core FHIR elements
            "PERIOD_START": datetime(2025, 1, 10),
            "PERIOD_END": None,
            # Organization management
            "MANAGING_ORGANIZATION_CODE": "ORG001",
            "MANAGING_ORGANIZATION_NAME": "Hospital A",
            # Care management (for future use)
            "CARE_MANAGER_ID": None,
            "CARE_MANAGER_NAME": None,
            # Episode relationships
            "PREVIOUS_EOC_ID": None,
            # Health issue (for future use)
            "HEALTHISSUE_ID": "HI001",
            # Metadata
            "DELETED": 0,
            "IFDB_MODIFICATION_DATETIME": datetime(2025, 1, 15),
        },
        {
            # Second episode with PREVIOUS_EOC_ID for testing linkage
            "OBJECT_ID": "EOC002",
            "PATIENT_ID": "123456",
            "STATUS": "finished",
            "PERIOD_START": datetime(2025, 1, 5),
            "PERIOD_END": datetime(2025, 1, 9),
            "MANAGING_ORGANIZATION_CODE": "ORG001",
            "MANAGING_ORGANIZATION_NAME": "Hospital A",
            "CARE_MANAGER_ID": "CM001",
            "CARE_MANAGER_NAME": "Dr. Hansen",
            "PREVIOUS_EOC_ID": "EOC001",  # Links to first episode
            "HEALTHISSUE_ID": "HI001",
            "DELETED": 1,
            "IFDB_MODIFICATION_DATETIME": datetime(2025, 1, 15),
        },
    ]


@pytest.fixture
def sample_encounter_rows() -> list[dict[str, Any]]:
    """Sample encounter data from IFDB extraction."""
    return [
        {
            "OBJECT_ID": "ENC001",
            "PATIENT_ID": "123456",
            "EPISODE_OF_CARE_ID": "EOC001",
            "STATUS_CODE": "in-progress",
            "STATUS_TEXT": "EncounterState.Status.StatusParent.Inprogress",
            "CLASS_CODE": "IMP",
            "CLASS_TEXT": "Indlagt",
            "PERIOD_START": datetime(2025, 1, 10, 8, 0, 0),
            "PERIOD_END": None,
            "DELETED": 0,
            "EPJI_MODIFICATION_DATETIME": datetime(2025, 1, 15),
        },
        {
            "OBJECT_ID": "ENC002",
            "PATIENT_ID": "123456",
            "EPISODE_OF_CARE_ID": "EOC001",
            "STATUS_CODE": "finished",
            "STATUS_TEXT": "EncounterState.Status.StatusParent.Finished",
            "CLASS_CODE": "AMB",
            "CLASS_TEXT": "Ambulant",
            "PERIOD_START": datetime(2025, 1, 5, 10, 0, 0),
            "PERIOD_END": datetime(2025, 1, 5, 14, 0, 0),
            "DELETED": 1,
            "EPJI_MODIFICATION_DATETIME": datetime(2025, 1, 15),
        },
    ]


@pytest.fixture
def sample_location_rows() -> list[dict[str, Any]]:
    """
    Sample location data from IFDB extraction.

    These are distinct locations identified by LOCATION_CODE.
    """
    return [
        {
            "LOCATION_CODE": "01.04.24.2.4",
            "LOCATION_NAME": "SLB A270 Team 1 Onkologisk Ambulatorium (Vejle)",
            "LOCATION_SORCODE": "247791000016006.0",
        },
        {
            "LOCATION_CODE": "01.08.15.2.1",
            "LOCATION_NAME": "OUH Hjertemed. Amb. K (Odense)",
            "LOCATION_SORCODE": "581831000016007.0",
        },
        {
            "LOCATION_CODE": "01.08.34.2.4",
            "LOCATION_NAME": "OUH Ortopædkirurgisk Afs. 01 (Svendborg)",
            "LOCATION_SORCODE": "487261000016004.0",
        },
    ]


@pytest.fixture
def sample_encounter_location_rows() -> dict[str, list[dict[str, Any]]]:
    """
    Sample location data for encounters, keyed by encounter OBJECT_ID.

    This simulates the return value of _fetch_location_data_for_batch().
    Each encounter can have multiple locations with different periods.
    """
    return {
        "ENC001": [
            {
                "OBJECT_ID": "ENC001",
                "LOCATION_CODE": "01.04.24.2.4",
                "PERIOD_START": datetime(2025, 1, 10, 8, 0, 0),
                "PERIOD_END": datetime(2025, 1, 12, 10, 0, 0),
            },
            {
                "OBJECT_ID": "ENC001",
                "LOCATION_CODE": "01.08.15.2.1",
                "PERIOD_START": datetime(2025, 1, 12, 10, 0, 0),
                "PERIOD_END": None,  # Still at this location
            },
        ],
        "ENC002": [
            {
                "OBJECT_ID": "ENC002",
                "LOCATION_CODE": "01.08.34.2.4",
                "PERIOD_START": datetime(2025, 1, 5, 10, 0, 0),
                "PERIOD_END": datetime(2025, 1, 5, 14, 0, 0),
            },
        ],
    }


@pytest.fixture
def mock_ifdb_oracle():
    """Mock IFDB Oracle resource."""
    mock = MagicMock()
    mock.get_connection.return_value.__enter__ = MagicMock()
    mock.get_connection.return_value.__exit__ = MagicMock()
    return mock


@pytest.fixture
def mock_fhir_server():
    """Mock FHIR server resource."""
    mock = MagicMock()
    mock.resolve_id_by_identifier = MagicMock(return_value="fhir-id-123")
    mock.batch_fetch_patients_by_cpr = MagicMock(return_value={})
    mock.post_resource = MagicMock(return_value={"id": "fhir-id-123"})
    return mock


@pytest.fixture
def mock_fhir_database():
    """Mock FHIR database resource."""
    mock = MagicMock()
    mock.get_connection.return_value.__enter__ = MagicMock()
    mock.get_connection.return_value.__exit__ = MagicMock()
    mock.preload_identifier_cache = MagicMock()
    mock._identifier_cache = {}
    return mock


@pytest.fixture
def mock_dagster_context():
    """Mock Dagster asset execution context."""
    mock = MagicMock()
    mock.partition_key = "2025-01-15"
    mock.log = MagicMock()
    mock.run = MagicMock()
    mock.run.tags = {}
    mock.asset_key = MagicMock()
    return mock
