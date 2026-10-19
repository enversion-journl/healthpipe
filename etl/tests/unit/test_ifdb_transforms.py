# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for IFDB transform functions.
"""

from unittest.mock import MagicMock

import pytest
from DSA.FHIR.Patient.patient_ifdb_contribution import transform_fhir_patient_ifdb
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    source_deleted_to_int,
)
from utils.fhir.fhir_extensions import EXTENSION_ADDRESS_PROTECTION

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("value", [True, 1, 1.0, "1", "true", "TRUE", "yes", "Y", " t "])
def test_is_true_flag_truthy_values(value):
    assert is_true_flag(value) is True


@pytest.mark.parametrize("value", [False, None, 0, 2, "0", "false", "no", "", "abc"])
def test_is_true_flag_falsey_values(value):
    assert is_true_flag(value) is False


@pytest.mark.parametrize("value", [True, 1, 2, -1, 1.0, "1", "true", "yes", "Y"])
def test_source_deleted_to_int_deleted_values(value):
    assert source_deleted_to_int(value) == 1


@pytest.mark.parametrize("value", [False, None, 0, 0.0, "0", "false", "no", "", "abc"])
def test_source_deleted_to_int_not_deleted_values(value):
    assert source_deleted_to_int(value) == 0


@pytest.mark.unit
def test_protected_patient_still_carries_address():
    """Protected patients still carry address plus the protection extension."""
    context = MagicMock()
    patients = transform_fhir_patient_ifdb(
        context=context,
        patient_rows=[
            {
                "patient_id": "999002",
                "identifier_cpr": "1506801234",
                "name_given": "Anna",
                "name_family": "Larsen",
                "address_street": "Hemmelig Vej 1",
                "address_postal": "7000",
                "address_city": "Fredericia",
                "address_protection": True,
            }
        ],
    )

    assert len(patients) == 1
    patient = patients[0]
    assert patient.address is not None
    assert patient.address[0].line == ["Hemmelig Vej 1"]
    assert patient.address[0].postalCode == "7000"
    protection = [e for e in (patient.extension or []) if e.url == EXTENSION_ADDRESS_PROTECTION]
    assert len(protection) == 1
    assert protection[0].valueBoolean is True
