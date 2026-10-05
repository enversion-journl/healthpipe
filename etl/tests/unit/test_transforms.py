# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for transform functions and utilities.
"""

from datetime import UTC

import pytest
from DSA.FHIR.utils.fhir_utils import fhir_date, fhir_datetime, find_identifier
from DSI.connectors.mssql.extractor import clean_ils_data as _clean_ils_data
from fhir.resources.R4B.patient import Patient
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID


@pytest.mark.unit
def test_fhir_date_conversion():
    """Test FHIR date conversion utility."""
    from datetime import date, datetime

    # Test with date object
    # fhir_date returns a FHIR Date object which validates to string format
    test_date = date(1990, 1, 1)
    result = fhir_date(test_date)
    assert str(result) == "1990-01-01" or result == "1990-01-01"

    # Test with datetime object
    test_datetime = datetime(1990, 1, 1, 12, 30, 0)
    result = fhir_date(test_datetime)
    assert str(result) == "1990-01-01" or result == "1990-01-01"

    # Test with string
    result = fhir_date("1990-01-01")
    assert str(result) == "1990-01-01" or result == "1990-01-01"


@pytest.mark.unit
def test_fhir_datetime_conversion():
    """Test FHIR datetime conversion utility."""
    from datetime import datetime

    # Test with datetime (UTC)
    test_dt = datetime(1990, 1, 1, 12, 30, 0, tzinfo=UTC)
    result = fhir_datetime(test_dt)
    # fhir_datetime may return a string or datetime object
    result_str = str(result)
    # Check that it contains the expected date and time components
    assert "1990-01-01" in result_str
    assert "12:30:00" in result_str

    # Test with naive datetime (assumed UTC)
    test_dt = datetime(1990, 1, 1, 12, 30, 0)
    result = fhir_datetime(test_dt)
    result_str = str(result)
    assert "1990-01-01" in result_str
    assert "12:30:00" in result_str


@pytest.mark.unit
def test_find_identifier():
    """Test finding identifier in FHIR resource."""
    from fhir.resources.R4B.identifier import Identifier

    # Create patient with identifiers
    patient = Patient(
        identifier=[
            Identifier(system=SYSTEM_CPR, value="0101901234"),
            Identifier(system=SYSTEM_IFDB_PATIENT_ID, value="12345"),
        ]
    )

    # Find by system
    cpr = find_identifier(patient, SYSTEM_CPR)
    assert cpr == "0101901234"

    patient_id = find_identifier(patient, SYSTEM_IFDB_PATIENT_ID)
    assert patient_id == "12345"

    # Non-existent system
    missing = find_identifier(patient, "urn:non-existent")
    assert missing is None


@pytest.mark.unit
def test_clean_ils_data_handles_invalid_numeric_strings():
    """Test that _clean_ils_data handles strings ending with .0 that are not valid numbers."""
    # Test data that reproduces the error: string ending with .0 but containing text
    test_data = [
        ("Tecnis Toric. Cyl 3.75/ +25.0",),  # This caused the ValueError - should remain unchanged
        ("123.0",),  # Valid numeric string that should be cleaned
        ("abc.0",),  # Invalid numeric string ending with .0 - should remain unchanged
        ("456.789",),  # Valid float string - should be converted to float
        ("normal_text",),  # Regular string - should remain unchanged
    ]

    column_descriptions = [("test_column", None, None, None, None, None, None)]

    # This should not raise an exception
    result = _clean_ils_data(test_data, column_descriptions)

    # Verify the results
    assert len(result) == 5

    # Check specific transformations
    assert result[0] == ("Tecnis Toric. Cyl 3.75/ +25.0",)  # Should remain unchanged
    assert result[1] == ("123",)  # Should be cleaned (removed .0)
    assert result[2] == ("abc.0",)  # Should remain unchanged (invalid number)
    assert result[3] == ("456.789",)  # Non-integer float string stays as-is
    assert result[4] == ("normal_text",)  # Should remain unchanged
