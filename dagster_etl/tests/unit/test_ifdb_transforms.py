# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for IFDB transform functions.
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

# Check if fhir.ifdb.yml exists (used by build_select_query_for_canonical)
_YAML_PATH = Path(__file__).resolve().parents[2] / "model" / "fhir.ifdb.yml"
requires_fhir_ifdb_yaml = pytest.mark.skipif(
    not _YAML_PATH.exists(),
    reason="fhir.ifdb.yml not present on this branch",
)
from core.resources import IFDBOracleResource
from DSA.FHIR.utils.ifdb.transforms import (
    build_select_query_for_canonical,
    is_true_flag,
    source_deleted_to_int,
    transform_adapter_ifdb_encounter_periode,
    transform_fhir_encounter_periode,
)


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


@requires_fhir_ifdb_yaml
@pytest.mark.integration
def test_build_select_query_for_canonical():
    """Test building SELECT query from canonical definition."""
    query = build_select_query_for_canonical(
        canonical_name="encounter_periode",
        partition_date="2025-10-29",
        partition_column="IFDB_MODIFICATION_DATETIME",
    )

    assert query is not None
    assert "SELECT" in query.upper()
    assert "IFDB.ENCOUNTER" in query
    assert "IFDB_MODIFICATION_DATETIME" in query
    assert "2025-10-29" in query


@requires_fhir_ifdb_yaml
def test_build_select_query_invalid_canonical():
    """Test building query with invalid canonical name."""
    with pytest.raises(ValueError):
        build_select_query_for_canonical(
            canonical_name="invalid_resource",
            partition_date="2025-10-29",
        )


@patch("DSA.FHIR.utils.ifdb.transforms.build_select_query_for_canonical")
def test_transform_adapter_ifdb_encounter_periode(mock_build_query):
    """Test adapter transform function."""
    # Setup mock
    mock_query = "SELECT OBJECT_ID, PATIENT_ID FROM IFDB.ENCOUNTER WHERE CAST(IFDB_MODIFICATION_DATETIME AS DATE) = DATE '2025-10-29'"
    mock_build_query.return_value = mock_query

    # Mock context
    mock_context = Mock()
    mock_context.log = Mock()
    mock_context.log.info = Mock()

    # Mock Oracle resource
    mock_oracle = Mock(spec=IFDBOracleResource)
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Setup cursor to return test data
    mock_cursor.description = [("ENCOUNTER_ID",), ("PATIENT_ID",)]
    mock_cursor.fetchall.return_value = [
        ("1", "1"),
        ("2", "1"),
    ]
    mock_cursor.close = Mock()

    mock_conn.cursor.return_value = mock_cursor

    # Create proper context manager for get_connection
    from contextlib import contextmanager

    @contextmanager
    def mock_get_connection():
        yield mock_conn

    mock_oracle.get_connection = mock_get_connection

    # Execute transform
    rows = transform_adapter_ifdb_encounter_periode(
        context=mock_context,
        ifdb_oracle=mock_oracle,
        partition_date="2025-10-29",
    )

    # Verify
    assert len(rows) == 2
    assert rows[0]["ENCOUNTER_ID"] == "1"
    assert rows[0]["PATIENT_ID"] == "1"

    # Verify query was built
    mock_build_query.assert_called_once()
    # Verify cursor was closed
    mock_cursor.close.assert_called_once()


def test_transform_fhir_encounter_periode():
    """Test FHIR transformation function."""
    # Mock context
    mock_context = Mock()
    mock_context.log = Mock()
    mock_context.log.info = Mock()

    # Test data
    encounter_rows = [
        {
            "encounter_id": "1",
            "patient_id": "1",
            "episode_id": "1",
            "status_text": "finished",
            "class_text": "inpatient",
            "period_start": datetime(2025, 1, 1, 10, 0, 0),
            "period_end": datetime(2025, 1, 5, 15, 0, 0),
        },
        {
            "encounter_id": "2",
            "patient_id": "1",
            "episode_id": None,
            "status_text": "in-progress",
            "class_text": "outpatient",
            "period_start": datetime(2025, 1, 10, 9, 0, 0),
            "period_end": None,
        },
    ]

    # Execute transform
    encounters = transform_fhir_encounter_periode(
        context=mock_context,
        encounter_rows=encounter_rows,
    )

    # Verify
    assert len(encounters) == 2

    # Check first encounter
    enc1 = encounters[0]
    assert enc1.identifier[0].value == "1"
    assert enc1.subject.identifier.value == "1"
    assert enc1.status == "finished"
    assert enc1.episodeOfCare is not None
    assert enc1.period.start is not None
    assert enc1.period.end is not None

    # Check second encounter
    enc2 = encounters[1]
    assert enc2.identifier[0].value == "2"
    assert enc2.status == "in-progress"
    assert enc2.episodeOfCare is None  # No episode_id
    assert enc2.period.start is not None
    assert enc2.period.end is None  # No end date


def test_transform_fhir_encounter_empty_list():
    """Test transformation with empty input."""
    mock_context = Mock()
    mock_context.log = Mock()
    mock_context.log.info = Mock()

    encounters = transform_fhir_encounter_periode(
        context=mock_context,
        encounter_rows=[],
    )

    assert len(encounters) == 0


def test_transform_fhir_encounter_missing_fields():
    """Test transformation with missing optional fields."""
    mock_context = Mock()
    mock_context.log = Mock()
    mock_context.log.info = Mock()

    encounter_rows = [
        {
            "encounter_id": "1",
            "patient_id": None,  # Missing patient
            "episode_id": None,
            "status_text": None,  # Missing status
            "class_text": None,  # Missing class
            "period_start": None,
            "period_end": None,
        },
    ]

    encounters = transform_fhir_encounter_periode(
        context=mock_context,
        encounter_rows=encounter_rows,
    )

    assert len(encounters) == 1
    enc = encounters[0]
    assert enc.identifier[0].value == "1"
    assert enc.subject is None  # No patient_id
    assert enc.status == "unknown"  # Default for missing status
    assert enc.period is None  # No period dates
    # class_fhir is the attribute name (class is a Python keyword)
    assert hasattr(enc, "class_fhir")
    assert enc.class_fhir.code == "unknown"  # Default class code
