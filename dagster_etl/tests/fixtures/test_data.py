"""
Test data management utilities.

This module provides:
- Patient data loading (mock and real)
- Partition data creation for tests
- Mock cursor response creation
"""

import logging
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

logger = logging.getLogger(__name__)


# =============================================================================
# SQL Query Templates (parameterized for safety)
# =============================================================================

# Column list for patient queries
_PATIENT_COLUMNS = """
    PATIENT_ID,
    CPR,
    FORNAVNE,
    EFTERNAVN,
    FOEDSELSDATO,
    KOEN_KODE,
    KOEN_KODETEKST,
    POSTNR,
    STEDNAVN,
    VEJ,
    MODTIDSPUNKT
"""

# MSSQL query with parameter placeholder
_MSSQL_PATIENT_QUERY = f"""
    SELECT TOP 1 {_PATIENT_COLUMNS}
    FROM dbo.PATIENT_OPLYSNINGER_COLUMNA
    WHERE PATIENT_ID = %s
"""

# Oracle query with parameter placeholder (uses :param syntax)
_ORACLE_PATIENT_QUERY = f"""
    SELECT {_PATIENT_COLUMNS}
    FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA
    WHERE PATIENT_ID = :patient_id
    AND ROWNUM = 1
"""


# =============================================================================
# Patient Data Column Mapping
# =============================================================================


def _row_to_patient_dict(row: tuple) -> dict[str, Any]:
    """Convert a database row tuple to a patient dictionary."""
    return {
        "PATIENT_ID": row[0],
        "CPR": row[1],
        "FORNAVNE": row[2],
        "EFTERNAVN": row[3],
        "FOEDSELSDATO": row[4],
        "KOEN_KODE": row[5],
        "KOEN_KODETEKST": row[6],
        "POSTNR": row[7],
        "STEDNAVN": row[8],
        "VEJ": row[9],
        "MODTIDSPUNKT": row[10],
    }


def _get_mock_patient_data(patient_id: str) -> dict[str, Any]:
    """Return mock patient data for testing without database."""
    return {
        "PATIENT_ID": patient_id,
        "CPR": "0101901234",
        "FORNAVNE": "Test",
        "EFTERNAVN": "Patient",
        "FOEDSELSDATO": datetime(1990, 1, 1),
        "KOEN_KODE": "1",
        "KOEN_KODETEKST": "Mand",
        "POSTNR": "5000",
        "STEDNAVN": "Odense",
        "VEJ": "Test Street 123",
        "MODTIDSPUNKT": datetime.now(),
    }


# =============================================================================
# Patient Data Loading
# =============================================================================


def load_test_patient_data(
    patient_id: str,
    use_real_db: bool = False,
    connection=None,
    datasource: str = "mssql_source",
) -> dict[str, Any]:
    """
    Load test patient data for a specific PATIENT_ID.

    Args:
        patient_id: Patient ID to load data for
        use_real_db: If True, query real database; if False, return mock data
        connection: Database connection (required if use_real_db=True)
        datasource: Datasource to use ("mssql_source" or "ifdb_oracle")

    Returns:
        Dictionary with patient data

    Raises:
        ValueError: If patient not found in database or invalid datasource
    """
    if not use_real_db or connection is None:
        return _get_mock_patient_data(patient_id)

    # Validate and convert patient_id to int for database query
    try:
        patient_id_int = int(patient_id)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid patient_id format: {patient_id}") from e

    with connection.cursor() as cursor:
        if datasource == "mssql_source":
            # Use parameterized query for MSSQL (pymssql uses %s placeholders)
            cursor.execute(_MSSQL_PATIENT_QUERY, (patient_id_int,))
        elif datasource == "ifdb_oracle":
            # Use parameterized query for Oracle (uses :name placeholders)
            cursor.execute(_ORACLE_PATIENT_QUERY, {"patient_id": patient_id_int})
        else:
            raise ValueError(f"Invalid datasource: {datasource}")

        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Patient {patient_id} not found in database")

        return _row_to_patient_dict(row)


def get_real_patient_data(
    connection,
    patient_id: str,
    datasource: str = "ifdb_oracle",
) -> dict[str, Any] | None:
    """
    Query real database for specific patient data.

    Args:
        connection: Database connection
        patient_id: Patient ID to query
        datasource: Datasource to use ("mssql_source" or "ifdb_oracle")

    Returns:
        Patient data dict or None if not found
    """
    try:
        return load_test_patient_data(
            patient_id,
            use_real_db=True,
            connection=connection,
            datasource=datasource,
        )
    except (ValueError, Exception):
        return None


def verify_real_patient_exists(
    connection,
    patient_id: str,
    datasource: str = "ifdb_oracle",
) -> bool:
    """
    Check if patient exists in real database.

    Args:
        connection: Database connection
        patient_id: Patient ID to check
        datasource: Datasource to use ("mssql_source" or "ifdb_oracle")

    Returns:
        True if patient exists, False otherwise
    """
    try:
        data = get_real_patient_data(connection, patient_id, datasource)
        return data is not None
    except Exception as e:
        logger.debug(f"Error checking patient {patient_id}: {type(e).__name__}: {e}")
        return False


# =============================================================================
# Test Data Creation Utilities
# =============================================================================


def create_test_partition_data(
    date_str: str,
    patient_ids: list[str],
    datasource: str = "ifdb_oracle",
) -> dict[str, list[dict[str, Any]]]:
    """
    Create mock partition test data for multiple patients.

    Args:
        date_str: Partition date (YYYY-MM-DD)
        patient_ids: List of patient IDs to include
        datasource: Datasource to use (for future compatibility)

    Returns:
        Dictionary with partition date as key and list of patient records as value
    """
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    patients = []
    for patient_id in patient_ids:
        patient_data = _get_mock_patient_data(patient_id)
        patient_data["MODTIDSPUNKT"] = datetime.combine(partition_date, datetime.min.time())
        patients.append(patient_data)

    return {date_str: patients}


def create_mock_mssql_cursor_response(
    patient_id: str,
    patient_data: dict[str, Any] | None = None,
) -> MagicMock:
    """
    Create a mock cursor with patient data response.

    Args:
        patient_id: Patient ID
        patient_data: Optional patient data dict, or None to use default

    Returns:
        Mock cursor object with fetchall returning patient data
    """
    if patient_data is None:
        patient_data = _get_mock_patient_data(patient_id)

    mock_cursor = MagicMock()
    row = tuple(patient_data.values())
    mock_cursor.fetchall.return_value = [row]

    return mock_cursor
