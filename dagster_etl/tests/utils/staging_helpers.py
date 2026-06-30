# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Helper utilities for staging table validation in integration tests.
"""

from typing import Any


def query_staging_table(target_resource, table_name: str) -> list[tuple]:
    """
    Query a staging table and return all rows.

    Args:
        target_resource: MSSQL target resource with get_connection() method
        table_name: Name of the staging table (without schema prefix)

    Returns:
        List of tuples containing row data

    Example:
        >>> rows = query_staging_table(target_resource, "metadata_mysupply")
        >>> assert len(rows) >= 1
    """
    with target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM [stg].[{table_name}]")
            rows = cursor.fetchall()
        finally:
            cursor.close()
        return rows


def find_test_record_by_value(
    rows: list[tuple], column_index: int, expected_value: Any
) -> tuple | None:
    """
    Find a specific test record in result rows by matching a column value.

    Args:
        rows: List of row tuples from database query
        column_index: Index of column to match against
        expected_value: Value to search for

    Returns:
        The first matching row tuple, or None if not found

    Example:
        >>> rows = [(1, "12345678", ...), (2, "99999999", ...)]
        >>> test_row = find_test_record_by_value(rows, 1, "12345678")
        >>> assert test_row[1] == "12345678"
    """
    for row in rows:
        if row[column_index] == expected_value:
            return row
    return None


def query_and_find_test_record(
    target_resource, table_name: str, column_index: int, expected_value: Any
) -> tuple:
    """
    Query staging table and find specific test record by column value.

    Convenience function combining query_staging_table and find_test_record_by_value.
    Raises AssertionError if record not found.

    Args:
        target_resource: MSSQL target resource
        table_name: Name of the staging table (without schema prefix)
        column_index: Index of column to match against
        expected_value: Value to search for

    Returns:
        The matching row tuple

    Raises:
        AssertionError: If no rows found or test record not found

    Example:
        >>> row = query_and_find_test_record(
        ...     target_resource,
        ...     "metadata_mysupply",
        ...     0,  # First column (CVR)
        ...     "12345678"
        ... )
        >>> assert row[0] == "12345678"
    """
    rows = query_staging_table(target_resource, table_name)
    assert len(rows) >= 1, f"No rows found in stg.{table_name}"

    test_row = find_test_record_by_value(rows, column_index, expected_value)
    assert (
        test_row is not None
    ), f"Test record with {expected_value} at column {column_index} not found in stg.{table_name}"

    return test_row


def validate_clean_code_generated(
    row: tuple, clean_code_index: int, expected_clean_code: str
) -> None:
    """
    Validate that a clean code was generated correctly in a staging row.

    Args:
        row: Row tuple from staging table
        clean_code_index: Index of clean code column
        expected_clean_code: Expected clean code value (e.g., "CVR_12345678")

    Raises:
        AssertionError: If clean code doesn't match

    Example:
        >>> validate_clean_code_generated(row, 2, "CVR_12345678")
    """
    actual_clean_code = row[clean_code_index]
    assert (
        actual_clean_code == expected_clean_code
    ), f"Clean code mismatch: expected '{expected_clean_code}', got '{actual_clean_code}'"


def validate_source_system(row: tuple, source_system_index: int, expected_system: str) -> None:
    """
    Validate that the source system is correctly set in a staging row.

    Args:
        row: Row tuple from staging table
        source_system_index: Index of SourceSystem column
        expected_system: Expected source system name (e.g., "MySupply", "SydAX", "ILS")

    Raises:
        AssertionError: If source system doesn't match

    Example:
        >>> validate_source_system(row, 3, "MySupply")
    """
    actual_system = row[source_system_index]
    assert (
        actual_system == expected_system
    ), f"Source system mismatch: expected '{expected_system}', got '{actual_system}'"
