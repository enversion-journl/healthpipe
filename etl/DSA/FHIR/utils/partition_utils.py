# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Partition utilities for FHIR ETL assets.

This module provides shared functionality for handling date-based partitions
across different database types (Oracle, MSSQL) and partition formats
(daily, monthly, full_load).
"""

from calendar import monthrange
from datetime import datetime


def parse_monthly_partition(partition_key: str) -> tuple[str, str]:
    """
    Parse monthly partition key (YYYY-MM) and return start and end dates.

    Args:
        partition_key: Monthly partition key in format "YYYY-MM" (e.g., "2025-01")

    Returns:
        Tuple of (start_date, end_date) in format "YYYY-MM-DD"

    Raises:
        ValueError: If partition_key is not in YYYY-MM format
    """
    if len(partition_key) != 7 or partition_key.count("-") != 1:
        raise ValueError(
            f"Invalid monthly partition key format: {partition_key}. Expected 'YYYY-MM' format."
        )

    try:
        year, month = map(int, partition_key.split("-"))
        start_date = f"{year:04d}-{month:02d}-01"
        last_day = monthrange(year, month)[1]
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
        return (start_date, end_date)
    except ValueError as e:
        raise ValueError(
            f"Invalid monthly partition key: {partition_key}. Could not parse as year-month: {e}"
        ) from e


def get_month_date_range(partition_key: str) -> tuple[str, str]:
    """
    Get the start and end dates for a month from a partition key.

    Handles both formats:
    - "YYYY-MM" (e.g., "2025-01") - short format
    - "YYYY-MM-DD" (e.g., "2025-01-01") - Dagster's MonthlyPartitionsDefinition format

    Args:
        partition_key: Monthly partition key in either format

    Returns:
        Tuple of (start_date, end_date) in format "YYYY-MM-DD"
        The end_date is the last day of the month (inclusive)

    Raises:
        ValueError: If partition_key format is invalid

    Examples:
        >>> get_month_date_range("2025-01")
        ("2025-01-01", "2025-01-31")
        >>> get_month_date_range("2025-01-01")
        ("2025-01-01", "2025-01-31")
        >>> get_month_date_range("2025-02-01")  # February
        ("2025-02-01", "2025-02-28")
        >>> get_month_date_range("2024-02-01")  # Leap year
        ("2024-02-01", "2024-02-29")
    """
    # Handle "YYYY-MM" format
    if len(partition_key) == 7 and partition_key.count("-") == 1:
        return parse_monthly_partition(partition_key)

    # Handle "YYYY-MM-DD" format (Dagster's MonthlyPartitionsDefinition)
    if len(partition_key) == 10 and partition_key.count("-") == 2:
        try:
            dt = datetime.strptime(partition_key, "%Y-%m-%d")
            year = dt.year
            month = dt.month
            start_date = f"{year:04d}-{month:02d}-01"
            last_day = monthrange(year, month)[1]
            end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
            return (start_date, end_date)
        except ValueError as e:
            raise ValueError(
                f"Invalid monthly partition key: {partition_key}. Could not parse as date: {e}"
            ) from e

    raise ValueError(
        f"Invalid monthly partition key format: {partition_key}. "
        "Expected 'YYYY-MM' or 'YYYY-MM-DD' format."
    )


def build_date_filter_where_clause(
    partition_key: str, column_name: str, is_oracle: bool = False
) -> str:
    """
    Build WHERE clause based on partition key format.

    Args:
        partition_key: Partition key (daily: "YYYY-MM-DD", monthly: "YYYY-MM",
                      full_load: "full_load")
        column_name: Date column name to filter on
        is_oracle: True if Oracle database (uses different date functions)

    Returns:
        WHERE clause string (without "WHERE" keyword). Empty string for full_load.

    Raises:
        ValueError: If partition_key format is invalid
    """
    if partition_key == "full_load":
        return ""

    # Check if it's a monthly partition (YYYY-MM format)
    if len(partition_key) == 7 and partition_key.count("-") == 1:
        start_date, end_date = parse_monthly_partition(partition_key)
        if is_oracle:
            # Oracle uses DATE 'YYYY-MM-DD' format and TRUNC for date comparison
            return (
                f"TRUNC(CAST({column_name} AS DATE)) >= DATE '{start_date}' "
                f"AND TRUNC(CAST({column_name} AS DATE)) <= DATE '{end_date}'"
            )
        else:
            # MSSQL uses CAST and standard date comparison
            return (
                f"CAST({column_name} AS DATE) >= '{start_date}' "
                f"AND CAST({column_name} AS DATE) <= '{end_date}'"
            )

    # Check if it's a daily partition (YYYY-MM-DD format)
    if len(partition_key) == 10 and partition_key.count("-") == 2:
        if is_oracle:
            # Oracle: use TRUNC for date comparison
            return f"TRUNC(CAST({column_name} AS DATE)) = DATE '{partition_key}'"
        else:
            return f"CAST({column_name} AS DATE) = '{partition_key}'"

    # Invalid format
    raise ValueError(
        f"Invalid partition key format: {partition_key}. "
        "Expected 'full_load', 'YYYY-MM-DD', or 'YYYY-MM'."
    )
