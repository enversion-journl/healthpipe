# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Partition utilities for FHIR ETL assets.

This module parses monthly partition keys into date ranges.
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
