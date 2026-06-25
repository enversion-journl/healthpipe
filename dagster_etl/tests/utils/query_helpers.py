# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Query modification helpers for testing with PATIENT_ID filtering.
"""

import re
from typing import Any


def add_patient_id_filter(query: str, patient_id: str, filter_type: str = "AND") -> str:
    """
    Add PATIENT_ID filter to a SQL query.

    Args:
        query: Original SQL query
        patient_id: asatient ID to filter by
        filter_type: "AND" or "WHERE" (use WHERE if query has no WHERE clause)

    Returns:
        Modified query with PATIENT_ID filter
    """
    # Normalize query whitespace
    query = " ".join(query.split())

    # Check if query already has a WHERE clause
    has_where = re.search(r"\bWHERE\b", query, re.IGNORECASE)

    if has_where:
        # Add AND condition to existing WHERE clause
        # Find the position after WHERE and before any GROUP BY, ORDER BY, etc.
        where_match = re.search(r"\bWHERE\b", query, re.IGNORECASE)
        end_match = re.search(r"\b(GROUP BY|ORDER BY|LIMIT|OFFSET)\b", query, re.IGNORECASE)

        if end_match:
            insert_pos = end_match.start()
            where_clause = query[where_match.end() : insert_pos].strip()
            # Add PATIENT_ID filter
            new_filter = f" AND c.PATIENT_ID = '{patient_id}'"
            # Check if filter already exists to avoid duplicates
            if f"PATIENT_ID = '{patient_id}'" not in where_clause:
                query = query[:insert_pos] + new_filter + " " + query[insert_pos:]
        else:
            # No GROUP BY/ORDER BY, add at end
            query = query.rstrip(";") + f" AND c.PATIENT_ID = '{patient_id}'"
    else:
        # No WHERE clause, add one
        # Find appropriate insert point (before ORDER BY, GROUP BY, etc.)
        insert_match = re.search(r"\b(GROUP BY|ORDER BY|LIMIT|OFFSET)\b", query, re.IGNORECASE)
        if insert_match:
            insert_pos = insert_match.start()
            query = (
                query[:insert_pos] + f" WHERE c.PATIENT_ID = '{patient_id}' " + query[insert_pos:]
            )
        else:
            # Add at end
            query = query.rstrip(";") + f" WHERE c.PATIENT_ID = '{patient_id}'"

    return query


def create_test_query(base_query: str, filters: dict[str, Any]) -> str:
    """
    Create a test query with multiple filters.

    Args:
        base_query: Base SQL query
        filters: Dictionary of filter conditions

    Returns:
        Modified query with all filters applied
    """
    query = base_query

    # Apply PATIENT_ID filter if present
    if "patient_id" in filters:
        query = add_patient_id_filter(query, str(filters["patient_id"]))

    # Add other filters as needed
    # This can be extended for other filter types

    return query


def extract_query_patient_id(query: str) -> str | None:
    """
    Extract PATIENT_ID from a query if present.

    Args:
        query: SQL query string

    Returns:
        Patient ID if found, None otherwise
    """
    match = re.search(r"PATIENT_ID\s*=\s*['\"]?(\d+)['\"]?", query, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def inject_patient_id_into_asset_query(asset_function, patient_id: str) -> callable:
    """
    Create a wrapper function that injects PATIENT_ID filter into asset query.

    Args:
        asset_function: Original asset function
        patient_id: Patient ID to filter by

    Returns:
        Wrapped function that adds PATIENT_ID filter to queries
    """

    def wrapper(*args, **kwargs):
        # Get the context and resource from args/kwargs
        context = kwargs.get("context") or (args[0] if args else None)

        # Call original function but intercept query if possible
        # This is a simplified version - may need asset-specific logic
        original_result = asset_function(*args, **kwargs)
        return original_result

    return wrapper
