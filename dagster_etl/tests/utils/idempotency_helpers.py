# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared utilities for idempotency testing across all FHIR resource types.

These helpers provide generic functions to:
- Count resources by identifier
- Get FHIR ID by identifier
- Delete test resources by identifier

All functions use PostgreSQL's JSONB containment operator (@>) for efficient querying.
"""

import json
import logging
from typing import Any

from core.resources import FHIRDatabaseResource

logger = logging.getLogger(__name__)


def count_resources_by_identifier(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    identifier_value: str,
) -> int:
    """
    Count how many resources exist with a given identifier.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        identifier_value: The identifier value to search for

    Returns:
        Number of resources matching the identifier
    """
    table_name = resource_type.lower()
    json_pattern = json.dumps(
        {"identifier": [{"system": identifier_system, "value": identifier_value}]}
    )

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE resource @> %s
                """,
                [json_pattern],
            )
            return cur.fetchone()[0]


def get_fhir_id_by_identifier(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    identifier_value: str,
) -> str | None:
    """
    Get the FHIR UUID for a resource by its identifier.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        identifier_value: The identifier value to search for

    Returns:
        The FHIR UUID if found, None otherwise
    """
    table_name = resource_type.lower()
    json_pattern = json.dumps(
        {"identifier": [{"system": identifier_system, "value": identifier_value}]}
    )

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id FROM {table_name}
                WHERE resource @> %s
                LIMIT 1
                """,
                [json_pattern],
            )
            row = cur.fetchone()
            return row[0] if row else None


def delete_test_resources(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    identifier_values: list[str],
) -> int:
    """
    Delete test resources by their identifiers.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        identifier_values: List of identifier values to delete

    Returns:
        Total number of resources deleted
    """
    table_name = resource_type.lower()
    deleted = 0

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            for identifier_value in identifier_values:
                json_pattern = json.dumps(
                    {"identifier": [{"system": identifier_system, "value": identifier_value}]}
                )
                cur.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE resource @> %s
                    """,
                    [json_pattern],
                )
                deleted += cur.rowcount
        conn.commit()

    logger.info(f"Deleted {deleted} {resource_type} test resources")
    return deleted


def get_resource_by_identifier(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    identifier_value: str,
) -> dict[str, Any] | None:
    """
    Get the full resource JSON by its identifier.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        identifier_value: The identifier value to search for

    Returns:
        The full resource as a dict if found, None otherwise
    """
    table_name = resource_type.lower()
    json_pattern = json.dumps(
        {"identifier": [{"system": identifier_system, "value": identifier_value}]}
    )

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT resource FROM {table_name}
                WHERE resource @> %s
                LIMIT 1
                """,
                [json_pattern],
            )
            row = cur.fetchone()
            return row[0] if row else None


def get_all_resources_by_identifier_prefix(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    identifier_prefix: str,
) -> list[dict[str, Any]]:
    """
    Get all resources whose identifier value starts with a given prefix.

    Useful for finding test resources that use a unique test prefix.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        identifier_prefix: The prefix to search for

    Returns:
        List of resources matching the prefix
    """
    table_name = resource_type.lower()

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            # Use JSONB path query for prefix matching
            cur.execute(
                f"""
                SELECT id, resource FROM {table_name}
                WHERE EXISTS (
                    SELECT 1 FROM jsonb_array_elements(resource->'identifier') AS ident
                    WHERE ident->>'system' = %s
                    AND ident->>'value' LIKE %s
                )
                """,
                [identifier_system, f"{identifier_prefix}%"],
            )
            return [{"id": row[0], "resource": row[1]} for row in cur.fetchall()]


def delete_resources_by_identifier_prefix(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    identifier_prefix: str,
) -> int:
    """
    Delete all resources whose identifier value starts with a given prefix.

    Useful for cleaning up test resources that use a unique test prefix.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        identifier_prefix: The prefix to search for

    Returns:
        Total number of resources deleted
    """
    table_name = resource_type.lower()

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {table_name}
                WHERE EXISTS (
                    SELECT 1 FROM jsonb_array_elements(resource->'identifier') AS ident
                    WHERE ident->>'system' = %s
                    AND ident->>'value' LIKE %s
                )
                """,
                [identifier_system, f"{identifier_prefix}%"],
            )
            deleted = cur.rowcount
        conn.commit()

    logger.info(
        f"Deleted {deleted} {resource_type} test resources with prefix '{identifier_prefix}'"
    )
    return deleted


def verify_no_duplicates(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    identifier_values: list[str],
) -> tuple[bool, list[str]]:
    """
    Verify that no duplicate resources exist for the given identifiers.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        identifier_values: List of identifier values to check

    Returns:
        Tuple of (all_unique: bool, duplicate_identifiers: list[str])
    """
    duplicates = []

    for identifier_value in identifier_values:
        count = count_resources_by_identifier(
            fhir_database=fhir_database,
            resource_type=resource_type,
            identifier_system=identifier_system,
            identifier_value=identifier_value,
        )
        if count > 1:
            duplicates.append(identifier_value)

    return len(duplicates) == 0, duplicates


def verify_fhir_ids_preserved(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    original_ids: dict[str, str],
) -> tuple[bool, list[str]]:
    """
    Verify that FHIR UUIDs are preserved after updates.

    Args:
        fhir_database: The FHIR database resource
        resource_type: FHIR resource type (e.g., "Patient", "Condition")
        identifier_system: The identifier system URI
        original_ids: Dict mapping identifier_value -> original_fhir_id

    Returns:
        Tuple of (all_preserved: bool, changed_identifiers: list[str])
    """
    changed = []

    for identifier_value, original_fhir_id in original_ids.items():
        current_fhir_id = get_fhir_id_by_identifier(
            fhir_database=fhir_database,
            resource_type=resource_type,
            identifier_system=identifier_system,
            identifier_value=identifier_value,
        )
        if current_fhir_id != original_fhir_id:
            changed.append(identifier_value)

    return len(changed) == 0, changed
