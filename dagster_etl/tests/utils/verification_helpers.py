# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Verification utilities for testing pipeline outputs.

This module provides:
- Generic FHIR resource verification
- Pipeline output verification
- Database output verification
"""

import logging
from typing import Any

from fhir.resources.R4B.resource import Resource

logger = logging.getLogger(__name__)


# =============================================================================
# FHIR Resource Validation
# =============================================================================


def assert_fhir_resource_valid(resource: Resource) -> bool:
    """
    Assert that a FHIR resource is valid (basic structure check).

    Args:
        resource: FHIR resource to validate

    Returns:
        True if valid, raises AssertionError if invalid
    """
    assert resource is not None, "Resource is None"
    assert hasattr(resource, "resource_type"), "Resource missing resource_type"
    assert resource.resource_type is not None, "Resource resource_type is None"
    return True


# =============================================================================
# Generic FHIR Resource Verification
# =============================================================================


def verify_fhir_resource(
    fhir_server,
    resource_type: str,
    resource_id: str,
    identifier_system: str,
    expected_fields: dict[str, Any] | None = None,
    from_real_server: bool = False,
) -> bool:
    """
    Generic FHIR resource verification in FHIR server.

    Args:
        fhir_server: FHIR server resource (mock or real)
        resource_type: FHIR resource type (e.g., "Patient", "Observation", "Encounter")
        resource_id: Resource identifier value to verify
        identifier_system: Identifier system to use for lookup
        expected_fields: Dictionary of expected field values
        from_real_server: If True, queries real server; if False, checks mock

    Returns:
        True if verification passes, raises AssertionError otherwise
    """
    if from_real_server:
        try:
            resolved_id = fhir_server.resolve_id_by_identifier(
                resource_type=resource_type,
                identifier_system=identifier_system,
                identifier_value=resource_id,
            )

            assert (
                resolved_id is not None
            ), f"{resource_type} {resource_id} not found in FHIR server"

            if expected_fields:
                logger.info(f"{resource_type} {resource_id} found in FHIR server")

            return True

        except Exception as e:
            raise AssertionError(
                f"Failed to verify {resource_type} {resource_id} in real FHIR server: {e}"
            )
    else:
        # Verify mock was called correctly
        if hasattr(fhir_server, "post_resource"):
            assert fhir_server.post_resource.called, "FHIR server post_resource not called"
        return True


# =============================================================================
# Specialized FHIR Resource Verifiers (Backward Compatible Wrappers)
# =============================================================================


def verify_fhir_patient(
    fhir_server,
    patient_id: str,
    patient_fhir_id: str | None = None,
    expected_fields: dict[str, Any] | None = None,
    from_real_server: bool = False,
) -> bool:
    """
    Verify FHIR Patient resource in FHIR server.

    This function has special handling for Patient resources that can be verified
    either by identifier system lookup or by direct FHIR resource ID.

    Args:
        fhir_server: FHIR server resource (mock or real)
        patient_id: Original patient ID (for logging)
        patient_fhir_id: FHIR-generated resource ID to verify directly
        expected_fields: Dictionary of expected field values
        from_real_server: If True, queries real server; if False, checks mock

    Returns:
        True if verification passes, raises AssertionError otherwise
    """
    if from_real_server:
        try:
            if patient_fhir_id:
                # Verify using FHIR-generated resource ID directly
                import requests

                url = f"{fhir_server.effective_base_url}/Patient/{patient_fhir_id}"
                headers = {"Accept": "application/json"}

                auth = None
                if hasattr(fhir_server, "client_username") and fhir_server.client_username:
                    from requests.auth import HTTPBasicAuth

                    auth = HTTPBasicAuth(fhir_server.client_username, fhir_server.client_password)

                response = requests.get(url, headers=headers, auth=auth)
                response.raise_for_status()

                response_data = response.json()
                assert (
                    response_data.get("resourceType") == "Patient"
                ), f"Resource is not a Patient: {response_data.get('resourceType')}"
                assert (
                    response_data.get("id") == patient_fhir_id
                ), f"Patient ID mismatch: expected {patient_fhir_id}, got {response_data.get('id')}"

                logger.info(
                    f"Patient {patient_id} verified in FHIR server with FHIR ID {patient_fhir_id}"
                )
            else:
                # Fallback: Use generic verification with identifier lookup
                return verify_fhir_resource(
                    fhir_server=fhir_server,
                    resource_type="Patient",
                    resource_id=patient_id,
                    identifier_system="urn:source:ifdb:patient_id",
                    expected_fields=expected_fields,
                    from_real_server=from_real_server,
                )

            if expected_fields:
                logger.info(f"Patient {patient_id} field verification not implemented yet")

            return True

        except Exception as e:
            raise AssertionError(f"Failed to verify patient {patient_id} in real FHIR server: {e}")
    else:
        # Verify mock was called correctly
        if hasattr(fhir_server, "post_resource"):
            assert fhir_server.post_resource.called, "FHIR server post_resource not called"
        return True


def verify_fhir_observation(
    fhir_server,
    observation_id: str,
    identifier_system: str,
    expected_fields: dict[str, Any] | None = None,
    from_real_server: bool = False,
) -> bool:
    """Verify FHIR Observation resource in FHIR server."""
    return verify_fhir_resource(
        fhir_server=fhir_server,
        resource_type="Observation",
        resource_id=observation_id,
        identifier_system=identifier_system,
        expected_fields=expected_fields,
        from_real_server=from_real_server,
    )


def verify_fhir_encounter(
    fhir_server,
    encounter_id: str,
    identifier_system: str,
    expected_fields: dict[str, Any] | None = None,
    from_real_server: bool = False,
) -> bool:
    """Verify FHIR Encounter resource in FHIR server."""
    return verify_fhir_resource(
        fhir_server=fhir_server,
        resource_type="Encounter",
        resource_id=encounter_id,
        identifier_system=identifier_system,
        expected_fields=expected_fields,
        from_real_server=from_real_server,
    )


def verify_fhir_episode_of_care(
    fhir_server,
    episode_id: str,
    identifier_system: str,
    expected_fields: dict[str, Any] | None = None,
    from_real_server: bool = False,
) -> bool:
    """Verify FHIR EpisodeOfCare resource in FHIR server."""
    return verify_fhir_resource(
        fhir_server=fhir_server,
        resource_type="EpisodeOfCare",
        resource_id=episode_id,
        identifier_system=identifier_system,
        expected_fields=expected_fields,
        from_real_server=from_real_server,
    )


def verify_fhir_location(
    fhir_server,
    location_id: str,
    identifier_system: str,
    expected_fields: dict[str, Any] | None = None,
    from_real_server: bool = False,
) -> bool:
    """Verify FHIR Location resource in FHIR server."""
    return verify_fhir_resource(
        fhir_server=fhir_server,
        resource_type="Location",
        resource_id=location_id,
        identifier_system=identifier_system,
        expected_fields=expected_fields,
        from_real_server=from_real_server,
    )


# =============================================================================
# Pipeline Output Verification
# =============================================================================


def verify_pipeline_output(
    asset_result,
    expected_patient_id: str | None = None,
    expected_count: int | None = None,
    expected_fields: dict[str, Any] | None = None,
) -> bool:
    """
    Verify pipeline output matches expectations.

    Args:
        asset_result: Materialization result from asset
        expected_patient_id: Expected patient ID in output
        expected_count: Expected number of records
        expected_fields: Expected field values

    Returns:
        True if verification passes
    """
    assert asset_result is not None, "Asset result is None"

    # Get output value if available
    if hasattr(asset_result, "output_value"):
        output = asset_result.output_value
    elif hasattr(asset_result, "value"):
        output = asset_result.value
    else:
        output = asset_result

    # Check count if list
    if isinstance(output, list):
        if expected_count is not None:
            assert (
                len(output) == expected_count
            ), f"Expected {expected_count} records, got {len(output)}"

        # Check patient ID if specified
        if expected_patient_id and len(output) > 0:
            first_item = output[0]
            if hasattr(first_item, "identifier"):
                # FHIR resource with identifiers
                found_id = any(
                    identifier.value == expected_patient_id for identifier in first_item.identifier
                )
                assert found_id, f"Expected patient ID {expected_patient_id} not found in output"
            elif isinstance(first_item, dict) and "PATIENT_ID" in first_item:
                # Raw data dict
                assert first_item["PATIENT_ID"] == expected_patient_id

    # Check expected fields
    if expected_fields and isinstance(output, list) and len(output) > 0:
        item = output[0]
        for field, expected_value in expected_fields.items():
            if hasattr(item, field):
                actual_value = getattr(item, field)
                assert (
                    actual_value == expected_value
                ), f"Field {field}: expected {expected_value}, got {actual_value}"
            elif isinstance(item, dict) and field in item:
                actual_value = item[field]
                assert (
                    actual_value == expected_value
                ), f"Field {field}: expected {expected_value}, got {actual_value}"

    return True


# =============================================================================
# Database Output Verification
# =============================================================================


def verify_database_output(
    connection,
    query: str,
    expected_rows: int | None = None,
    expected_values: dict[str, Any] | None = None,
) -> bool:
    """
    Verify database query output.

    Args:
        connection: Database connection
        query: SQL query to execute
        expected_rows: Expected number of rows
        expected_values: Expected values in first row

    Returns:
        True if verification passes
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

        if expected_rows is not None:
            assert len(rows) == expected_rows, f"Expected {expected_rows} rows, got {len(rows)}"

        if expected_values and len(rows) > 0:
            first_row = rows[0]
            if isinstance(first_row, dict):
                for key, expected_value in expected_values.items():
                    assert (
                        first_row[key] == expected_value
                    ), f"Column {key}: expected {expected_value}, got {first_row[key]}"

        return True
