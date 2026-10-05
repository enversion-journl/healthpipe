# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Utilities for creating FHIR References that work with identifier-based resolution.

This module provides a consistent approach to creating References across all FHIR assets.
The FHIR server we're using expects identifier-based references rather than direct
references (using the 'reference' field), so we keep the original identifier-based
reference and only verify the target resource exists.

Background:
-----------
FHIR supports two types of references:
1. Direct references: {"reference": "Patient/123"} - uses the server's internal ID
2. Identifier-based references: {"identifier": {"system": "...", "value": "..."}} -
   uses a business identifier that the server resolves at transaction time

Our FHIR server returns "extra property" errors when we use direct references,
so we use identifier-based references throughout.
"""

from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference


def verify_reference_exists(
    identifier_value: str,
    resolved_ids: dict[str, str],
    resource_type: str,
    raise_on_missing: bool = True,
) -> str | None:
    """
    Verify that a referenced resource exists in the FHIR server.

    Args:
        identifier_value: The identifier value to look up
        resolved_ids: Dict mapping identifier values to FHIR server IDs
        resource_type: The type of resource (for error messages)
        raise_on_missing: If True, raise ValueError when resource not found.
                         If False, return None instead.

    Returns:
        The FHIR server ID if found, None if not found and raise_on_missing=False

    Raises:
        ValueError: If the resource doesn't exist and raise_on_missing=True
    """
    fhir_id = resolved_ids.get(identifier_value)
    if not fhir_id:
        if raise_on_missing:
            raise ValueError(f"{resource_type} not found for identifier {identifier_value}")
        return None
    return fhir_id


def create_identifier_reference(
    identifier_system: str,
    identifier_value: str,
) -> Reference:
    """
    Create an identifier-based Reference.

    This creates a "logical" reference using an identifier, which is the format
    expected by our FHIR server. The server will resolve the identifier to the
    actual resource at transaction time.

    Args:
        identifier_system: The identifier system URI
        identifier_value: The identifier value

    Returns:
        A Reference with only the identifier field set
    """
    return Reference(
        identifier=Identifier(
            system=identifier_system,
            value=str(identifier_value),
        )
    )
