# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
GDPR-compliant logging utilities for FHIR resources.

This module provides functions to mask sensitive personal data in FHIR resources
before logging them for debugging purposes.
"""

import re
from typing import Any

# Danish CPR number pattern: 6 digits + 4 digits (DDMMYY-XXXX), dash optional.
# CPR is handled both with and without the separating dash; both must mask.
CPR_PATTERN = re.compile(r"\b\d{6}-?\d{4}\b")

# Common Danish name patterns (simplified)
NAME_PATTERN = re.compile(r"\b[A-ZÆØÅ][a-zæøå]+(?:\s+[A-ZÆØÅ][a-zæøå]+)+\b")

# Address patterns (street names, postal codes, city names)
ADDRESS_PATTERN = re.compile(r"\b\d+\s+[A-ZÆØÅ][a-zæøå]+\s*\d*\b")  # Street addresses
POSTAL_PATTERN = re.compile(r"\b\d{4}\b")  # Danish postal codes


def mask_cpr_numbers(text: str) -> str:
    """Mask CPR numbers in text by replacing with asterisks."""
    return CPR_PATTERN.sub("XXXXXX-XXXX", text)


def mask_names(text: str) -> str:
    """Mask potential names in text by replacing with asterisks."""
    return NAME_PATTERN.sub("***NAME***", text)


def mask_addresses(text: str) -> str:
    """Mask potential address information in text."""
    # Mask street addresses
    text = ADDRESS_PATTERN.sub("***STREET***", text)
    # Mask postal codes
    text = POSTAL_PATTERN.sub("****", text)
    return text


def mask_fhir_resource_for_logging(resource: Any) -> str:
    """
    Create a masked JSON representation of a FHIR resource for safe logging.

    This function recursively traverses the resource and masks sensitive fields
    like names, addresses, CPR numbers, and other personal identifiers.

    Args:
        resource: FHIR resource object (Patient, EpisodeOfCare, etc.)

    Returns:
        JSON string representation with sensitive data masked
    """
    try:
        # Get the resource as a dict
        if hasattr(resource, "dict"):
            resource_dict = resource.dict(exclude_none=True)
        elif hasattr(resource, "__dict__"):
            resource_dict = resource.__dict__
        elif isinstance(resource, dict):
            resource_dict = resource.copy()
        else:
            return str(resource)

        # Recursively mask the dictionary
        masked_dict = _mask_dict_recursive(resource_dict)

        # Convert back to JSON-like string
        import json

        return json.dumps(masked_dict, indent=2, default=str, ensure_ascii=False)

    except Exception:
        # Fallback: convert to string and mask what we can
        resource_str = str(resource)
        resource_str = mask_cpr_numbers(resource_str)
        resource_str = mask_names(resource_str)
        resource_str = mask_addresses(resource_str)
        return resource_str


def _mask_dict_recursive(data: Any) -> Any:
    """
    Recursively mask sensitive data in a dictionary or list structure.

    Args:
        data: The data structure to mask

    Returns:
        Masked copy of the data
    """
    if isinstance(data, dict):
        masked: Any = {}
        for key, value in data.items():
            # Mask specific sensitive fields
            if key.lower() in ["name", "given", "family"]:
                # Person names: mask wholesale. family/given are single words that
                # the two-word NAME_PATTERN misses, so regex masking is not enough.
                masked[key] = _mask_person_name(value)
            elif key.lower() in ["text", "display"]:
                # text/display are often clinical code descriptions, not PII;
                # keep the (conservative) regex-based name masking here.
                if isinstance(value, str):
                    masked[key] = mask_names(value)
                elif isinstance(value, list):
                    masked[key] = [_mask_dict_recursive(item) for item in value]
                else:
                    masked[key] = _mask_dict_recursive(value)
            elif key.lower() in ["address", "line", "city", "postalcode", "country"]:
                if isinstance(value, str):
                    masked[key] = mask_addresses(value)
                elif isinstance(value, list):
                    masked[key] = [_mask_dict_recursive(item) for item in value]
                else:
                    masked[key] = _mask_dict_recursive(value)
            elif key.lower() in ["value", "identifier"] and isinstance(value, str):
                # Mask identifiers that might contain CPR numbers
                masked[key] = mask_cpr_numbers(value)
            elif key.lower() == "system" and isinstance(value, str):
                # Keep system URIs as-is, they're not sensitive
                masked[key] = value
            else:
                masked[key] = _mask_dict_recursive(value)
        return masked

    elif isinstance(data, list):
        return [_mask_dict_recursive(item) for item in data]

    elif isinstance(data, str):
        # Apply all masking to string values
        masked = mask_cpr_numbers(data)
        masked = mask_names(masked)
        masked = mask_addresses(masked)
        return masked

    else:
        # For other types (int, float, bool, etc.), return as-is
        return data


def _mask_person_name(value: Any) -> Any:
    """Mask a FHIR person-name field (name/given/family) wholesale.

    family/given values are single words, which the two-word ``NAME_PATTERN``
    would not match; we know these keys carry personal names, so replace the
    whole value. Handles nested HumanName dicts and given-name lists.
    """
    if isinstance(value, str):
        return "***NAME***"
    if isinstance(value, list):
        return [_mask_person_name(item) for item in value]
    if isinstance(value, dict):
        return _mask_dict_recursive(value)
    return value


def mask_identifier_for_logging(identifier: str, identifier_system: str) -> str:
    """
    Mask an identifier value for safe logging based on the identifier system.

    Args:
        identifier: The identifier value to potentially mask
        identifier_system: The identifier system URI

    Returns:
        Masked identifier or original if not sensitive
    """
    # Check if this is a CPR system (Danish personal ID)
    if "cpr" in identifier_system.lower() or "civil" in identifier_system.lower():
        return mask_cpr_numbers(identifier)
    # Check if this is a name-based identifier
    elif "name" in identifier_system.lower():
        return mask_names(identifier)
    else:
        # For other systems, mask CPR numbers just in case
        return mask_cpr_numbers(identifier)
