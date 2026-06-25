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

# Danish CPR number pattern: 6 digits, dash, 4 digits (DDMMYY-XXXX)
CPR_PATTERN = re.compile(r"\b\d{6}-\d{4}\b")

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
        masked = {}
        for key, value in data.items():
            # Mask specific sensitive fields
            if key.lower() in ["name", "given", "family", "text", "display"]:
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


def create_masked_log_message(message_template: str, *args, **kwargs) -> str:
    """
    Create a log message with sensitive data masked.

    Args:
        message_template: String template for the message
        *args: Positional arguments to format into template
        **kwargs: Keyword arguments to format into template

    Returns:
        Formatted message with sensitive data masked
    """
    # First format the message
    if args:
        message = message_template % args
    else:
        message = message_template

    # Then mask any sensitive data in the result
    message = mask_cpr_numbers(message)
    message = mask_names(message)
    message = mask_addresses(message)

    return message
