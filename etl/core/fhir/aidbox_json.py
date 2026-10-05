# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Aidbox JSON compatibility helpers."""

from typing import Any


def transform_references_for_aidbox(resource_json: dict[str, Any]) -> None:
    """
    Transform FHIR references from {id, type} to {id, resourceType} format.

    The fhir.resources library serializes Reference.type as 'type', but Aidbox
    expects 'resourceType' for proper UI navigation. This function recursively
    transforms all references in the resource JSON.

    Args:
        resource_json: The serialized FHIR resource as a dict (modified in place)
    """
    if not isinstance(resource_json, dict):
        return

    # Check if this dict is a reference with {id, type} format
    if "type" in resource_json and "id" in resource_json and "resourceType" not in resource_json:
        # Check if it looks like a reference (has type as a string resource name)
        type_value = resource_json.get("type")
        if isinstance(type_value, str) and type_value[0].isupper():
            # Transform type -> resourceType
            resource_json["resourceType"] = resource_json.pop("type")

    # Recursively process all values
    for _key, value in list(resource_json.items()):
        if isinstance(value, dict):
            transform_references_for_aidbox(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    transform_references_for_aidbox(item)
