# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from datetime import UTC, datetime

from fhir.resources.R4B.fhirtypes import Date, DateTime, Time
from fhir.resources.resource import Resource

SOURCE_TAG_SYSTEM = "urn:enversion:source-system"
SOURCE_EXTENSION_URL = "urn:enversion:source"


def decorate_with_source(
    resource: Resource,
    source_name: str,
    updated_at: datetime | None = None,
) -> Resource:
    """
    Return a copy of the given FHIR resource decorated with source metadata.

    - Adds a Coding tag to meta.tag with system SOURCE_TAG_SYSTEM and code=source_name
    - Appends an Extension at meta.extension with url SOURCE_EXTENSION_URL containing
      nested extensions `source` (valueString) and `updated` (valueDateTime)
    """
    payload = resource.dict()

    meta = payload.get("meta") or {}

    # Add/merge tag
    tags = meta.get("tag") or []
    if not any(
        (t.get("system") == SOURCE_TAG_SYSTEM and t.get("code") == source_name) for t in tags
    ):
        tags.append(
            {
                "system": SOURCE_TAG_SYSTEM,
                "code": source_name,
                "display": source_name,
            }
        )
    meta["tag"] = tags

    # Compute updated timestamp as FHIR dateTime
    when_dt = updated_at or datetime.now(UTC)
    when = DateTime.validate(when_dt.isoformat())

    # Keep tag in meta
    payload["meta"] = meta

    # Add top-level extensions (avoid meta.extension to sidestep strict validators)
    root_ext = payload.get("extension") or []
    root_ext.append(
        {
            "url": SOURCE_EXTENSION_URL,
            "extension": [
                {"url": "source", "valueString": source_name},
                {"url": "updated", "valueDateTime": when},
            ],
        }
    )
    payload["extension"] = root_ext

    # Reconstruct an instance of the same resource class
    return resource.__class__(**payload)


# FHIR scalar helpers (R4B)
def fhir_datetime(value: datetime | str) -> str:
    """
    Coerce Python datetime or ISO8601 string to FHIR-compliant dateTime string.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return DateTime.validate(value.isoformat())
    return DateTime.validate(str(value))


def fhir_date(value: datetime | str) -> str:
    """
    Coerce to FHIR date string (YYYY-MM-DD). Accepts datetime or string.
    """
    if isinstance(value, datetime):
        return Date.validate(value.date().isoformat())
    return Date.validate(str(value))


def fhir_time(value: datetime | str) -> str:
    """
    Coerce to FHIR time string (HH:MM:SS[.sss]). Accepts datetime or string.
    """
    if isinstance(value, datetime):
        return Time.validate(value.time().isoformat())
    return Time.validate(str(value))


def find_identifier(resource: Resource, system_uri: str) -> str | None:
    """
    Finds the value of the first identifier with a matching system URI on a FHIR resource.
    """
    if not hasattr(resource, "identifier") or not resource.identifier:
        return None
    for identifier in resource.identifier:
        if identifier.system == system_uri:
            return identifier.value
    return None
