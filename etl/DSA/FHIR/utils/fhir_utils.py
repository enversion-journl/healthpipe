# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from datetime import UTC, datetime

from fhir.resources.R4B.fhirtypes import Date, DateTime
from fhir.resources.resource import Resource


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
