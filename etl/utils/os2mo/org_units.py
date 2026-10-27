# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The read of OS2mo's org units — used by the return leg (``DSI/os2mo``).

Kept apart from the asset so the GraphQL document is readable in one place and
testable without a server. Nothing here performs I/O.
"""

from typing import Any

__all__ = [
    "ORG_UNITS_PAGE_QUERY",
    "ORG_UNITS_PAGE_SIZE",
    "extract_org_units_page",
]

# How many org units to pull per page.
ORG_UNITS_PAGE_SIZE = 500

# The READ type calls the unit type `unit_type`, and the flat `parent` /
# `unit_type` fields are deprecated ("will be removed in a future version of
# OS2mo"), so the `*_response` wrappers are what this asks for.
#
# The null date filter reads every unit that has EVER existed, not only the
# ones valid today. A closed unit has no `current`, so without `validities` it
# would not be read at all — and the archive would soft-delete it instead of
# recording that it closed.
_ORG_UNIT_FIELDS = """
        uuid
        user_key
        name
        validity { from to }
        parent_response { uuid current { user_key } }
        unit_type_response { uuid current { user_key name } }
"""

ORG_UNITS_PAGE_QUERY = (
    """
query Os2moOrgUnitsPage($limit: int!, $cursor: Cursor) {
  org_units(limit: $limit, cursor: $cursor, filter: { from_date: null, to_date: null }) {
    objects {
      current {"""
    + _ORG_UNIT_FIELDS
    + """      }
      validities {"""
    + _ORG_UNIT_FIELDS
    + """      }
    }
    page_info { next_cursor }
  }
}
"""
)


def extract_org_units_page(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    """Read one page of org units plus the cursor for the next one.

    One record per unit: its ``current`` version when it has one, otherwise its
    latest validity. Shaped for
    ``OS2moClient.paginate``'s ``extract`` callback.
    """
    objects = ((payload or {}).get("org_units") or {}).get("objects") or []
    units: list[dict[str, Any]] = []
    for obj in objects:
        if not obj:
            continue
        if obj.get("current"):
            units.append(obj["current"])
            continue
        validities = [v for v in obj.get("validities") or [] if v]
        if validities:
            latest = max(validities, key=lambda v: str((v.get("validity") or {}).get("from") or ""))
            units.append(latest)
    page_info = ((payload or {}).get("org_units") or {}).get("page_info") or {}
    return units, page_info.get("next_cursor")
