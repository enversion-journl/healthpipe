# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging for the SHAK dimension from codesystem tables.

Reads codesystem.SHAKConcept and codesystem.SHAKHierarchy to produce the fields
needed for dim.SHAK. The SHAK->SOR cross-reference is not included here — it is
available via conceptmap.SHAKToSOR.

Output fields:
- code: SHAK code
- display: SHAK name (display)
- parent_code: Parent SHAK code from the derived hierarchy
- level: Hierarchy level (1 = root/hospital, computed from traversal depth)
- aktiv: Active status. SHAK concepts come from the current SOR snapshot and are
  treated as active; the validity window is retained as codesystem properties.
"""

import dagster as dg
from core.resources import MSSQLResource


def _build_hierarchy_lookup(cursor) -> dict[str, str]:
    """Build child_code -> parent_code dict from codesystem.SHAKHierarchy."""
    cursor.execute("SELECT child_code, parent_code FROM [codesystem].[SHAKHierarchy]")
    return {str(row[0]).strip(): str(row[1]).strip() for row in cursor.fetchall()}


def _compute_levels(hierarchy_lookup: dict[str, str]) -> dict[str, int]:
    """Compute hierarchy depth for each code (root = 1)."""
    levels: dict[str, int] = {}
    all_codes = set(hierarchy_lookup.keys()) | set(hierarchy_lookup.values())

    for code in all_codes:
        depth = 1
        current = code
        visited: set[str] = set()
        while current in hierarchy_lookup and current not in visited:
            visited.add(current)
            current = hierarchy_lookup[current]
            depth += 1
        levels[code] = depth

    return levels


@dg.asset(
    name="stg_SHAK",
    deps=["cs_shak"],
    group_name="shak_staging",
    description=(
        "SHAK staging from codesystem tables. Reads codesystem.SHAK* to produce "
        "staging entries for the standalone dim.SHAK dimension."
    ),
)
def stg_SHAK(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> list[dict]:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, display FROM [codesystem].[SHAKConcept]")
            concepts = [(str(r[0]).strip(), r[1]) for r in cursor.fetchall()]
            context.log.info(f"Loaded {len(concepts)} concepts from codesystem.SHAKConcept")

            hierarchy_lookup = _build_hierarchy_lookup(cursor)
        finally:
            cursor.close()

    levels = _compute_levels(hierarchy_lookup)

    entries = [
        {
            "code": code,
            "display": display if display else None,
            "parent_code": hierarchy_lookup.get(code),
            "level": levels.get(code, 1),
            "aktiv": True,
        }
        for code, display in concepts
    ]

    context.log.info(f"Built {len(entries)} SHAK staging entries")
    return entries
