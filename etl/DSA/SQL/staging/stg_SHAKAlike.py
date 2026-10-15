# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging for the SHAK-a-like dimension from codesystem tables.

Reads codesystem.SHAKAlikeConcept and codesystem.SHAKAlikeHierarchy.
The SHAK-a-like -> SOR cross-reference lives in conceptmap.SHAKAlikeToSOR.

`aktiv` is read directly from arc.epji_location.dw_is_deleted rather
than from codesystem.SHAKAlikeConcept: cs_shakalike UPSERTs concepts and
deliberately never deletes them (so map/dim references survive), so the
concept table alone can't tell a live location from one EPJI has removed.
The archive is the only layer that still carries that signal.
"""

import dagster as dg
from core.resources import MSSQLResource


def _build_hierarchy_lookup(cursor) -> dict[str, str]:
    cursor.execute("SELECT child_code, parent_code FROM [codesystem].[SHAKAlikeHierarchy]")
    return {str(row[0]).strip(): str(row[1]).strip() for row in cursor.fetchall()}


def _build_deletion_lookup(cursor) -> dict[str, bool]:
    cursor.execute("SELECT code, dw_is_deleted FROM [arc].[epji_location]")
    return {str(row[0]).strip(): bool(row[1]) for row in cursor.fetchall()}


def _compute_levels(hierarchy_lookup: dict[str, str]) -> dict[str, int]:
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
    name="stg_SHAKAlike",
    deps=["cs_shakalike", "arc_epji_shakalike"],
    group_name="shakalike_staging",
    description=("SHAK-a-like staging from codesystem.SHAKAlike* for dim.SHAKAlike."),
)
def stg_SHAKAlike(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> list[dict]:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, display FROM [codesystem].[SHAKAlikeConcept]")
            concepts = [(str(r[0]).strip(), r[1]) for r in cursor.fetchall()]
            context.log.info(f"Loaded {len(concepts)} concepts from codesystem.SHAKAlikeConcept")
            hierarchy_lookup = _build_hierarchy_lookup(cursor)
            deletion_lookup = _build_deletion_lookup(cursor)
        finally:
            cursor.close()

    levels = _compute_levels(hierarchy_lookup)
    entries = [
        {
            "code": code,
            "display": display if display else None,
            "parent_code": hierarchy_lookup.get(code),
            "level": levels.get(code, 1),
            "aktiv": not deletion_lookup.get(code, False),
        }
        for code, display in concepts
    ]
    context.log.info(f"Built {len(entries)} SHAK-a-like staging entries")
    return entries
