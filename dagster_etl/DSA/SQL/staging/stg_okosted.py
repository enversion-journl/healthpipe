# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging for Okosted (Omkostningssted) dimension data from codesystem tables.

Reads from codesystem.okosted_concept, _properties, _hierarchy to produce
the fields needed for dim.Okosted. No SOR mapping columns -- the cross-reference
is available via conceptmap.okosted_to_sor if needed.

Output fields:
- code: Omkostningsstednummer (OKOSTED code)
- display: Omkostningsstedtekst
- parent_code: Parent OKOSTED code from hierarchy
- level: Hierarchy level (computed from hierarchy traversal depth, root=1)
- aktiv: Active status from codesystem properties
"""

import dagster as dg
from core.resources import MSSQLResource


def _build_hierarchy_lookup(cursor) -> dict[str, str]:
    """Build child_code -> parent_code dict from codesystem.okosted_hierarchy."""
    cursor.execute("SELECT ChildCode, ParentCode FROM [codesystem].[OkostedHierarchy]")
    return {str(row[0]).strip(): str(row[1]).strip() for row in cursor.fetchall()}


def _compute_levels(hierarchy_lookup: dict[str, str]) -> dict[str, int]:
    """Compute hierarchy depth for each code (root=1)."""
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


def _parse_aktiv(value: str | None) -> bool:
    """Parse the 'aktiv' property value to a bool. Missing/empty defaults to True (active)."""
    return value.lower() == "true" if value else True


def _build_properties_lookup(cursor) -> dict[str, dict[str, str]]:
    """Build code -> {propertyCode: value} dict from codesystem.okosted_properties."""
    cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[OkostedProperties]")
    lookup: dict[str, dict[str, str]] = {}
    for row in cursor.fetchall():
        code = str(row[0]).strip()
        key = str(row[1]).strip()
        value = str(row[2]).strip() if row[2] else ""
        lookup.setdefault(code, {})[key] = value
    return lookup


@dg.asset(
    name="stg_okosted",
    deps=["cs_okosted"],
    group_name="okosted_staging",
    description=(
        "Okosted staging from codesystem tables. Reads codesystem.okosted_* "
        "to produce staging entries for the standalone dim.Okosted dimension."
    ),
)
def stg_okosted(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> list[dict]:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, Display FROM [codesystem].[OkostedConcept]")
            concepts = [(str(r[0]).strip(), r[1]) for r in cursor.fetchall()]
            context.log.info(f"Loaded {len(concepts)} concepts from codesystem.okosted_concept")

            hierarchy_lookup = _build_hierarchy_lookup(cursor)
            properties_lookup = _build_properties_lookup(cursor)
        finally:
            cursor.close()

    levels = _compute_levels(hierarchy_lookup)

    entries = []
    for code, display in concepts:
        display_val = display if display else None

        aktiv = _parse_aktiv(properties_lookup.get(code, {}).get("aktiv"))

        parent_code = hierarchy_lookup.get(code)

        entries.append(
            {
                "code": code,
                "display": display_val,
                "parent_code": parent_code,
                "level": levels.get(code, 1),
                "aktiv": aktiv,
            }
        )

    aktiv_count = sum(1 for e in entries if e["aktiv"])
    context.log.info(
        f"Built {len(entries)} Okosted staging entries "
        f"({aktiv_count} active, {len(entries) - aktiv_count} inactive)"
    )

    return entries
