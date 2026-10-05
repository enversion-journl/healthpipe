# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: SHAK (Sygehus-Afdelingsklassifikation)

Reads the SHAK master data from ext.shaksor_map (built by extract_shak_register
from the official SHAKSORMap.csv) and MERGEs into the persistent tables
codesystem.SHAKConcept, _Properties, _Hierarchy.

Hierarchy is derived from the codes themselves — SHAK codes encode an is-a
hierarchy by length/prefix:
  * 4-digit codes are hospitals (sygehuse) and form the roots.
  * Longer numeric codes hang under the longest shorter code that is a prefix
    (e.g. 1355010 -> 135501 -> 1355).
  * Codes with a trailing alphabetic suffix (e.g. 665033G) hang under their
    numeric base (665033) when that base exists.
Only edges whose parent also exists as a concept are emitted, so orphaned codes
become additional roots rather than dangling references.

CONCEPTS are UPSERTed (stale codes kept so map.SHAK/dim.SHAK references stay
intact), while PROPERTIES and HIERARCHY are refreshed snapshot-style — a changed
property value or parent edge in a later SHAKSORMap snapshot replaces the old
row instead of accumulating next to it.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables
from DSA.FHIR.utils.code_normalization import extract_shak_parent_from_suffix

# ext.shaksor_map columns that are NOT concept properties.
_NON_PROPERTY_COLUMNS = {
    "code",
    "display",
    "SorCode",
    "SorDisplay",
    "isDirect",
    "dw_timestamp_insert",
    "dw_timestamp_update",
}
_DATE_PROPERTY_KEYS = {"validFrom", "validTo"}


def _numeric_base(code: str) -> str:
    """Return the leading numeric run of a code (``665033G`` -> ``665033``)."""
    digits: list[str] = []
    for ch in code:
        if ch.isdigit():
            digits.append(ch)
        else:
            break
    return "".join(digits)


def derive_shak_hierarchy(codes: set[str]) -> list[dict]:
    """Derive parent/child edges from the SHAK code set.

    For each code: prefer the numeric base of a suffixed code (when present),
    otherwise the longest strictly-shorter existing code that prefixes the
    numeric base (down to the 4-digit hospital level). Returns a list of
    ``{"CODE": child, "PARENT_CODE": parent}`` for codes whose parent exists.
    """
    hierarchy: list[dict] = []
    for code in codes:
        parent = extract_shak_parent_from_suffix(code)
        if parent and parent in codes:
            hierarchy.append({"CODE": code, "PARENT_CODE": parent})
            continue

        base = _numeric_base(code)
        for length in range(len(base) - 1, 3, -1):
            candidate = base[:length]
            if candidate in codes and candidate != code:
                hierarchy.append({"CODE": code, "PARENT_CODE": candidate})
                break
    return hierarchy


def build_shak_codesystem(
    shak_rows: list,
    shak_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build the (concepts, properties, hierarchy) triple from ext.shaksor_map rows."""
    if shak_columns is not None:
        shak_data = [dict(zip(shak_columns, row, strict=False)) for row in shak_rows]
    else:
        shak_data = list(shak_rows)

    property_columns = [
        col
        for col in (shak_columns or (shak_data[0].keys() if shak_data else []))
        if col not in _NON_PROPERTY_COLUMNS
    ]

    concepts: list[dict] = []
    properties: list[dict] = []
    codes: set[str] = set()

    for row in shak_data:
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        codes.add(code)

        display = str(row.get("display", "")).strip() if row.get("display") else ""
        concepts.append({"CODE": code, "DISPLAY": display or code})

        for key in property_columns:
            value = row.get(key)
            if value is None or not str(value).strip():
                continue
            prop_type = "Date" if key in _DATE_PROPERTY_KEYS else "String"
            properties.append(
                {"CODE": code, "KEY": key, "VALUE": str(value).strip(), "TYPE": prop_type}
            )

    hierarchy = derive_shak_hierarchy(codes)
    return concepts, properties, hierarchy


@dg.asset(
    name="cs_shak",
    deps=["extract_shak_register", "staging_infra_codesystem"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "shak"},
    description=(
        "Build the SHAK CodeSystem from ext.shaksor_map (concepts + SHAK master "
        "properties; hierarchy derived from the codes). Concepts are UPSERTed "
        "(stale codes kept); properties and hierarchy are refreshed snapshot-style."
    ),
)
def cs_shak(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM [ext].[shaksor_map]")
            shak_rows = cursor.fetchall()
            shak_cols = [col[0] for col in cursor.description]

            context.log.info(f"Read {len(shak_rows)} rows from ext.shaksor_map")

            concepts, properties, hierarchy = build_shak_codesystem(shak_rows, shak_cols)
            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            result = merge_codesystem_tables(
                cursor,
                "shak",
                concepts,
                properties,
                hierarchy,
                delete_stale_properties=True,
                delete_stale_hierarchy=True,
            )
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.shak: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u)"
    )
    return dg.MaterializeResult(metadata=result)
