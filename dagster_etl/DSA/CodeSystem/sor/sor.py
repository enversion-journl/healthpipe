# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: SOR

Reads SOR register data from ext.sor_register (Region Syddanmark subset),
builds codesystem structure, and MERGEs into persistent tables:
codesystem.sor_concept, _properties, _hierarchy.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables

KNOWN_PROPERTY_KEYS = [
    "entityType",
    "entityTypeId",
    "municipality",
    "region",
    "owner",
    "ownerId",
    "cvrNumber",
    "website",
    "email",
    "phone",
    "address",
    "postalCode",
    "city",
    "openingDate",
    "closingDate",
]

SKIP_COLUMNS = {"Code", "Display", "ParentCode", "DW_Timestamp_Insert", "DW_Timestamp_Update"}


def build_sor_codesystem(
    sor_rows: list,
    sor_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build codesystem structure from ext.sor_register rows.

    Handles two formats:
    1. Wide format (each row has Code, Display, ParentCode, + property columns)
    2. Pre-normalised format (separate concept/property/hierarchy rows already merged
       into the ext table with CODE, KEY, VALUE, TYPE columns)

    Returns:
        (concepts, properties, hierarchy) triple
    """
    sor_data = _rows_to_dicts(sor_rows, sor_columns) if sor_columns else _ensure_dicts(sor_rows)

    if not sor_data:
        return [], [], []

    sample = sor_data[0]
    if "KEY" in sample and "VALUE" in sample:
        return _build_from_normalised(sor_data)
    return _build_from_wide(sor_data)


def _build_from_wide(sor_data: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Build triple from wide-format rows (Code, Display, ParentCode, + property columns)."""
    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    property_columns: list[str] = []
    if sor_data:
        property_columns = [k for k in sor_data[0] if k not in SKIP_COLUMNS]

    for row in sor_data:
        code = str(row.get("Code", "")).strip()
        if not code:
            continue
        display = str(row.get("Display", "")).strip() if row.get("Display") else ""

        concepts.append({"CODE": code, "DISPLAY": display or code})

        for prop_key in property_columns:
            value = row.get(prop_key)
            if value is not None and str(value).strip():
                prop_type = "String"
                if prop_key in ("openingDate", "closingDate"):
                    prop_type = "Date"
                properties.append(
                    {
                        "CODE": code,
                        "KEY": prop_key,
                        "VALUE": str(value).strip(),
                        "TYPE": prop_type,
                    }
                )

        parent_code = str(row.get("ParentCode", "")).strip() if row.get("ParentCode") else ""
        if parent_code:
            hierarchy.append({"CODE": code, "PARENT_CODE": parent_code})

    return concepts, properties, hierarchy


def _build_from_normalised(
    sor_data: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build triple from pre-normalised rows (CODE, KEY, VALUE, TYPE)."""
    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    concept_set: set[str] = set()
    display_lookup: dict[str, str] = {}

    for row in sor_data:
        code = str(row.get("CODE", "")).strip()
        key = str(row.get("KEY", "")).strip()
        value = str(row.get("VALUE", "")).strip() if row.get("VALUE") else ""
        row_type = str(row.get("TYPE", "String")).strip()

        if not code or not key:
            continue

        if key == "display":
            display_lookup[code] = value
            concept_set.add(code)
        elif key == "parentCode":
            if value:
                hierarchy.append({"CODE": code, "PARENT_CODE": value})
            concept_set.add(code)
        else:
            if value:
                properties.append({"CODE": code, "KEY": key, "VALUE": value, "TYPE": row_type})
            concept_set.add(code)

    for code in concept_set:
        display = display_lookup.get(code, code)
        concepts.append({"CODE": code, "DISPLAY": display})

    return concepts, properties, hierarchy


def _rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict]:
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _ensure_dicts(rows: list) -> list[dict]:
    if rows and isinstance(rows[0], dict):
        return rows
    return rows


@dg.asset(
    name="cs_sor",
    deps=["extract_sor_register"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "sor"},
    description=("Build SOR codesystem from ext.sor_register " "and MERGE into codesystem.sor_*"),
)
def cs_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM [ext].[sor_register]")
            sor_rows = cursor.fetchall()
            sor_cols = [col[0] for col in cursor.description]

            context.log.info(f"Read from ext.sor_register: {len(sor_rows)} rows")

            concepts, properties, hierarchy = build_sor_codesystem(sor_rows, sor_cols)

            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            result = merge_codesystem_tables(cursor, "sor", concepts, properties, hierarchy)
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.sor: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u)"
    )

    return dg.MaterializeResult(metadata=result)
