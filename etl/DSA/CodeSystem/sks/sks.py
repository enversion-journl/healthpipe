# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: SKS

Reads SKS register data from ext.sks_register,
builds codesystem structure, and MERGEs into persistent tables:
codesystem.sks_concept, _properties, _hierarchy.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables


def build_sks_codesystem(
    sks_rows: list,
    sks_columns: list[str],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build codesystem structure from ext.sks_register rows.

    ext.sks_register has columns: Code, display, ParentCode, classification,
    Inactive. A code may appear in several rows (one per classification) —
    each yields a classification property, so a concept can carry multiple.
    Codes with Inactive='true' get the standard FHIR `inactive` property.

    Returns: (concepts, properties, hierarchy) triple
    """
    sks_data = [dict(zip(sks_columns, row, strict=False)) for row in sks_rows]

    if not sks_data:
        return [], [], []

    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    for row in sks_data:
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        display = str(row.get("display", "")).strip() if row.get("display") else ""

        concepts.append({"CODE": code, "DISPLAY": display or code})

        classification = (
            str(row.get("classification", "")).strip() if row.get("classification") else ""
        )
        if classification:
            properties.append(
                {
                    "CODE": code,
                    "KEY": "classification",
                    "VALUE": classification,
                    "TYPE": "String",
                }
            )

        inactive = str(row.get("Inactive", "")).strip().lower() if row.get("Inactive") else ""
        if inactive == "true":
            properties.append(
                {
                    "CODE": code,
                    "KEY": "inactive",
                    "VALUE": "true",
                    "TYPE": "Boolean",
                }
            )

        parent_code = str(row.get("parent_code", "")).strip() if row.get("parent_code") else ""
        if parent_code:
            hierarchy.append({"CODE": code, "PARENT_CODE": parent_code})

    return concepts, properties, hierarchy


@dg.asset(
    name="cs_sks",
    deps=["extract_sks_register", "staging_infra_codesystem"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "sks"},
    description="Build SKS codesystem from ext.sks_register and MERGE into codesystem.sks_*",
)
def cs_sks(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM [ext].[sks_register]")
            sks_rows = cursor.fetchall()
            sks_cols = [col[0] for col in cursor.description]

            context.log.info(f"Read from ext.sks_register: {len(sks_rows)} rows")

            concepts, properties, hierarchy = build_sks_codesystem(sks_rows, sks_cols)

            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            result = merge_codesystem_tables(
                cursor,
                "sks",
                concepts,
                properties,
                hierarchy,
                multi_value_properties=True,
                delete_stale=True,
            )
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.sks: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u/{result.get('concepts_deleted', 0)}d), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u/{result.get('properties_deleted', 0)}d), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u/{result.get('hierarchy_deleted', 0)}d)"
    )

    return dg.MaterializeResult(metadata=result)
