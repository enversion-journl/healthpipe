# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: ean

Reads ean (GLN) source data from ext.ean_nemhandel,
builds flat codesystem, and MERGEs into persistent tables:
codesystem.EANConcept, EANProperties, EANHierarchy.

The merge only upserts: nothing is deleted, so a partial NemHandel response
cannot remove the properties of the GLNs it is missing. Rows are only touched
when they change, so dw_timestamp_update is a real change signal — the OS2mo
EAN delivery reads participantName's as a renamed GLN's "last changed".
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables


def build_ean_codesystem(
    ean_rows: list,
    ean_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build codesystem structure from ext.ean_nemhandel rows.

    Flat hierarchy (no parent codes). Each GLN entry becomes a concept
    with participantName and participantOwnerId as properties, always both
    (NULL when empty).

    Returns:
        (concepts, properties, hierarchy) triple
    """
    ean_data = _rows_to_dicts(ean_rows, ean_columns) if ean_columns else _ensure_dicts(ean_rows)

    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    for p in ean_data:
        code = str(p.get("participantID", "")).strip()
        if not code:
            continue
        name = str(p.get("participantName", "")).strip() if p.get("participantName") else ""
        owner_id = (
            str(p.get("participantOwnerId", "")).strip() if p.get("participantOwnerId") else ""
        )

        display = name if name else f"ean {code}"
        concepts.append({"CODE": code, "DISPLAY": display})

        # Both properties are always written — NULL when NemHandel has no
        # value — so a name NemHandel itself clears is cleared here too.
        properties.append(
            {"CODE": code, "KEY": "participantName", "VALUE": name or None, "TYPE": "String"}
        )
        properties.append(
            {"CODE": code, "KEY": "participantOwnerId", "VALUE": owner_id or None, "TYPE": "String"}
        )

        hierarchy.append({"CODE": code, "PARENT_CODE": ""})

    return concepts, properties, hierarchy


def _rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict]:
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _ensure_dicts(rows: list) -> list[dict]:
    if rows and isinstance(rows[0], dict):
        return rows
    return rows


@dg.asset(
    name="cs_ean",
    deps=["extract_ean_nemhandel"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "ean"},
    description="Build ean codesystem from ext.ean_nemhandel and MERGE into codesystem.EAN*",
)
def cs_ean(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM [ext].[ean_nemhandel]")
            ean_rows = cursor.fetchall()
            ean_cols = [col[0] for col in cursor.description]

            context.log.info(f"Read from ext.ean_nemhandel: {len(ean_rows)} rows")

            concepts, properties, hierarchy = build_ean_codesystem(ean_rows, ean_cols)

            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            result = merge_codesystem_tables(
                cursor,
                "ean",
                concepts,
                properties,
                hierarchy,
                update_only_on_change=True,
            )
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.ean: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u)"
    )

    return dg.MaterializeResult(metadata=result)
