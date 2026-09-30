# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: Lager

Reads Lager source data from the ILS DFM archive (arc.ils_sgh, arc.ils_lgr),
builds codesystem hierarchy, and MERGEs into persistent tables:
codesystem.lager_concept, _properties, _hierarchy.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables


def _safe_strip(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _add_property(properties: list[dict], code: str, key: str, value: str, ptype: str = "String"):
    properties.append({"CODE": code, "KEY": key, "VALUE": value, "TYPE": ptype})


def build_lager_hierarchy(
    sgh_rows: list,
    lgr_rows: list,
    sgh_columns: list[str] | None = None,
    lgr_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build hierarchy structure for lager codesystem from arc.ils_* table rows.

    Hierarchy: REGION_SYD -> SGH_{sgh} -> SGH_{sgh}_LGR_{lgr}

    Returns:
        (concepts, properties, hierarchy) triple
    """
    sgh_data = _rows_to_dicts(sgh_rows, sgh_columns) if sgh_columns else _ensure_dicts(sgh_rows)
    lgr_data = _rows_to_dicts(lgr_rows, lgr_columns) if lgr_columns else _ensure_dicts(lgr_rows)

    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    root_code = "REGION_SYD"
    concepts.append({"CODE": root_code, "DISPLAY": "Region Syddanmark"})
    _add_property(properties, root_code, "entityType", "region")

    sgh_codes: dict[str, str] = {}
    for sgh_row in sgh_data:
        sgh = _safe_strip(sgh_row.get("sgh"))
        navn = _safe_strip(sgh_row.get("navn"))
        if not sgh:
            continue
        sgh_code = f"SGH_{sgh}"
        sgh_codes[sgh] = sgh_code
        concepts.append({"CODE": sgh_code, "DISPLAY": navn or f"Sygehus {sgh}"})
        _add_property(properties, sgh_code, "entityType", "sygehus")
        _add_property(properties, sgh_code, "sgh", sgh)
        hierarchy.append({"CODE": sgh_code, "PARENT_CODE": root_code})

    seen_lgr: set[str] = set()
    for lgr_row in lgr_data:
        sgh = _safe_strip(lgr_row.get("sgh"))
        lgr = _safe_strip(lgr_row.get("lgr"))
        navn = _safe_strip(lgr_row.get("navn"))
        if not sgh or not lgr:
            continue
        lgr_code = f"SGH_{sgh}_LGR_{lgr}"
        if lgr_code in seen_lgr:
            continue
        seen_lgr.add(lgr_code)
        parent = sgh_codes.get(sgh)
        if not parent:
            continue
        concepts.append({"CODE": lgr_code, "DISPLAY": navn or f"Lager {lgr}"})
        _add_property(properties, lgr_code, "entityType", "lager")
        _add_property(properties, lgr_code, "sgh", sgh)
        _add_property(properties, lgr_code, "lgr", lgr)
        _add_property(properties, lgr_code, "mapCode", f"{sgh}-{lgr}")
        hierarchy.append({"CODE": lgr_code, "PARENT_CODE": parent})

    return concepts, properties, hierarchy


def _rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict]:
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _ensure_dicts(rows: list) -> list[dict]:
    if rows and isinstance(rows[0], dict):
        return rows
    return rows


@dg.asset(
    name="cs_lager",
    deps=["arc_ils_sgh", "arc_ils_lgr"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "lager"},
    description=(
        "Build Lager codesystem from arc.ils_* archive " "and MERGE into codesystem.lager_*"
    ),
)
def cs_lager(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM [arc].[ils_sgh]")
            sgh_rows = cursor.fetchall()
            sgh_cols = [col[0] for col in cursor.description]

            cursor.execute("SELECT * FROM [arc].[ils_lgr]")
            lgr_rows = cursor.fetchall()
            lgr_cols = [col[0] for col in cursor.description]

            context.log.info(f"Read from arc.ils_*: {len(sgh_rows)} sgh, {len(lgr_rows)} lgr")

            concepts, properties, hierarchy = build_lager_hierarchy(
                sgh_rows, lgr_rows, sgh_cols, lgr_cols
            )

            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            result = merge_codesystem_tables(cursor, "lager", concepts, properties, hierarchy)
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.lager: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u)"
    )

    return dg.MaterializeResult(metadata=result)
