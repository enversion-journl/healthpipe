# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: Rekvirent

Reads Rekvirent source data from ext.* tables (ILS: sgh, rekv, rekvgrp),
builds codesystem hierarchy, and MERGEs into persistent tables:
codesystem.rekvirent_concept, _properties, _hierarchy.

GDPR: HP/OHP rekvirenter get their names masked to '[hjemmepatient]' in the SQL extraction.
The ext.rekv_ils table already contains GDPR-masked names from the source query.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables


def build_rekvirent_hierarchy(
    sgh_rows: list[tuple],
    rekv_rows: list[tuple],
    rekvgrp_rows: list[tuple],
    sgh_columns: list[str] | None = None,
    rekv_columns: list[str] | None = None,
    rekvgrp_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build hierarchy structure for rekvirent codesystem from ext.* table rows.

    Args:
        sgh_rows: Rows from ext.sgh_ils (columns: sgh, navn, ...)
        rekv_rows: Rows from ext.rekv_ils (columns: sgh, rgrp, rtyp, rnr, navn, ...)
        rekvgrp_rows: Rows from ext.rekvgrp_ils (columns: rgrp, navn, ...)
        *_columns: Column name lists from cursor.description (optional, for dict conversion)

    Returns:
        Tuple of (concepts, properties, hierarchy) where each is list[dict]
        - concepts: [{CODE, DISPLAY}, ...]
        - properties: [{CODE, KEY, VALUE, TYPE}, ...]
        - hierarchy: [{CODE, PARENT_CODE}, ...]
    """
    sgh_data = _rows_to_dicts(sgh_rows, sgh_columns) if sgh_columns else _ensure_dicts(sgh_rows)
    rekv_data = (
        _rows_to_dicts(rekv_rows, rekv_columns) if rekv_columns else _ensure_dicts(rekv_rows)
    )
    rekvgrp_data = (
        _rows_to_dicts(rekvgrp_rows, rekvgrp_columns)
        if rekvgrp_columns
        else _ensure_dicts(rekvgrp_rows)
    )

    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    rekvgrp_lookup = {
        str(row["rgrp"]).strip(): str(row["navn"]).strip() if row.get("navn") else ""
        for row in rekvgrp_data
    }

    root_code = "REGION_SYD"
    concepts.append({"CODE": root_code, "DISPLAY": "Region Syddanmark"})
    properties.append({"CODE": root_code, "KEY": "entityType", "VALUE": "region", "TYPE": "String"})

    sgh_codes: dict[str, str] = {}
    for sgh_row in sgh_data:
        sgh = str(sgh_row["sgh"]).strip()
        navn = str(sgh_row["navn"]).strip() if sgh_row.get("navn") else ""
        sgh_code = f"SGH_{sgh}"
        sgh_codes[sgh] = sgh_code

        concepts.append({"CODE": sgh_code, "DISPLAY": navn or f"Sygehus {sgh}"})
        properties.append(
            {"CODE": sgh_code, "KEY": "entityType", "VALUE": "sygehus", "TYPE": "String"}
        )
        properties.append({"CODE": sgh_code, "KEY": "sgh", "VALUE": sgh, "TYPE": "String"})

        if sgh not in ("1000", "9999") and "Region Syddanmark" not in navn:
            hierarchy.append({"CODE": sgh_code, "PARENT_CODE": root_code})

    rgrp_codes: dict[tuple[str, str], str] = {}
    for rekv_row in rekv_data:
        sgh = str(rekv_row["sgh"]).strip()
        rgrp = str(rekv_row["rgrp"]).strip() if rekv_row.get("rgrp") else None
        if not rgrp:
            continue
        parent_sgh_code = sgh_codes.get(sgh)
        if not parent_sgh_code:
            continue
        rgrp_key = (sgh, rgrp)
        if rgrp_key not in rgrp_codes:
            rgrp_code = f"RGRP_{sgh}_{rgrp}"
            rgrp_codes[rgrp_key] = rgrp_code
            rgrp_display = rekvgrp_lookup.get(rgrp, f"Rekvirentgruppe {rgrp}")
            concepts.append({"CODE": rgrp_code, "DISPLAY": rgrp_display})
            properties.append(
                {
                    "CODE": rgrp_code,
                    "KEY": "entityType",
                    "VALUE": "rekvirentgruppe",
                    "TYPE": "String",
                }
            )
            properties.append({"CODE": rgrp_code, "KEY": "sgh", "VALUE": sgh, "TYPE": "String"})
            properties.append({"CODE": rgrp_code, "KEY": "rgrp", "VALUE": rgrp, "TYPE": "String"})
            hierarchy.append({"CODE": rgrp_code, "PARENT_CODE": parent_sgh_code})

    rtyp_codes: dict[tuple[str, str, str], str] = {}
    for rekv_row in rekv_data:
        sgh = str(rekv_row["sgh"]).strip()
        rgrp = str(rekv_row["rgrp"]).strip() if rekv_row.get("rgrp") else None
        rtyp = str(rekv_row["rtyp"]).strip() if rekv_row.get("rtyp") else None
        if not rgrp or not rtyp:
            continue
        rgrp_code = rgrp_codes.get((sgh, rgrp))
        if not rgrp_code:
            continue
        rtyp_key = (sgh, rgrp, rtyp)
        if rtyp_key not in rtyp_codes:
            rtyp_code = f"RTYP_{sgh}_{rgrp}_{rtyp}"
            rtyp_codes[rtyp_key] = rtyp_code
            rtyp_display = f"Rekvirenttype {rtyp}"
            concepts.append({"CODE": rtyp_code, "DISPLAY": rtyp_display})
            properties.append(
                {
                    "CODE": rtyp_code,
                    "KEY": "entityType",
                    "VALUE": "rekvirenttype",
                    "TYPE": "String",
                }
            )
            properties.append({"CODE": rtyp_code, "KEY": "sgh", "VALUE": sgh, "TYPE": "String"})
            properties.append({"CODE": rtyp_code, "KEY": "rgrp", "VALUE": rgrp, "TYPE": "String"})
            properties.append({"CODE": rtyp_code, "KEY": "rtyp", "VALUE": rtyp, "TYPE": "String"})
            hierarchy.append({"CODE": rtyp_code, "PARENT_CODE": rgrp_code})

    for rekv_row in rekv_data:
        sgh = str(rekv_row["sgh"]).strip()
        rgrp = str(rekv_row["rgrp"]).strip() if rekv_row.get("rgrp") else None
        rtyp = str(rekv_row["rtyp"]).strip() if rekv_row.get("rtyp") else None
        rnr = str(rekv_row["rnr"]).strip() if rekv_row.get("rnr") else None
        navn = str(rekv_row["navn"]).strip() if rekv_row.get("navn") else ""
        if not rgrp or not rnr:
            continue
        rnr_code = f"RNR_{sgh}_{rnr}"
        display = navn or f"Rekvirent {rnr}"
        if rtyp:
            parent_code = rtyp_codes.get((sgh, rgrp, rtyp))
        else:
            parent_code = rgrp_codes.get((sgh, rgrp))
        if not parent_code:
            continue
        concepts.append({"CODE": rnr_code, "DISPLAY": display})
        properties.append(
            {"CODE": rnr_code, "KEY": "entityType", "VALUE": "rekvirent", "TYPE": "String"}
        )
        properties.append({"CODE": rnr_code, "KEY": "sgh", "VALUE": sgh, "TYPE": "String"})
        properties.append({"CODE": rnr_code, "KEY": "rgrp", "VALUE": rgrp, "TYPE": "String"})
        if rtyp:
            properties.append({"CODE": rnr_code, "KEY": "rtyp", "VALUE": rtyp, "TYPE": "String"})
        properties.append({"CODE": rnr_code, "KEY": "rnr", "VALUE": rnr, "TYPE": "String"})
        properties.append(
            {"CODE": rnr_code, "KEY": "mapCode", "VALUE": f"{sgh}-{rnr}", "TYPE": "String"}
        )
        hierarchy.append({"CODE": rnr_code, "PARENT_CODE": parent_code})

    return concepts, properties, hierarchy


def _rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict]:
    """Convert cursor.fetchall() tuples to list of dicts using column names."""
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _ensure_dicts(rows: list) -> list[dict]:
    """Ensure rows are dicts (pass through if already dicts from as_dict cursor)."""
    if rows and isinstance(rows[0], dict):
        return rows
    return rows


@dg.asset(
    name="cs_rekvirent",
    deps=["ext_sgh_ils", "ext_rekv_ils", "ext_rekvgrp_ils"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "rekvirent"},
    description=(
        "Build Rekvirent codesystem from ext.* tables " "and MERGE into codesystem.rekvirent_*"
    ),
)
def cs_rekvirent(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM [ext].[sgh_ils]")
            sgh_rows = cursor.fetchall()
            sgh_cols = [col[0] for col in cursor.description]

            cursor.execute("SELECT * FROM [ext].[rekv_ils]")
            rekv_rows = cursor.fetchall()
            rekv_cols = [col[0] for col in cursor.description]

            cursor.execute("SELECT * FROM [ext].[rekvgrp_ils]")
            rekvgrp_rows = cursor.fetchall()
            rekvgrp_cols = [col[0] for col in cursor.description]

            context.log.info(
                f"Read from ext.*: {len(sgh_rows)} sgh, {len(rekv_rows)} rekv, "
                f"{len(rekvgrp_rows)} rekvgrp"
            )

            concepts, properties, hierarchy = build_rekvirent_hierarchy(
                sgh_rows,
                rekv_rows,
                rekvgrp_rows,
                sgh_cols,
                rekv_cols,
                rekvgrp_cols,
            )

            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            result = merge_codesystem_tables(cursor, "rekvirent", concepts, properties, hierarchy)
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.rekvirent: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u)"
    )

    return dg.MaterializeResult(metadata=result)
