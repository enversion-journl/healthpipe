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

from datetime import datetime

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables


def _safe_strip(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        # pyodbc surfaces BIT as bool. Normalise to '1'/'0' so False is kept as a
        # stated value (str(False) is truthy text) and casts cleanly downstream.
        return "1" if value else "0"
    return str(value).strip()


def _add_property(properties: list[dict], code: str, key: str, value: str, ptype: str = "String"):
    properties.append({"CODE": code, "KEY": key, "VALUE": value, "TYPE": ptype})


def _add_optional_property(
    properties: list[dict], code: str, key: str, value, ptype: str = "String"
):
    """Write the property only when the source actually states a value.

    codesystem.LagerProperties syncs to DSO, so a blank row per warehouse per
    optional column is pure noise — and `_get_prop` in stg_dim_Lager maps '' to
    None anyway, so an empty row carries no information the absent row does not.
    """
    stripped = _safe_strip(value)
    if stripped:
        _add_property(properties, code, key, stripped, ptype)


# Warehouse attributes from dbo.lgr -> property key on the warehouse concept.
#
# These MUST stay in sync with PROPERTY_COLUMN_MAP in stg_dim_Lager: a key that
# is pivoted without being written here yields a column that is always NULL. That
# was exactly what happened to adr/land/postdi/tlf, which were declared in
# codesystem.json and pivoted in staging but never written — now caught by the
# property parity test in etl/tests/unit/test_dim_lager_lagerejer.py.
#
# The keys are camelCase business names (like entityType/mapCode), not the ILS
# abbreviations; the translation lives here and in data_lineage/dim/dim_lager.yml.
_LGR_ATTRIBUTE_PROPERTIES: tuple[tuple[str, str, str], ...] = (
    # (source column in dbo.lgr, property key, FHIR property type)
    #
    # The address fields (adr/land/postdi/tlf) are deliberately NOT here: address
    # details are populated outside the CodeSystem and joined into stg_dim_Lager
    # from arc.ils_lgr — see DSA/SQL/common/lager_adresse.py for why.
    #
    # Requisitioners. Strategisk Indkoeb: lgrejer is "a requisitioner used for goods
    # movements into the warehouse"; rekvsvind "a requisitioner used to record
    # shrinkage". dim_Lager resolves both into dw_ek_Rekvirent_* keys.
    ("lgrejer", "lagerejerRekvirentnummer", "String"),
    ("rekvsvind", "svindRekvirentnummer", "String"),
    # "a default delivery time for the warehouse" — an integer, but carried as
    # text in the property table (value is NVARCHAR) and cast in dim_Lager.
    ("deflevdage", "standardLeveringsdage", "Integer"),
    # The source's own active flag on the warehouse ('1'/'0', see _safe_strip).
    # Needed to reproduce the "active warehouses only" filter used in reporting.
    ("aktiv", "aktiv", "Boolean"),
)


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

    # The dedup below keeps the FIRST row seen per (sgh, lgr), so order the rows
    # by the same rule build_adresse_update_sql applies in SQL (pro2modified
    # DESC, prrowid ASC, NULLs last) — otherwise display/properties and the
    # address enrichment could come from two different source rows on duplicates.
    lgr_data = sorted(lgr_data, key=lambda row: _safe_strip(row.get("prrowid")))
    lgr_data = sorted(
        lgr_data, key=lambda row: row.get("pro2modified") or datetime.min, reverse=True
    )

    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    root_code = "REGION_SYD"
    concepts.append({"CODE": root_code, "DISPLAY": "Example Region"})
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
        for source_column, key, ptype in _LGR_ATTRIBUTE_PROPERTIES:
            _add_optional_property(properties, lgr_code, key, lgr_row.get(source_column), ptype)
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
