# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: Okosted (Omkostningssted)

Reads Okosted source data from arc.sydax_omkostningssted_hierarki,
builds codesystem hierarchy from nummer1-20/tekst1-20 columns,
and MERGEs into persistent tables:
codesystem.okosted_concept, _properties, _hierarchy.

RUNBOOK — full current-version snapshot each run:
  arc.sydax_omkostningssted_hierarki is the DFM-managed SCD archive; each
  OmkostningsstedHierarkiId may have multiple rows (one per SCD version).
  cs_okosted collapses to the current version via ROW_NUMBER PARTITION BY
  OmkostningsstedHierarkiId ORDER BY EndDateTime DESC, StartDateTime DESC,
  dw_timestamp_update DESC (rn=1), giving a complete current-version snapshot
  on every run. The result is UPSERTed (merge_codesystem_tables,
  delete_stale=False) into the persistent codesystem.Okosted* tables which
  feed stg/map/dim_Okosted.

  No dependency on any full-load extract job — the arc table always holds the
  full history; a from-scratch rebuild of codesystem.Okosted* can run cs_okosted
  directly after the sydax_archive group without any prerequisite full-load.
"""

from datetime import datetime

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables


def build_okosted_hierarchy(
    okosted_rows: list,
    okosted_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build hierarchy structure for okosted codesystem from ext.* table rows.

    Codes are based on ALL nummer1-20 values:
    - Each unique nummer value at each level becomes a code
    - display is the corresponding tekst value
    - OmkostningsstedHierarkiId is stored in properties to track back to original row
    - Hierarchy is built by matching nummer values at parent levels

    All dict key lookups are lowercased via _normalize_keys to handle
    case differences between DB drivers / source schemas.

    Returns:
        (concepts, properties, hierarchy) triple
    """
    okosted_data = (
        _rows_to_dicts(okosted_rows, okosted_columns)
        if okosted_columns
        else _ensure_dicts(okosted_rows)
    )

    okosted_data = [_normalize_keys(row) for row in okosted_data]

    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    code_lookup: dict[str, dict] = {}
    row_max_levels: dict[int, int] = {}

    for row in okosted_data:
        max_level = 0
        for level in range(1, 21):
            nummer = row.get(f"nummer{level}")
            if nummer is not None and str(nummer).strip():
                max_level = level

        row_max_levels[id(row)] = max_level

        for level in range(1, 21):
            nummer = row.get(f"nummer{level}")
            if nummer is None or not str(nummer).strip():
                continue

            nummer_value = str(nummer).strip()
            tekst = row.get(f"tekst{level}")
            tekst_value = (
                str(tekst).strip() if tekst is not None and str(tekst).strip() else nummer_value
            )

            code = nummer_value

            if code not in code_lookup:
                code_lookup[code] = {
                    "level": level,
                    "display": tekst_value,
                    "row_levels": [],
                }

            code_lookup[code]["row_levels"].append((row, level))

    for code, code_info in code_lookup.items():
        row_levels = code_info["row_levels"]

        newest_row = None
        newest_startdate = None

        for row, _code_level in row_levels:
            startsdate = row.get("startsdate") or row.get("startdatetime")
            if startsdate is not None:
                date_obj = _parse_date(startsdate)
                if date_obj is not None:
                    if newest_startdate is None or date_obj > newest_startdate:
                        newest_startdate = date_obj
                        newest_row = row

        if newest_row is None and row_levels:
            newest_row = row_levels[0][0]

        display = code_info["display"]
        if newest_row:
            for check_level in range(1, 21):
                nummer = newest_row.get(f"nummer{check_level}")
                if nummer is not None and str(nummer).strip() == code:
                    tekst = newest_row.get(f"tekst{check_level}")
                    if tekst is not None and str(tekst).strip():
                        display = str(tekst).strip()
                    break

        concepts.append({"CODE": code, "DISPLAY": display})

        seen_hierarki_ids: set[int] = set()
        for row, code_level in row_levels:
            nummer_at_level = row.get(f"nummer{code_level}")
            if nummer_at_level is None or str(nummer_at_level).strip() != code:
                continue

            row_max_level = row_max_levels.get(id(row), 0)
            if code_level != row_max_level:
                continue

            hierarki_id = row.get("omkostningsstedhierarkiid")
            if hierarki_id is not None:
                hierarki_id_int = int(hierarki_id)
                if hierarki_id_int not in seen_hierarki_ids:
                    properties.append(
                        {
                            "CODE": code,
                            "KEY": "OmkostningsstedHierarkiId",
                            "VALUE": str(hierarki_id_int),
                            "TYPE": "integer",
                        }
                    )
                    seen_hierarki_ids.add(hierarki_id_int)

        min_level = min(code_level for (_, code_level) in row_levels)
        properties.append(
            {
                "CODE": code,
                "KEY": "nummer_kolonne",
                "VALUE": f"nummer{min_level}",
                "TYPE": "String",
            }
        )

        if newest_row:
            aktiv = newest_row.get("aktiv")
            if aktiv is not None:
                properties.append(
                    {"CODE": code, "KEY": "aktiv", "VALUE": str(aktiv), "TYPE": "Boolean"}
                )

            startsdate = newest_row.get("startsdate") or newest_row.get("startdatetime")
            if startsdate is not None:
                startsdate_str = (
                    startsdate.strftime("%Y-%m-%d")
                    if hasattr(startsdate, "strftime")
                    else str(startsdate)
                )
                properties.append(
                    {"CODE": code, "KEY": "startsdate", "VALUE": startsdate_str, "TYPE": "Date"}
                )

            enddate = newest_row.get("enddate") or newest_row.get("enddatetime")
            if enddate is not None:
                enddate_str = (
                    enddate.strftime("%Y-%m-%d") if hasattr(enddate, "strftime") else str(enddate)
                )
                properties.append(
                    {"CODE": code, "KEY": "enddate", "VALUE": enddate_str, "TYPE": "Date"}
                )

            kildesystem = newest_row.get("kildesystem")
            if kildesystem is not None and str(kildesystem).strip():
                properties.append(
                    {
                        "CODE": code,
                        "KEY": "kildesystem",
                        "VALUE": str(kildesystem).strip(),
                        "TYPE": "String",
                    }
                )

        parent_found = False
        for row, code_level in row_levels:
            if parent_found:
                break
            if code_level > 1:
                parent_nummer = row.get(f"nummer{code_level - 1}")
                if parent_nummer is not None and str(parent_nummer).strip():
                    parent_code = str(parent_nummer).strip()
                    if parent_code in code_lookup:
                        hierarchy.append({"CODE": code, "PARENT_CODE": parent_code})
                        parent_found = True

    return concepts, properties, hierarchy


def _parse_date(value):
    """Parse a date value from various formats into a comparable date object."""
    if hasattr(value, "date"):
        return value.date() if callable(value.date) else value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
    return value


def _normalize_keys(d: dict) -> dict:
    """Lowercase all dict keys to handle case differences between DB drivers."""
    return {k.lower(): v for k, v in d.items()}


def _rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict]:
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _ensure_dicts(rows: list) -> list[dict]:
    if rows and isinstance(rows[0], dict):
        return rows
    return rows


@dg.asset(
    name="cs_okosted",
    deps=["arc_sydax_omkostningssted_hierarki"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "okosted"},
    description=(
        "Build Okosted codesystem from arc.sydax_omkostningssted_hierarki "
        "(collapsed to current SCD version via ROW_NUMBER) "
        "and MERGE into codesystem.okosted_*. "
        "Reads a full current-version snapshot each run — no dependency on "
        "a full-load extract job. A from-scratch rebuild is safe to run directly "
        "after the sydax_archive group."
    ),
)
def cs_okosted(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT * FROM (
                    SELECT h.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY h.[OmkostningsstedHierarkiId]
                               ORDER BY h.[EndDateTime] DESC, h.[StartDateTime] DESC,
                                        h.[dw_timestamp_update] DESC
                           ) AS _rn
                    FROM [arc].[sydax_omkostningssted_hierarki] h
                ) cur
                WHERE cur._rn = 1
                """
            )
            okosted_rows = cursor.fetchall()
            okosted_cols = [col[0] for col in cursor.description]

            context.log.info(
                f"Read from arc.sydax_omkostningssted_hierarki (current version): "
                f"{len(okosted_rows)} rows, "
                f"columns: {okosted_cols[:10]}{'...' if len(okosted_cols) > 10 else ''}"
            )

            concepts, properties, hierarchy = build_okosted_hierarchy(okosted_rows, okosted_cols)

            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            result = merge_codesystem_tables(cursor, "okosted", concepts, properties, hierarchy)
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.okosted: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u)"
    )

    return dg.MaterializeResult(metadata=result)
