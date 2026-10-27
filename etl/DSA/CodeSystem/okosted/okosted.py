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
  code_max_length=120, delete_stale=False) into the persistent codesystem.Okosted*
  tables which feed stg/map/dim_Okosted.

  No dependency on any full-load extract job — the arc table always holds the
  full history; a from-scratch rebuild of codesystem.Okosted* can run cs_okosted
  directly after the sydax_archive group without any prerequisite full-load.

  Node identity: A node is the pair (nummer, niveau) — the same nummer value
  recurring at different levels becomes distinct nodes. Each node's code is
  "<nummer>_NIVEAU_<niveau>"; hierarchy edges always go exactly one level up.
  Deleted rows (slettet=1) are built and included. Each node carries properties:
  enhedstype, nummer, and status (aktiv/slettet) from its own rows, plus
  startsdate, enddate, kildesystem, and OmkostningsstedHierarkiId when present.
"""

from datetime import datetime

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables, touch_concepts

LEVEL_SUFFIX = "_NIVEAU_"
MAX_CODE_LENGTH = 120
MAX_LEVELS = 20


def _node_code(nummer: str, niveau: int) -> str:
    return f"{nummer}{LEVEL_SUFFIX}{niveau}"


def _newest_row(rows: list[dict]) -> dict:
    """The row with the newest startsdate/startdatetime, first row as fallback."""
    newest_row = None
    newest_startdate = None
    for row in rows:
        startsdate = row.get("startsdate") or row.get("startdatetime")
        if startsdate is not None:
            date_obj = _parse_date(startsdate)
            if date_obj is not None and (newest_startdate is None or date_obj > newest_startdate):
                newest_startdate = date_obj
                newest_row = row
    if newest_row is None:
        return rows[0]
    return newest_row


def _enhedstype(row: dict, niveau: int) -> str:
    if row.get(f"jeid{niveau}") is not None:
        return "juridisk enhed"
    if row.get(f"veid{niveau}") is not None:
        return "virksomhedsenhed"
    if row.get(f"afid{niveau}") is not None:
        return "afdeling"
    if row.get(f"osid{niveau}") is not None:
        return "omkostningssted"
    return "Ukendt"


def build_okosted_hierarchy(
    okosted_rows: list,
    okosted_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict], dict[str, int]]:
    """
    Build hierarchy structure for okosted codesystem from ext.* table rows.

    A node's identity is the pair (nummer, niveau): the same nummer value
    recurring at different levels (a deleted placeholder row reusing a
    number, a self-referencing root row, …) becomes distinct nodes rather
    than being merged into one code. Each node's code is
    "<nummer>_NIVEAU_<niveau>".

    All dict key lookups are lowercased via _normalize_keys to handle
    case differences between DB drivers / source schemas.

    Returns:
        (concepts, properties, hierarchy, counters) quadruple. counters
        tracks parent_conflicts, path_gaps and unknown_enhedstype — data
        quality signals from the source, not failures.
    """
    okosted_data = (
        _rows_to_dicts(okosted_rows, okosted_columns)
        if okosted_columns
        else _ensure_dicts(okosted_rows)
    )
    okosted_data = [_normalize_keys(row) for row in okosted_data]

    counters = {"parent_conflicts": 0, "path_gaps": 0, "unknown_enhedstype": 0}

    # 1) Collect cells: node (nummer, niveau) -> list of rows it appears in.
    node_rows: dict[tuple[str, int], list[dict]] = {}
    row_max_levels: dict[int, int] = {}
    for row in okosted_data:
        max_level = 0
        for niveau in range(1, MAX_LEVELS + 1):
            nummer = row.get(f"nummer{niveau}")
            if nummer is None or not str(nummer).strip():
                continue
            nummer = str(nummer).strip()
            if LEVEL_SUFFIX in nummer:
                raise ValueError(
                    f"Source nummer {nummer!r} contains the reserved level "
                    f"suffix {LEVEL_SUFFIX!r} — codes would be ambiguous."
                )
            node_rows.setdefault((nummer, niveau), []).append(row)
            max_level = niveau
        row_max_levels[id(row)] = max_level

    concepts, properties, hierarchy = [], [], []
    for (nummer, niveau), rows in node_rows.items():
        code = _node_code(nummer, niveau)
        if len(code) > MAX_CODE_LENGTH:
            raise ValueError(f"Code {code!r} exceeds {MAX_CODE_LENGTH} chars.")

        newest = _newest_row(rows)

        display = str(newest.get(f"tekst{niveau}") or "").strip() or nummer
        concepts.append({"CODE": code, "DISPLAY": display})

        properties.append({"CODE": code, "KEY": "nummer", "VALUE": nummer, "TYPE": "String"})

        enhedstype = _enhedstype(newest, niveau)
        if enhedstype == "Ukendt":
            counters["unknown_enhedstype"] += 1
        properties.append(
            {"CODE": code, "KEY": "enhedstype", "VALUE": enhedstype, "TYPE": "String"}
        )

        # The node's OWN rows: those where this (nummer, niveau) is the
        # deepest filled cell. A grouping level (RSD, PSD, …) has none — it
        # only appears as an ancestor in other nodes' rows.
        own_rows = [r for r in rows if row_max_levels[id(r)] == niveau]

        seen_hierarki_ids: set[int] = set()
        for row in own_rows:
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

        # Status comes from the node's own row only: an ancestor's status in
        # SydAX is not recorded, and a descendant's row says nothing about it.
        status_row = _status_row(own_rows)
        if status_row is not None:
            for key in ("aktiv", "slettet"):
                value = status_row.get(key)
                if value is not None:
                    properties.append(
                        {"CODE": code, "KEY": key, "VALUE": str(value), "TYPE": "Boolean"}
                    )

        startsdate = newest.get("startsdate") or newest.get("startdatetime")
        if startsdate is not None:
            startsdate_str = (
                startsdate.strftime("%Y-%m-%d")
                if hasattr(startsdate, "strftime")
                else str(startsdate)
            )
            properties.append(
                {"CODE": code, "KEY": "startsdate", "VALUE": startsdate_str, "TYPE": "Date"}
            )

        enddate = newest.get("enddate") or newest.get("enddatetime")
        if enddate is not None:
            enddate_str = (
                enddate.strftime("%Y-%m-%d") if hasattr(enddate, "strftime") else str(enddate)
            )
            properties.append(
                {"CODE": code, "KEY": "enddate", "VALUE": enddate_str, "TYPE": "Date"}
            )

        kildesystem = newest.get("kildesystem")
        if kildesystem is not None and str(kildesystem).strip():
            properties.append(
                {
                    "CODE": code,
                    "KEY": "kildesystem",
                    "VALUE": str(kildesystem).strip(),
                    "TYPE": "String",
                }
            )

        if niveau > 1:
            parents = {
                str(r.get(f"nummer{niveau - 1}") or "").strip()
                for r in rows
                if str(r.get(f"nummer{niveau - 1}") or "").strip()
            }
            if len(parents) > 1:
                counters["parent_conflicts"] += 1
            parent = str(newest.get(f"nummer{niveau - 1}") or "").strip()
            if parent:
                hierarchy.append({"CODE": code, "PARENT_CODE": _node_code(parent, niveau - 1)})
            else:
                counters["path_gaps"] += 1

    return concepts, properties, hierarchy, counters


def _is_set(value) -> bool:
    return str(value).strip().lower() in ("1", "true")


def _status_row(own_rows: list[dict]) -> dict | None:
    """The own row whose Aktiv/Slettet stand for the code, or None.

    When a code has several own rows that disagree, a row that is not deleted
    wins, then an active one, then the highest OmkostningsstedHierarkiId — so
    a code still in use somewhere is delivered as in use, and the pick does
    not depend on the order the rows were read in.
    """
    if not own_rows:
        return None
    return min(
        own_rows,
        key=lambda row: (
            _is_set(row.get("slettet")),
            not _is_set(row.get("aktiv")),
            -int(row.get("omkostningsstedhierarkiid") or 0),
        ),
    )


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

            concepts, properties, hierarchy, counters = build_okosted_hierarchy(
                okosted_rows, okosted_cols
            )

            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            if counters.get("parent_conflicts", 0) > 0:
                context.log.warning(
                    f"parent_conflicts: {counters['parent_conflicts']} — "
                    f"nodes with multiple parent references resolved to newest row"
                )
            if counters.get("path_gaps", 0) > 0:
                context.log.warning(
                    f"path_gaps: {counters['path_gaps']} — "
                    f"nodes with missing intermediate levels (made roots)"
                )
            if counters.get("unknown_enhedstype", 0) > 0:
                context.log.warning(
                    f"unknown_enhedstype: {counters['unknown_enhedstype']} — "
                    f"nodes with no typed ID column (marked as Ukendt)"
                )

            result = merge_codesystem_tables(
                cursor,
                "okosted",
                concepts,
                properties,
                hierarchy,
                code_max_length=MAX_CODE_LENGTH,
                # The build is a full snapshot, so what it no longer emits is gone.
                delete_stale_properties=True,
                delete_stale_hierarchy=True,
                update_only_on_change=True,
                collect_deleted_codes=True,
            )
            deleted_codes = result.pop("deleted_codes", [])
            if deleted_codes:
                touch_concepts(cursor, "okosted", deleted_codes)
                result["concepts_touched_for_deletes"] = len(deleted_codes)
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.okosted: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u"
        f"/{result.get('properties_deleted', 0)}d), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u"
        f"/{result.get('hierarchy_deleted', 0)}d)"
    )

    result.update(counters)

    return dg.MaterializeResult(metadata=result)
