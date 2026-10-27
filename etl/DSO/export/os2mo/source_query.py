# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The reads of codesystem.* and conceptmap.* that feed an OS2mo delivery, and
the write of the delivery set to export.CodesystemToOs2mo.

Every codesystem in the warehouse has the same three tables —
``codesystem.<name>Concept``, ``<name>Hierarchy`` and ``<name>Properties`` — so
one pivot, parameterised by a :class:`CodesystemExportSpec`, reads them all.

Always a full read. The delivery is a full load by contract, so there is no
DFM checkpoint and no delta window here. When a spec filters on an active flag,
the filter is applied in SQL rather than in Python: for SOR the inactive codes
are ~96% of the national register, and the builder never looks at them.
"""

import json
from datetime import datetime
from typing import Any

from core.resources import MSSQLResource
from utils.os2mo.codesystem_export import (
    CodesystemExportSpec,
    MappingSource,
    parse_export_date,
    parse_export_timestamp,
)

__all__ = [
    "EXPORT_TABLE",
    "EXPORT_TABLE_COLUMNS",
    "build_mapping_query",
    "build_source_query",
    "read_mappings",
    "read_source_rows",
    "write_export_table",
]

_SOURCE_COLUMNS = (
    "code",
    "display",
    "parent_code",
    "valid_from",
    "valid_to",
    "type",
    "active",
    "status_active",
    "status_deleted",
    "created",
    "updated",
    "has_properties",
)


def _property_join(alias: str, property_code: str | None, *, inner: bool = False) -> str:
    """One pivoted property, or nothing when the spec does not use it."""
    if property_code is None:
        return ""
    join = "INNER JOIN" if inner else "LEFT JOIN"
    return (
        f"    {join} [codesystem].[{{name}}Properties] {alias}\n"
        f"           ON {alias}.[code] = c.[code] AND {alias}.[property_code] = '{property_code}'\n"
    )


def build_source_query(spec: CodesystemExportSpec) -> str:
    """The pivot for one codesystem, with each code's warehouse timestamps.

    ``created`` is when the code first entered ``codesystem.<name>Concept``;
    ``updated`` is the latest change to the code itself — its concept row, any
    of its properties or its parent edge.

    Those timestamps only mean "changed" because the codesystem is merged with
    ``update_only_on_change`` — otherwise every matched row is bumped nightly.

    The delivered codesystems delete stale hierarchy edges, so a code has one
    parent edge; ``TOP 1`` only keeps a second edge from duplicating the row.

    Identifiers come from the spec, which only accepts plain identifiers, and
    the active value is a code constant — nothing here comes from run config.
    """
    active_join = ""
    if spec.active_property is not None:
        active_join = (
            _property_join("p_active", spec.active_property, inner=True).rstrip("\n")
            + f"\n          AND p_active.[value] = '{spec.active_value}'\n"
        )
    type_select = "p_type.[value]" if spec.type_property else "CAST(NULL AS NVARCHAR(500))"
    active_select = "p_active.[value]" if spec.active_property else "CAST(NULL AS NVARCHAR(500))"
    status_active_select = (
        "p_sact.[value]" if spec.status_active_property else "CAST(NULL AS NVARCHAR(500))"
    )
    status_deleted_select = (
        "p_sdel.[value]" if spec.status_deleted_property else "CAST(NULL AS NVARCHAR(500))"
    )

    sql = (
        "    SELECT c.[code]          AS [code],\n"
        "           c.[display]       AS [display],\n"
        "           h.[parent_code]   AS [parent_code],\n"
        "           p_from.[value]    AS [valid_from],\n"
        "           p_to.[value]      AS [valid_to],\n"
        f"           {type_select} AS [type],\n"
        f"           {active_select} AS [active],\n"
        f"           {status_active_select} AS [status_active],\n"
        f"           {status_deleted_select} AS [status_deleted],\n"
        "           c.[dw_timestamp_insert] AS [created],\n"
        "           (SELECT MAX(ts) FROM (VALUES (c.[dw_timestamp_update]),\n"
        "                                        (h.[dw_timestamp_update]),\n"
        "                                        (p_all.[last_update])) AS v(ts)) AS [updated],\n"
        "           CASE WHEN p_all.[last_update] IS NULL THEN 0 ELSE 1 END AS [has_properties]\n"
        "    FROM [codesystem].[{name}Concept] c\n" + active_join + "    OUTER APPLY (\n"
        "        SELECT TOP 1 hh.[parent_code], hh.[dw_timestamp_update]\n"
        "        FROM [codesystem].[{name}Hierarchy] hh\n"
        "        WHERE hh.[child_code] = c.[code]\n"
        "        ORDER BY hh.[dw_timestamp_insert] DESC, hh.[parent_code]\n"
        "    ) h\n"
        "    OUTER APPLY (\n"
        "        SELECT MAX(pp.[dw_timestamp_update]) AS [last_update]\n"
        "        FROM [codesystem].[{name}Properties] pp\n"
        "        WHERE pp.[code] = c.[code]\n"
        "    ) p_all\n"
        + _property_join("p_from", spec.from_property)
        + _property_join("p_to", spec.to_property)
        + _property_join("p_type", spec.type_property)
        + _property_join("p_sact", spec.status_active_property)
        + _property_join("p_sdel", spec.status_deleted_property)
    )
    return sql.format(name=spec.name)


def build_mapping_query(mapping: MappingSource) -> str:
    """Every (source, target, relationship) row of one ConceptMap."""
    return (
        "    SELECT [source_code], [target_code], [relationship]\n"
        f"    FROM [conceptmap].[{mapping.table}]"
    )


def _fetch(mssql: MSSQLResource, sql: str) -> list[tuple]:
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return list(cursor.fetchall())
        finally:
            cursor.close()


def read_source_rows(mssql: MSSQLResource, spec: CodesystemExportSpec) -> list[dict[str, Any]]:
    """Read the codesystem's codes with the properties the export needs."""
    rows = _fetch(mssql, build_source_query(spec))
    return [dict(zip(_SOURCE_COLUMNS, row, strict=False)) for row in rows]


def read_mappings(
    mssql: MSSQLResource, spec: CodesystemExportSpec
) -> dict[str, list[tuple[str, str, str]]]:
    """Every pre-mapped target per source code, across the spec's ConceptMaps."""
    by_code: dict[str, list[tuple[str, str, str]]] = {}
    for mapping in spec.mappings:
        for source_code, target_code, relationship in _fetch(mssql, build_mapping_query(mapping)):
            if not source_code or not target_code:
                continue
            by_code.setdefault(str(source_code), []).append(
                (str(target_code), mapping.system, str(relationship or ""))
            )
    return by_code


# --------------------------------------------------------------------------
# Writing the delivery set
# --------------------------------------------------------------------------

EXPORT_TABLE = "[export].[CodesystemToOs2mo]"

# Column order of export.CodesystemToOs2mo, minus dw_timestamp_insert which the
# writer stamps itself. Named as the file's columns.
EXPORT_TABLE_COLUMNS = (
    "codesystem",
    "user_key",
    "parent_user_key",
    "validity_from",
    "validity_to",
    "name",
    "org_unit_type",
    "active",
    "deleted",
    "created",
    "updated",
    "mappings",
)

INSERT_CHUNK_SIZE = 1000


def _export_row_to_tuple(
    spec: CodesystemExportSpec, row: dict[str, Any], built_at: datetime
) -> tuple:
    """Convert one export row into the table's typed column order.

    The export model carries the CSV's text, because that file is the contract.
    The table is for querying, so the values are converted back to real types
    here; empty strings become NULL rather than ''.
    """

    def _bit(value: Any) -> int | None:
        return {"true": 1, "false": 0}.get(str(value or ""))

    def _text(key: str) -> str | None:
        value = str(row.get(key) or "").strip()
        return value or None

    mappings = row.get("_mappings") or []
    return (
        spec.name,
        str(row["user_key"]),
        _text("parent_user_key"),
        parse_export_date(row.get("validity_from")),
        parse_export_date(row.get("validity_to")),
        _text("name"),
        _text("org_unit_type"),
        _bit(row.get("active")),
        _bit(row.get("deleted")),
        parse_export_timestamp(row.get("created")),
        parse_export_timestamp(row.get("updated")),
        (
            json.dumps(
                [
                    {"code": code, "system": system, "relationship": relation}
                    for code, system, relation in mappings
                ],
                ensure_ascii=False,
            )
            if mappings
            else None
        ),
        built_at,
    )


def write_export_table(
    mssql: MSSQLResource,
    spec: CodesystemExportSpec,
    export_rows: list[dict[str, Any]],
    built_at: datetime,
) -> int:
    """Replace this codesystem's rows in export.CodesystemToOs2mo.

    DELETE + INSERT for the one codesystem rather than a MERGE: the table holds
    the CURRENT set per codesystem, not a history. Each codesystem's export
    asset only touches its own rows, so they can run independently.

    Both statements run on one connection and are committed together, so a
    failure mid-insert leaves the previous set in place rather than nothing.
    """
    columns = (*EXPORT_TABLE_COLUMNS, "dw_timestamp_insert")
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(f"[{column}]" for column in columns)
    insert_sql = f"INSERT INTO {EXPORT_TABLE} ({column_list}) VALUES ({placeholders})"

    rows = [_export_row_to_tuple(spec, row, built_at) for row in export_rows]

    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True
            cursor.execute(f"DELETE FROM {EXPORT_TABLE} WHERE [codesystem] = ?", spec.name)
            for start in range(0, len(rows), INSERT_CHUNK_SIZE):
                cursor.executemany(insert_sql, rows[start : start + INSERT_CHUNK_SIZE])
            conn.commit()
        finally:
            cursor.close()

    return len(rows)
