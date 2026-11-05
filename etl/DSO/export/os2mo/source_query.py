# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The reads of codesystem.* that feed an OS2mo delivery, and the write of the
delivery set to export.CodesystemToOs2mo.

Every codesystem in the warehouse has the same three tables —
``codesystem.<name>Concept``, ``<name>Hierarchy`` and ``<name>Properties`` — so
one pivot, parameterised by a :class:`CodesystemExportSpec`, reads them all.

A spec with a ``code_source`` (EAN) reads its codes from that table instead,
and only its name from the codesystem — see :func:`build_code_source_query`.

Always a full read. The delivery is a full load by contract, so there is no
DFM checkpoint and no delta window here. When a spec has a scope, the scope is
applied in SQL rather than in Python: for SOR the codes outside the region's
tree are the bulk of the national register, and the builder never
looks at them.

export.CodesystemToOs2mo mirrors the file column for column, so a query reads
like the CSV. Three of the file's columns — ``dw_is_deleted``,
``dw_timestamp_insert`` and ``dw_timestamp_update`` — collide with this
warehouse's own audit-column names, so the table carries them with a
``_codesystem`` postfix; the table's own (postfix-free) ``dw_timestamp_insert``
is the build stamp the writer appends, not a file column.
"""

from datetime import datetime
from typing import Any

from core.resources import MSSQLResource
from utils.os2mo.codesystem_export import (
    CodesystemExportSpec,
    parse_export_date,
    parse_export_timestamp,
)

__all__ = [
    "EXPORT_TABLE",
    "EXPORT_TABLE_COLUMNS",
    "build_code_source_query",
    "build_source_query",
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
    "scope",
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


def build_code_source_query(spec: CodesystemExportSpec) -> str:
    """The codes of a spec's ``code_source``, named from the codesystem.

    Every distinct non-empty code in the source table is delivered, whether
    the codesystem knows it or not, so ``has_properties`` is always 1 and no
    code is ever "gone". ``display`` is the ``display_property`` value, NULL
    (an empty navn) when the codesystem has none.

    ``created`` is the first time the code was seen in the source table;
    ``updated`` is the later of that and the name property's own
    ``dw_timestamp_update``, so a changed name moves it.
    """
    source = spec.code_source
    assert source is not None
    null = "CAST(NULL AS NVARCHAR(500))"
    name_join = ""
    display_select = null
    updated_select = "k.[first_seen]"
    if spec.display_property is not None:
        name_join = (
            "    LEFT JOIN [codesystem].[{name}Properties] p_name\n"
            "           ON p_name.[code] = k.[code]"
            f" AND p_name.[property_code] = '{spec.display_property}'\n"
        )
        display_select = "p_name.[value]"
        updated_select = (
            "(SELECT MAX(ts) FROM (VALUES (k.[first_seen]),\n"
            "                                        (p_name.[dw_timestamp_update])) AS v(ts))"
        )
    sql = (
        "    SELECT k.[code]          AS [code],\n"
        f"           {display_select} AS [display],\n"
        f"           {null} AS [parent_code],\n"
        f"           {null} AS [valid_from],\n"
        f"           {null} AS [valid_to],\n"
        f"           {null} AS [type],\n"
        f"           {null} AS [scope],\n"
        f"           {null} AS [status_active],\n"
        f"           {null} AS [status_deleted],\n"
        "           k.[first_seen]    AS [created],\n"
        f"           {updated_select} AS [updated],\n"
        "           1 AS [has_properties]\n"
        "    FROM (\n"
        f"        SELECT src.[{source.column}] AS [code],\n"
        "               MIN(src.[dw_timestamp_insert]) AS [first_seen]\n"
        f"        FROM [{source.schema}].[{source.table}] src\n"
        f"        WHERE src.[{source.column}] IS NOT NULL AND src.[{source.column}] <> ''\n"
        f"        GROUP BY src.[{source.column}]\n"
        "    ) k\n" + name_join
    )
    return sql.format(name=spec.name)


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
    the scope value is a code constant — nothing here comes from run config.
    """
    scope_join = ""
    if spec.scope_property is not None:
        scope_join = (
            _property_join("p_scope", spec.scope_property, inner=True).rstrip("\n")
            + f"\n          AND p_scope.[value] = '{spec.scope_value}'\n"
        )
    from_select = "p_from.[value]" if spec.from_property else "CAST(NULL AS NVARCHAR(500))"
    to_select = "p_to.[value]" if spec.to_property else "CAST(NULL AS NVARCHAR(500))"
    type_select = "p_type.[value]" if spec.type_property else "CAST(NULL AS NVARCHAR(500))"
    scope_select = "p_scope.[value]" if spec.scope_property else "CAST(NULL AS NVARCHAR(500))"
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
        f"           {from_select} AS [valid_from],\n"
        f"           {to_select} AS [valid_to],\n"
        f"           {type_select} AS [type],\n"
        f"           {scope_select} AS [scope],\n"
        f"           {status_active_select} AS [status_active],\n"
        f"           {status_deleted_select} AS [status_deleted],\n"
        "           c.[dw_timestamp_insert] AS [created],\n"
        "           (SELECT MAX(ts) FROM (VALUES (c.[dw_timestamp_update]),\n"
        "                                        (h.[dw_timestamp_update]),\n"
        "                                        (p_all.[last_update])) AS v(ts)) AS [updated],\n"
        "           CASE WHEN p_all.[last_update] IS NULL THEN 0 ELSE 1 END AS [has_properties]\n"
        "    FROM [codesystem].[{name}Concept] c\n" + scope_join + "    OUTER APPLY (\n"
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
    query = build_code_source_query(spec) if spec.code_source else build_source_query(spec)
    rows = _fetch(mssql, query)
    return [dict(zip(_SOURCE_COLUMNS, row, strict=False)) for row in rows]


# --------------------------------------------------------------------------
# Writing the delivery set
# --------------------------------------------------------------------------

EXPORT_TABLE = "[export].[CodesystemToOs2mo]"

# Column order of export.CodesystemToOs2mo, minus the table's own
# dw_timestamp_insert (the build stamp), which the writer appends itself.
# Named as the file's columns, except the three that collide with this
# warehouse's own audit-column names — those carry a _codesystem postfix.
EXPORT_TABLE_COLUMNS = (
    "codesystem",
    "kode",
    "kode_parent",
    "gyldig_fra",
    "gyldig_til",
    "navn",
    "type",
    "niveau",
    "aktiv",
    "dw_is_deleted_codesystem",
    "dw_timestamp_insert_codesystem",
    "dw_timestamp_update_codesystem",
)

INSERT_CHUNK_SIZE = 1000


def _export_row_to_tuple(
    spec: CodesystemExportSpec, row: dict[str, Any], built_at: datetime
) -> tuple:
    """Convert one file row (keyed by BASE_COLUMNS) into the table's typed,
    _codesystem-postfixed column order, plus the table's own build stamp.

    The export model carries the CSV's text, because that file is the contract.
    The table is for querying, so the values are converted back to real types
    here; empty strings become NULL rather than ''.
    """

    def _bit(value: Any) -> int | None:
        return {"true": 1, "false": 0}.get(str(value or ""))

    def _text(key: str) -> str | None:
        value = str(row.get(key) or "").strip()
        return value or None

    return (
        spec.name,
        str(row["kode"]),
        _text("kode_parent"),
        parse_export_date(row.get("gyldig_fra")),
        parse_export_date(row.get("gyldig_til")),
        _text("navn"),
        _text("type"),
        int(row["niveau"]),
        _bit(row.get("aktiv")),
        _bit(row.get("dw_is_deleted")),
        parse_export_timestamp(row.get("dw_timestamp_insert")),
        parse_export_timestamp(row.get("dw_timestamp_update")),
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
