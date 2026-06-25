# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Metadata-driven discovery of DW_EK columns for data quality checks.

Queries INFORMATION_SCHEMA and sys.* to discover surrogate key columns,
parse dimension names from naming conventions, detect primary keys,
and resolve target dimension/map tables.
"""

from __future__ import annotations

import re

# Pattern: DW_EK_{DimName} or DW_EK_{DimName}_{RoleSuffix}
# Dimension names must NOT contain underscores.
_DW_EK_PATTERN = re.compile(r"^DW_EK_([A-Za-z][A-Za-z0-9]*)(?:_(.+))?$")


def parse_dim_name(column_name: str) -> tuple[str, str | None] | None:
    """Parse a DW_EK_ column name into (dim_name, role_suffix | None).

    Returns None if the column name doesn't match the DW_EK_ pattern.

    Examples:
        parse_dim_name("DW_EK_Kreditor")           -> ("Kreditor", None)
        parse_dim_name("DW_EK_Dato_Godkendelses")  -> ("Dato", "Godkendelses")
        parse_dim_name("DW_EK_Org_MarkoerA")       -> ("Org", "MarkoerA")
        parse_dim_name("SomeColumn")                -> None
    """
    match = _DW_EK_PATTERN.match(column_name)
    if not match:
        return None
    dim_name = match.group(1)
    role_suffix = match.group(2)
    return (dim_name, role_suffix)


def discover_dw_ek_columns(cursor, schemas: list[str]) -> list[dict]:
    """Query INFORMATION_SCHEMA for all DW_EK_ columns in given schemas.

    Returns list of dicts with keys:
        schema, table, column, dim_name, role_suffix, data_type

    Note: schemas are safe hardcoded values (fct/dim/map), so string formatting
    is used instead of parameterized queries to avoid pymssql/mssql-python placeholder
    incompatibility (%s vs ?).
    """
    schema_list = ",".join(f"'{s}'" for s in schemas)
    cursor.execute(
        f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN ({schema_list})
          AND COLUMN_NAME LIKE 'DW[_]EK[_]%'
        ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        """
    )
    results = []
    for row in cursor.fetchall():
        schema, table, column, data_type = row
        parsed = parse_dim_name(column)
        if parsed is None:
            continue
        dim_name, role_suffix = parsed
        results.append(
            {
                "schema": schema,
                "table": table,
                "column": column,
                "dim_name": dim_name,
                "role_suffix": role_suffix,
                "data_type": data_type,
            }
        )
    return results


def get_primary_key_columns(cursor, schema: str, table: str) -> set[str]:
    """Return set of column names that are part of the table's primary key.

    Uses string formatting (safe: schema/table come from INFORMATION_SCHEMA).
    """
    cursor.execute(
        f"""
        SELECT c.name
        FROM sys.index_columns ic
        JOIN sys.columns c
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        JOIN sys.indexes i
            ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.is_primary_key = 1
          AND ic.object_id = OBJECT_ID(N'[{schema}].[{table}]')
        """
    )
    return {row[0] for row in cursor.fetchall()}


def get_existing_tables(cursor, schema: str) -> set[str]:
    """Return set of table names that exist in the given schema.

    Uses string formatting (safe: schema comes from hardcoded list).
    """
    cursor.execute(
        f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE'
        """
    )
    return {row[0] for row in cursor.fetchall()}


def sentinel_literal(data_type: str) -> str:
    """Return the SQL literal for the sentinel value based on column data type."""
    if data_type in ("varchar", "nvarchar", "char", "nchar"):
        return "'-1'"
    return "-1"
