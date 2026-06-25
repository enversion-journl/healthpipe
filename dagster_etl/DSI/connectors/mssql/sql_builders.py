# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""MSSQL where-clause builders, partition logic, schema introspection, and consistency checks.

All SQL-generation helpers for MSSQL extraction live here. The MssqlExtractor
class imports from this module rather than defining these inline.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dagster import AssetExecutionContext
from DSI.connectors.mssql.models import (
    TABLE_TYPE_BRIDGE,
    TABLE_TYPE_DEPENDENT,
    TABLE_TYPE_MASTER_DATA,
    TableConfig,
)

# ---------------------------------------------------------------------------
# Partition helpers
# ---------------------------------------------------------------------------


def is_monthly_partition(partition_key: str) -> bool:
    return len(partition_key) == 7 and partition_key.count("-") == 1


def get_month_date_range(partition_key: str) -> tuple[str, str]:
    from calendar import monthrange

    year, month = map(int, partition_key.split("-"))
    _, last_day = monthrange(year, month)
    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
    return start_date, end_date


# ---------------------------------------------------------------------------
# Single-column date filter
# ---------------------------------------------------------------------------


def build_single_column_date_filter(
    column_name: str,
    partition_key: str,
    is_string_date: bool = False,
    is_scheduled_catchup: bool = False,
    is_first_partition: bool = False,
) -> str:
    if is_monthly_partition(partition_key):
        start_date, end_date = get_month_date_range(partition_key)
        if is_first_partition:
            if is_string_date:
                return f"[{column_name}] <= '{end_date}'"
            return f"CAST([{column_name}] AS DATE) <= '{end_date}'"
        if is_string_date:
            return f"[{column_name}] >= '{start_date}' AND [{column_name}] <= '{end_date}'"
        return (
            f"CAST([{column_name}] AS DATE) >= '{start_date}' AND "
            f"CAST([{column_name}] AS DATE) <= '{end_date}'"
        )

    if is_scheduled_catchup:
        today_date = datetime.strptime(partition_key, "%Y-%m-%d")
        yesterday_date = today_date - timedelta(days=1)
        start_ts = yesterday_date.strftime("%Y-%m-%d") + " 01:00:00"
        if is_string_date:
            return f"[{column_name}] >= '{yesterday_date.strftime('%Y-%m-%d')}'"
        return f"[{column_name}] >= '{start_ts}'"

    if is_first_partition:
        if is_string_date:
            return f"[{column_name}] <= '{partition_key}'"
        return f"CAST([{column_name}] AS DATE) <= '{partition_key}'"

    if is_string_date:
        return f"[{column_name}] = '{partition_key}'"
    return f"CAST([{column_name}] AS DATE) = '{partition_key}'"


# ---------------------------------------------------------------------------
# Top-level where-clause builder (transactional / master / dependent)
# ---------------------------------------------------------------------------


def build_partition_where_clause(
    config: TableConfig,
    partition_key: str,
    source_system: str,
    is_scheduled_catchup: bool = False,
    is_first_partition: bool = False,
) -> str | None:
    if config.is_full_load or config.table_type == TABLE_TYPE_MASTER_DATA:
        return config.custom_where_clause

    if config.table_type in (TABLE_TYPE_DEPENDENT, TABLE_TYPE_BRIDGE):
        return build_relation_where_clause(
            config, partition_key, source_system, is_scheduled_catchup, is_first_partition
        )

    if config.partition_columns:
        date_filters = []
        for col in config.partition_columns:
            filter_clause = build_single_column_date_filter(
                col,
                partition_key,
                config.is_string_date,
                is_scheduled_catchup,
                is_first_partition,
            )
            date_filters.append(f"({filter_clause})")

        date_filter = " OR ".join(date_filters)
        if config.custom_where_clause:
            return f"({date_filter}) AND ({config.custom_where_clause})"
        return date_filter

    if not config.partition_column:
        return config.custom_where_clause

    date_filter = build_single_column_date_filter(
        config.partition_column,
        partition_key,
        config.is_string_date,
        is_scheduled_catchup,
        is_first_partition,
    )
    if config.custom_where_clause:
        return f"({date_filter}) AND ({config.custom_where_clause})"
    return date_filter


# ---------------------------------------------------------------------------
# Config-driven FK mapping for dependent/bridge tables
# ---------------------------------------------------------------------------


def build_relation_where_clause(
    config: TableConfig,
    partition_key: str,
    source_system: str,
    is_scheduled_catchup: bool = False,
    is_first_partition: bool = False,
) -> str | None:
    """Config-driven FK mapping for dependent/bridge tables."""
    if config.is_full_load:
        return config.custom_where_clause

    fk_column = config.fk_column
    parent_partition_col = config.parent_partition_col
    parent_fk_column = config.parent_fk_column
    fk_cast = config.fk_cast
    fk_path = config.fk_path
    parent_table = config.parent_table

    if not fk_column or not parent_partition_col:
        raise ValueError(
            f"TableConfig for dependent/bridge table "
            f"'{config.source_schema}.{config.source_table}' is missing required FK mapping "
            f"fields. Set fk_column and parent_partition_col on the TableConfig. "
            f"Got fk_column={fk_column!r}, parent_partition_col={parent_partition_col!r}."
        )

    date_filter = _build_parent_date_filter(
        parent_partition_col,
        partition_key,
        source_system,
        is_scheduled_catchup,
        is_first_partition,
    )

    parent_schema = _infer_parent_schema(config, source_system)
    parent_fk = parent_fk_column or fk_column

    if fk_path:
        path_entry = fk_path[0]
        bridge_table = path_entry[0]
        bridge_schema = path_entry[1]
        bridge_join_col = path_entry[2]
        bridge_fk_col = path_entry[3] if len(path_entry) > 3 else "FinansposteringId"
        parent_pk = parent_fk_column or bridge_fk_col

        outer_ref = f"[{config.source_schema}].[{config.source_table}].{fk_column}"

        return (
            f"EXISTS ("
            f"SELECT 1 "
            f"FROM [{bridge_schema}].[{bridge_table}] _bridge "
            f"INNER JOIN [{parent_schema}].[{parent_table}] _parent "
            f"ON {_cast_col('_bridge.' + bridge_fk_col, fk_cast)} "
            f"= {_cast_col('_parent.' + parent_pk, fk_cast)} "
            f"WHERE {_cast_col('_bridge.' + bridge_join_col, None)} "
            f"= {_cast_col(outer_ref, None)} "
            f"AND {_qualify_date_filter(date_filter, '_parent')}"
            f")"
        )
    else:
        if fk_cast:
            return (
                f"CAST({fk_column} AS {fk_cast}) IN ("
                f"SELECT CAST({parent_fk} AS {fk_cast}) "
                f"FROM [{parent_schema}].[{parent_table}] "
                f"WHERE {date_filter}"
                f")"
            )
        return (
            f"{fk_column} IN ("
            f"SELECT {parent_fk} FROM [{parent_schema}].[{parent_table}] "
            f"WHERE {date_filter}"
            f")"
        )


def _qualify_date_filter(date_filter: str, alias: str) -> str:
    """Prefix bare column references in date_filter with a table alias.

    Handles single-part (daily) and multi-part (monthly ``… AND …``) filters.
    """
    parts = date_filter.split(" AND ")
    return " AND ".join(_qualify_single_date_part(p.strip(), alias) for p in parts)


def _qualify_single_date_part(part: str, alias: str) -> str:
    if part.startswith("CAST("):
        return part.replace("CAST(", f"CAST({alias}.", 1)
    if part.startswith("["):
        return part.replace("[", f"{alias}.[", 1)
    col_end = min(
        (part.index(c) for c in (" ", "=", ">", "<") if c in part),
        default=len(part),
    )
    col_name = part[:col_end]
    return f"{alias}.{col_name}{part[col_end:]}"


def _cast_col(col: str, fk_cast: str | None) -> str:
    if fk_cast:
        return f"CAST({col} AS {fk_cast})"
    return col


def _infer_parent_schema(config: TableConfig, source_system: str) -> str:
    if source_system == "SydAX":
        return "Finans"
    elif source_system == "MySupply":
        return "dbo"
    return config.source_schema


def _build_parent_date_filter(
    parent_partition_col: str,
    partition_key: str,
    source_system: str,
    is_scheduled_catchup: bool = False,
    is_first_partition: bool = False,
) -> str:
    is_string_date = source_system == "MySupply"

    if is_monthly_partition(partition_key):
        start_date, end_date = get_month_date_range(partition_key)
        if is_first_partition:
            if is_string_date:
                return f"{parent_partition_col} <= '{end_date}'"
            return f"CAST({parent_partition_col} AS DATE) <= '{end_date}'"
        if is_string_date:
            return (
                f"{parent_partition_col} >= '{start_date}' AND "
                f"{parent_partition_col} <= '{end_date}'"
            )
        return (
            f"CAST({parent_partition_col} AS DATE) >= '{start_date}' AND "
            f"CAST({parent_partition_col} AS DATE) <= '{end_date}'"
        )

    if is_scheduled_catchup:
        today_date = datetime.strptime(partition_key, "%Y-%m-%d")
        yesterday_date = today_date - timedelta(days=1)
        if is_string_date:
            return f"{parent_partition_col} >= '{yesterday_date.strftime('%Y-%m-%d')}'"
        start_ts = yesterday_date.strftime("%Y-%m-%d") + " 01:00:00"
        return f"[{parent_partition_col}] >= '{start_ts}'"

    if is_first_partition:
        if is_string_date:
            return f"{parent_partition_col} <= '{partition_key}'"
        return f"CAST({parent_partition_col} AS DATE) <= '{partition_key}'"

    if is_string_date:
        return f"{parent_partition_col} = '{partition_key}'"
    return f"CAST({parent_partition_col} AS DATE) = '{partition_key}'"


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


def get_table_columns_info(
    source_conn,
    config: TableConfig,
) -> tuple[list[tuple[str, Any, Any, Any, Any, Any]], list[str]]:
    """Query INFORMATION_SCHEMA for source table columns.

    Returns ``(columns_info, column_names)``.
    """
    schema_query = f"""
    SELECT
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        IS_NULLABLE,
        COLUMN_DEFAULT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{config.source_table}'
    AND TABLE_SCHEMA = '{config.source_schema}'
    ORDER BY ORDINAL_POSITION
    """
    info_cursor = source_conn.cursor()
    try:
        info_cursor.execute(schema_query)
        schema_results = info_cursor.fetchall()
    finally:
        info_cursor.close()

    if not schema_results:
        raise ValueError(
            f"No columns found for table {config.source_schema}.{config.source_table}. "
            "Check that the table exists and the schema name is correct."
        )

    columns_info = []
    for row in schema_results:
        columns_info.append(
            (
                row[0],
                row[1],
                row[5] == "YES",
                row[2],
                row[3],
                row[4],
            )
        )
    column_names = [row[0] for row in schema_results]
    return columns_info, column_names


# ---------------------------------------------------------------------------
# Consistency checking
# ---------------------------------------------------------------------------


def get_source_row_count(source_conn, config: TableConfig) -> int:
    cursor = source_conn.cursor()
    try:
        query = f"SELECT COUNT(*) FROM [{config.source_schema}].[{config.source_table}]"
        cursor.execute(query)
        count = cursor.fetchone()[0]
    finally:
        cursor.close()
    return count


def get_target_row_count(target_conn, config: TableConfig) -> int:
    cursor = target_conn.cursor()
    try:
        query = f"SELECT COUNT(*) FROM [ext].[{config.target_table}]"
        cursor.execute(query)
        count = cursor.fetchone()[0]
    except Exception:
        count = -1
    finally:
        cursor.close()
    return count


def compute_column_hash(conn, config: TableConfig, is_source: bool = True) -> str | None:
    if not config.consistency_columns:
        return None

    cursor = conn.cursor()
    columns_concat = " + '|' + ".join(
        f"ISNULL(CAST([{col}] AS NVARCHAR(MAX)), '')" for col in config.consistency_columns
    )

    table_ref = (
        f"[{config.source_schema}].[{config.source_table}]"
        if is_source
        else f"[ext].[{config.target_table}]"
    )
    query = f"""
        SELECT CHECKSUM_AGG(CHECKSUM({columns_concat}))
        FROM {table_ref}
    """
    try:
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return str(result) if result is not None else "0"
    except Exception:
        return None
    finally:
        cursor.close()


def check_data_consistency(
    context: AssetExecutionContext,
    source_conn,
    target_conn,
    config: TableConfig,
) -> bool:
    source_count = get_source_row_count(source_conn, config)
    target_count = get_target_row_count(target_conn, config)

    context.log.info(
        f"Consistency check for {config.target_table}: "
        f"source={source_count} rows, target={target_count} rows"
    )

    if target_count < 0:
        context.log.info(f"Target table ext.{config.target_table} does not exist - loading")
        return False

    if source_count != target_count:
        context.log.info(
            f"Row count mismatch for {config.target_table}: "
            f"source={source_count}, target={target_count} - reloading"
        )
        return False

    if not config.consistency_columns:
        context.log.info(
            f"Row counts match for {config.target_table} "
            f"(no consistency columns defined) - skipping load"
        )
        return True

    source_hash = compute_column_hash(source_conn, config, is_source=True)
    target_hash = compute_column_hash(target_conn, config, is_source=False)
    context.log.debug(f"Hash comparison: source={source_hash}, target={target_hash}")
    if source_hash != target_hash:
        context.log.info(f"Data hash mismatch for {config.target_table} - reloading")
        return False

    context.log.info(f"Data unchanged for {config.target_table} - skipping load")
    return True
