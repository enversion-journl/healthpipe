# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Oracle extraction models and utilities for IFDB DSI layer.

This module provides shared functionality for extracting data from IFDB Oracle
database with support for daily, monthly, and full load partitions.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import dagster as dg
import oracledb
from core.extract import ExtractBatchingPolicy, iter_cursor_fetchmany
from core.partitions import is_first_partition as _is_first_partition
from DSA.FHIR.utils.partition_utils import get_month_date_range


@dataclass(frozen=True)
class TableExtractionConfig:
    """Configuration for extracting data from an IFDB Oracle table.

    Attributes:
        table: Fully qualified table name (e.g., "IFDB.ENCOUNTER")
        columns: List of column names to extract
        partition_column: Column to use for partition filtering
            (e.g., "EPJI_MODIFICATION_DATETIME").
            If None, no date filtering is applied (useful for full loads or non-temporal tables).
        description: Human-readable description of the extraction
        additional_joins: Optional JOIN clauses to add to the query
        additional_where: Optional additional WHERE conditions (without WHERE keyword)
    """

    table: str
    columns: list[str]
    partition_column: str | None
    description: str = ""
    additional_joins: str = ""
    additional_where: str = ""
    batching: ExtractBatchingPolicy | None = None


def build_partition_query(
    config: TableExtractionConfig,
    partition_key: str,
    partition_type: str,
    is_scheduled_catchup: bool = False,
    is_first_partition: bool = False,
) -> str:
    """
    Build SQL query with partition filtering for IFDB Oracle.

    Args:
        config: Table extraction configuration
        partition_key: The partition key value (date string or "full_load")
        partition_type: One of "daily", "monthly", or "full_load"
        is_scheduled_catchup: Whether to use the catchup window (Yesterday 06:00 to Today 06:00)
        is_first_partition: When True, only apply an upper bound so that all historical
            data before the configured start_date is included.

    Returns:
        Complete SQL query string

    Raises:
        ValueError: If partition_type is invalid
    """
    columns_str = ", ".join(config.columns)
    base_query = f"SELECT {columns_str} FROM {config.table}"

    if config.additional_joins:
        base_query = f"{base_query} {config.additional_joins}"

    where_clauses: list[str] = []

    col = config.partition_column

    if partition_type in ("bulk", "full_load") or col is None:
        pass
    elif partition_type == "daily":
        if is_scheduled_catchup:
            today_date = datetime.strptime(partition_key, "%Y-%m-%d")
            yesterday_date = today_date - timedelta(days=1)
            start_ts = yesterday_date.strftime("%Y-%m-%d") + " 05:00:00"
            where_clauses.append(f"{col} > TO_DATE('{start_ts}', 'YYYY-MM-DD HH24:MI:SS')")
        elif is_first_partition:
            next_day = (datetime.strptime(partition_key, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
            where_clauses.append(f"{col} < TO_DATE('{next_day}', 'YYYY-MM-DD')")
        else:
            next_day = (datetime.strptime(partition_key, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
            where_clauses.append(
                f"{col} >= TO_DATE('{partition_key}', 'YYYY-MM-DD') "
                f"AND {col} < TO_DATE('{next_day}', 'YYYY-MM-DD')"
            )
    elif partition_type == "monthly":
        start_date, end_date = get_month_date_range(partition_key)
        next_day_after_end = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        if is_first_partition:
            where_clauses.append(f"{col} < TO_DATE('{next_day_after_end}', 'YYYY-MM-DD')")
        else:
            where_clauses.append(
                f"{col} >= TO_DATE('{start_date}', 'YYYY-MM-DD') "
                f"AND {col} < TO_DATE('{next_day_after_end}', 'YYYY-MM-DD')"
            )
    else:
        raise ValueError(
            f"Invalid partition type: {partition_type}. "
            "Expected 'daily', 'monthly', or 'full_load'"
        )

    if config.additional_where:
        where_clauses.append(config.additional_where)

    if where_clauses:
        where_str = " AND ".join(f"({clause})" for clause in where_clauses)
        base_query = f"{base_query} WHERE {where_str}"

    return base_query


def fetch_rows_as_dicts(
    cursor, context: dg.AssetExecutionContext | None = None
) -> list[dict[str, Any]]:
    """
    Fetch all rows from an Oracle cursor as dictionaries, handling LOB objects.

    This function must be called while the database connection is still open,
    as LOB objects can only be read while the connection is active.

    Args:
        cursor: Oracle cursor object with executed query
        context: Optional AssetExecutionContext for logging warnings

    Returns:
        List of dictionaries where each dict represents a row with column names as keys

    Note:
        LOB objects (CLOB, BLOB) are read immediately and converted to strings/bytes.
        Failed LOB reads are logged as warnings and set to None.
    """
    columns = [desc[0] for desc in cursor.description]
    return [_row_as_dict(columns, row, context) for row in cursor.fetchall()]


def iter_rows_as_dict_batches(
    cursor,
    batch_size: int,
    context: dg.AssetExecutionContext | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Yield Oracle rows as dict batches, converting LOB values while connected."""

    columns = [desc[0] for desc in cursor.description]
    for batch in iter_cursor_fetchmany(cursor, batch_size):
        yield [_row_as_dict(columns, row, context) for row in batch]


def _row_as_dict(
    columns: list[str],
    row,
    context: dg.AssetExecutionContext | None = None,
) -> dict[str, Any]:
    row_dict = {}
    for col_name, value in zip(columns, row, strict=False):
        row_dict[col_name] = _convert_oracle_value(col_name, value, context)
    return row_dict


def _convert_oracle_value(
    col_name: str,
    value,
    context: dg.AssetExecutionContext | None = None,
) -> Any:
    if value is not None and isinstance(value, oracledb.LOB):
        try:
            return value.read()
        except Exception as e:
            if context:
                context.log.warning(f"Failed to read LOB value for column {col_name}: {e}")
            return None
    return value


def stringify_oracle_value(value) -> str | None:
    """Stringify an Oracle value, normalizing integer-valued numerics.

    Oracle NUMBER columns may arrive as:
      - Python float:   float(99276330680.0)
      - decimal.Decimal: Decimal('99276330680.0')
      - str after LOB read or driver conversion: '99276330680.0'

    All must be normalized to "99276330680" to avoid polluting NVARCHAR
    ID columns and breaking downstream identifier matching.
    """
    from decimal import Decimal

    if value is None:
        return None
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(value.to_integral_value())
        return str(value)
    s = str(value)
    if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        return s[:-2]
    return s


def execute_extraction(
    context: dg.AssetExecutionContext,
    ifdb_oracle,
    config: TableExtractionConfig,
    partition_key: str,
    partition_type: str,
) -> list[dict[str, Any]]:
    """
    Execute a full extraction from IFDB Oracle.

    Args:
        context: Dagster execution context
        ifdb_oracle: IFDBOracleResource instance
        config: Table extraction configuration
        partition_key: The partition key value
        partition_type: One of "daily", "monthly", or "full_load"

    Returns:
        List of row dictionaries
    """
    is_scheduled_catchup = False
    if (
        context.has_tag("ifdb_extraction_mode")
        and context.run.tags.get("ifdb_extraction_mode") == "scheduled_catchup"
    ):
        is_scheduled_catchup = True
        context.log.info(
            "Running in SCHEDULED CATCHUP mode: "
            "Extracting window from Yesterday 05:00 to Today 06:00."
        )

    first_partition = _is_first_partition(partition_key)
    if first_partition:
        context.log.info(
            f"First partition detected ({partition_key}): using upper-bound-only filtering "
            "to include all historical data."
        )

    query = build_partition_query(
        config, partition_key, partition_type, is_scheduled_catchup, first_partition
    )

    # Extract entity name from table (e.g., "IFDB.PATIENT_OPLYSNINGER_COLUMNA" -> "patient")
    table_name = config.table.split(".")[-1].lower()
    entity_name = table_name.split("_")[0] if "_" in table_name else table_name

    context.log.info(f"Executing extraction query for {config.table}")
    context.log.debug(f"Query: {query}")

    rows: list[dict[str, Any]] = []
    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            rows = fetch_rows_as_dicts(cursor, context)
        finally:
            cursor.close()

    # Enhanced logging with structured output
    context.log.info(
        f"[IFDB EXTRACTION] Retrieved {len(rows)} {entity_name} records from {config.table} "
        f"(partition: {partition_type}, key: {partition_key})"
    )

    return rows
