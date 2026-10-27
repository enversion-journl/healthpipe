# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""MssqlExtractor — MSSQL-specific extraction and loading for ERP source systems.

Extracts data from MSSQL databases (ILS, SydAX, MySupply) and loads into
``ext.*`` staging tables on the target data warehouse.  Supports three insert
strategies: mssql-python bulk-copy, mssql-python executemany, and pymssql
executemany.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from core.partitions import is_first_partition as _is_first_partition
from dagster import AssetExecutionContext
from DSI.connectors.base import ExtractResult, SqlExtractor
from DSI.connectors.mssql.bulk_copy import (
    build_bcp_truncation_limits,
    bulkcopy_insert_with_retry,
    compute_streaming_chunk_size,
    normalize_decimals_for_bcp,
    refresh_bulkcopy_conn,
    truncate_for_bcp,
)
from DSI.connectors.mssql.models import (
    TABLE_TYPE_MASTER_DATA,
    TableConfig,
    TableLoadResult,
)
from DSI.connectors.mssql.sql_builders import (
    build_partition_where_clause,
    build_relation_where_clause,
    check_data_consistency,
    get_table_columns_info,
    is_monthly_partition,
)


def _is_mssql_python_conn(conn) -> bool:
    """Return True if *conn* was created by the mssql-python driver."""
    return type(conn).__module__.startswith("mssql_python")


# mssql_python DDBC SQL type constants used by cursor.setinputsizes()
_SQL_WVARCHAR = -9
_SQL_TYPE_TIMESTAMP = 93
_SQL_TYPE_DATE = 91

# Per-chunk log throttle: log the first chunk, every Nth chunk (heartbeat), and
# any chunk whose fetch or insert exceeds the slow threshold. Env-tunable;
# EXT_LOG_CHUNK_INTERVAL=1 restores full per-chunk logging for debugging.
_LOG_CHUNK_INTERVAL = int(os.getenv("EXT_LOG_CHUNK_INTERVAL", "100"))
_SLOW_CHUNK_SECONDS = float(os.getenv("EXT_SLOW_CHUNK_SECONDS", "5.0"))


def _should_log_chunk(
    chunk_num: int,
    fetch_elapsed: float,
    insert_elapsed: float,
    interval: int | None = None,
    slow_seconds: float | None = None,
) -> bool:
    """Throttle per-chunk logging: first chunk, periodic heartbeat, or slow chunk."""
    interval = _LOG_CHUNK_INTERVAL if interval is None else interval
    slow_seconds = _SLOW_CHUNK_SECONDS if slow_seconds is None else slow_seconds
    return (
        chunk_num == 1
        or chunk_num % interval == 0
        or fetch_elapsed >= slow_seconds
        or insert_elapsed >= slow_seconds
    )


@dataclass
class BulkCopyStreamStats:
    """Aggregate stats from one bulk-copy streaming run."""

    rows: int = 0
    truncations: int = 0
    normalizations: int = 0
    t_fetch: float = 0.0
    t_prep: float = 0.0
    t_insert: float = 0.0


def stream_bulkcopy_chunks(
    *,
    context,
    source_cursor,
    bc_conn,
    target_resource,
    config,
    column_names: list[str],
    bcp_limits: list,
    chunk_size: int,
    source_system: str,
    load_timestamp: datetime,
    partition_date: date | None,
    table_lock: bool = True,
    refresh_interval: int = 100,
    label: str | None = None,
    transform_chunk=None,
    on_insert_error=None,
) -> BulkCopyStreamStats:
    """Stream a result set into the bulk-copy target, chunk by chunk.

    The shared core of the serial loader (``MssqlExtractor._stream_via_bulkcopy``)
    and the parallel windowed extractor (``parallel_extract._extract_window``):
    fetchmany -> optional per-chunk transform -> append the 3 audit columns ->
    truncate/normalize -> ``bulkcopy_insert_with_retry``, refreshing the bulk-copy
    connection every ``refresh_interval`` chunks and logging throttled progress.

    Callers own ``chunk_size``/``bcp_limits`` computation and supply the diverging
    bits as callbacks:
    - ``transform_chunk(chunk, description) -> rows`` for ILS cleaning / column
      filtering (default: identity).
    - ``on_insert_error(exc, data, chunk_offset) -> Exception`` to wrap an insert
      failure (e.g. column-level diagnosis); default re-raises unchanged.

    ``context`` is anything exposing ``.log.info/.warning`` (the Dagster context,
    or a counting wrapper). Does NOT close ``source_cursor`` (the caller owns it).
    """
    stats = BulkCopyStreamStats()
    open_contexts: list = []
    chunk_num = 0
    try:
        while True:
            if chunk_num > 0 and chunk_num % refresh_interval == 0 and target_resource is not None:
                bc_conn = refresh_bulkcopy_conn(
                    context,
                    bc_conn,
                    target_resource,
                    label=f"[{label or config.target_table}] chunk {chunk_num} refresh "
                    f"(every {refresh_interval})",
                    open_contexts=open_contexts,
                )

            t0 = time.perf_counter()
            chunk = source_cursor.fetchmany(chunk_size)
            fetch_elapsed = time.perf_counter() - t0
            stats.t_fetch += fetch_elapsed
            if not chunk:
                break

            t0 = time.perf_counter()
            if transform_chunk is not None:
                chunk = transform_chunk(chunk, source_cursor.description)
            data = [tuple(row) + (source_system, load_timestamp, partition_date) for row in chunk]
            del chunk
            stats.truncations += truncate_for_bcp(data, bcp_limits, context)
            stats.normalizations += normalize_decimals_for_bcp(data)
            stats.t_prep += time.perf_counter() - t0

            t0 = time.perf_counter()
            try:
                inserted, bc_conn = bulkcopy_insert_with_retry(
                    context,
                    bc_conn,
                    config,
                    column_names,
                    data,
                    mssql_target_resource=target_resource,
                    table_lock=table_lock,
                    open_contexts=open_contexts,
                )
            except Exception as exc:
                if on_insert_error is not None:
                    raise on_insert_error(exc, data, stats.rows) from exc
                raise
            insert_elapsed = time.perf_counter() - t0
            stats.t_insert += insert_elapsed
            stats.rows += inserted
            chunk_num += 1
            del data

            if _should_log_chunk(chunk_num, fetch_elapsed, insert_elapsed):
                slow = (
                    " [SLOW]" if max(fetch_elapsed, insert_elapsed) >= _SLOW_CHUNK_SECONDS else ""
                )
                if label is None:
                    context.log.info(
                        f"Chunk {chunk_num}: {stats.rows:,} rows so far | "
                        f"fetch={fetch_elapsed:.1f}s insert={insert_elapsed:.1f}s{slow}"
                    )
                else:
                    context.log.info(
                        f"[{label}] chunk {chunk_num}: {stats.rows:,} rows | "
                        f"fetch={fetch_elapsed:.1f}s insert={insert_elapsed:.1f}s{slow}"
                    )
    finally:
        for ctx in open_contexts:
            try:
                ctx.__exit__(None, None, None)
            except Exception:
                pass
    if stats.normalizations > 0:
        context.log.info(
            f"Normalized {stats.normalizations:,} Decimal value(s) across {chunk_num} chunk(s) "
            f"for BCP compatibility"
        )
    return stats


def _build_input_sizes(
    columns_info: list[tuple[str, Any, Any, Any, Any, Any]] | None,
    num_data_columns: int,
    sample_row: tuple | None = None,
) -> list[tuple[int, int, int]]:
    """Build a setinputsizes spec for mssql_python executemany.

    The mssql_python driver infers parameter widths from the first row of data
    passed to ``executemany``.  When a later row contains a longer value, the
    driver raises ``RuntimeError: Input string UTF-16 length exceeds allowed
    column size``.  Calling ``cursor.setinputsizes()`` with explicit sizes
    that match the ext table DDL avoids this.

    When ``columns_info`` has no type for a column (``col_type is None``),
    ``sample_row`` is inspected to pick the right ODBC binding — in particular
    to distinguish date/datetime (which require SQL_TYPE_DATE/TIMESTAMP and
    native Python objects) from strings/ints (which use WVARCHAR).

    Returns a list of ``(sql_type, column_size, decimal_digits)`` tuples — one
    per parameter — covering all data columns plus the three audit columns
    (SourceSystem, dw_load_timestamp, dw_partition_date).
    """
    sizes: list[tuple[int, int, int]] = []

    for idx in range(num_data_columns):
        col_type = None
        col_size = None
        col_precision = None
        col_scale = None
        if columns_info and idx < len(columns_info):
            _, col_type, _, col_size, col_precision, col_scale = columns_info[idx]

        if isinstance(col_type, str) and col_type:
            type_lower = col_type.lower()
            if type_lower in ("bigint", "int", "smallint", "tinyint", "bit"):
                sizes.append((_SQL_WVARCHAR, 255, 0))
            elif type_lower in ("decimal", "numeric", "money", "smallmoney", "float", "real"):
                sizes.append((_SQL_WVARCHAR, 255, 0))
            elif type_lower in ("datetime", "datetime2", "smalldatetime", "datetimeoffset"):
                sizes.append((_SQL_TYPE_TIMESTAMP, 0, 0))
            elif type_lower == "date":
                sizes.append((_SQL_TYPE_DATE, 0, 0))
            elif type_lower in ("nvarchar", "nchar", "ntext"):
                width = col_size if col_size and 0 < col_size <= 4000 else 4000
                sizes.append((_SQL_WVARCHAR, width, 0))
            elif type_lower in ("varchar", "char", "text"):
                width = col_size if col_size and 0 < col_size <= 8000 else 4000
                sizes.append((_SQL_WVARCHAR, width, 0))
            else:
                sizes.append((_SQL_WVARCHAR, 255, 0))
        else:
            sample_val = (
                sample_row[idx] if sample_row is not None and idx < len(sample_row) else None
            )
            if sample_val is not None and isinstance(sample_val, datetime):
                sizes.append((_SQL_TYPE_TIMESTAMP, 0, 0))
            elif sample_val is not None and isinstance(sample_val, date):
                sizes.append((_SQL_TYPE_DATE, 0, 0))
            else:
                sizes.append((_SQL_WVARCHAR, 255, 0))

    sizes.append((_SQL_WVARCHAR, 50, 0))
    sizes.append((_SQL_TYPE_TIMESTAMP, 0, 0))
    sizes.append((_SQL_TYPE_DATE, 0, 0))

    return sizes


_NUMERIC_SQL_TYPES = frozenset(
    {
        "bigint",
        "int",
        "smallint",
        "tinyint",
        "decimal",
        "numeric",
        "money",
        "smallmoney",
        "float",
        "real",
        "bit",
    }
)

_INTEGER_SQL_TYPES = frozenset({"bigint", "int", "smallint", "tinyint"})

_INTEGER_RANGES: dict[str, tuple[int, int]] = {
    "bigint": (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
    "int": (-2_147_483_648, 2_147_483_647),
    "smallint": (-32_768, 32_767),
    "tinyint": (0, 255),
}

_DATE_SQL_TYPES = frozenset(
    {"date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time"}
)


def _is_valid_for_type(
    value: Any,
    sql_type: str | None,
    col_precision: int | None = None,
    col_scale: int | None = None,
) -> bool:
    """Check whether *value* is plausibly convertible to *sql_type* by SQL Server."""
    if value is None:
        return True
    if not sql_type:
        return True

    type_lower = sql_type.lower()

    if type_lower in _NUMERIC_SQL_TYPES:
        if isinstance(value, bool):
            return type_lower == "bit"
        if isinstance(value, int | float | Decimal):
            if type_lower in _INTEGER_SQL_TYPES:
                try:
                    int_val = int(value)
                    lo, hi = _INTEGER_RANGES.get(type_lower, (_INTEGER_RANGES["bigint"]))
                    return lo <= int_val <= hi
                except (ValueError, OverflowError):
                    return False
            return True
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return True
            if type_lower in _INTEGER_SQL_TYPES:
                try:
                    d = Decimal(cleaned)
                    int_val = int(d)
                    if d != int_val and type_lower != "bigint":
                        return False
                    lo, hi = _INTEGER_RANGES.get(type_lower, _INTEGER_RANGES["bigint"])
                    return lo <= int_val <= hi
                except Exception:
                    return False
            if type_lower in ("decimal", "numeric"):
                try:
                    d = Decimal(cleaned)
                    if "e" in cleaned.lower():
                        return False
                    if col_precision is not None:
                        sign, digits, exp = d.as_tuple()
                        num_digits = len(digits)
                        exp = cast(int, exp)
                        scale = -exp if exp < 0 else 0
                        integer_digits = num_digits - scale
                        allowed_integer = col_precision - (col_scale or 0)
                        if integer_digits > allowed_integer:
                            return False
                    return True
                except Exception:
                    return False
            try:
                Decimal(cleaned)
                if "e" in cleaned.lower():
                    return False
                return True
            except Exception:
                return False
        return False

    if type_lower in _DATE_SQL_TYPES:
        if isinstance(value, date | datetime):
            return True
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    datetime.strptime(value.strip(), fmt)
                    return True
                except ValueError:
                    continue
            return False
        return False

    return True


def _get_table_column_types(target_conn, table_name: str) -> dict[str, str] | None:
    """Read exact column DDL types from the ext table via INFORMATION_SCHEMA."""
    try:
        cursor = target_conn.cursor()
        try:
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
                "NUMERIC_PRECISION, NUMERIC_SCALE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = 'ext' AND TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                (table_name,),
            )
            result = {}
            for row in cursor.fetchall():
                col_name, data_type, char_len, num_prec, num_scale = row
                if data_type in ("decimal", "numeric"):
                    result[col_name] = f"{data_type.upper()}({num_prec},{num_scale})"
                elif data_type in ("nvarchar", "varchar") and char_len:
                    if char_len == -1:
                        result[col_name] = f"{data_type.upper()}(MAX)"
                    else:
                        result[col_name] = f"{data_type.upper()}({char_len})"
                elif data_type == "datetime2":
                    result[col_name] = f"DATETIME2({num_scale})" if num_scale else "DATETIME2"
                else:
                    result[col_name] = data_type.upper()
            return result if result else None
        finally:
            cursor.close()
    except Exception:
        return None


def _probe_row_with_try_convert(
    target_conn,
    table_name: str,
    column_ddl_types: dict[str, str],
    all_column_names: list[str],
    row: tuple,
) -> list[tuple[str, str, Any]]:
    """Use SQL Server TRY_CONVERT to find which columns in *row* fail conversion.

    Returns a list of (column_name, ddl_type, value) for each failing column.
    """
    failures: list[tuple[str, str, Any]] = []
    try:
        cursor = target_conn.cursor()
        try:
            for col_idx, col_name in enumerate(all_column_names):
                if col_idx >= len(row):
                    break
                value = row[col_idx]
                if value is None:
                    continue
                ddl_type = column_ddl_types.get(col_name)
                if not ddl_type:
                    continue
                base_type = ddl_type.split("(")[0].lower()
                if base_type in ("nvarchar", "varchar", "nchar", "char", "ntext", "text", "xml"):
                    continue

                str_value = str(value)
                try:
                    cursor.execute(
                        f"SELECT CASE WHEN TRY_CONVERT({ddl_type}, ?) IS NULL "
                        f"THEN 0 ELSE 1 END",
                        (str_value,),
                    )
                    result = cursor.fetchone()
                    if result and result[0] == 0:
                        failures.append((col_name, ddl_type, value))
                except Exception:
                    failures.append((col_name, ddl_type, value))
        finally:
            cursor.close()
    except Exception:
        pass
    return failures


def _find_bad_row_by_binary_search(
    target_conn,
    insert_sql: str,
    data: list[tuple],
    input_sizes: list[tuple[int, int, int]] | None,
) -> int | None:
    """Binary search to find the first row that causes an insert failure.

    Tries inserting shrinking slices until a single failing row is isolated.
    Returns the index within *data*, or None if it cannot be determined.
    """
    if len(data) <= 1:
        return 0 if data else None

    lo, hi = 0, len(data)
    last_known_bad = None

    for _ in range(30):
        if hi - lo <= 1:
            last_known_bad = lo
            break
        mid = (lo + hi) // 2
        if _try_insert_slice(target_conn, insert_sql, data[lo:mid], input_sizes):
            lo = mid
        else:
            last_known_bad = lo
            hi = mid

    return last_known_bad


def _try_insert_slice(
    target_conn,
    insert_sql: str,
    rows: list[tuple],
    input_sizes: list[tuple[int, int, int]] | None,
) -> bool:
    """Try inserting *rows* inside a transaction that is always rolled back.

    Returns True if the insert succeeds (no conversion error).
    """
    if not rows:
        return True
    try:
        cursor = target_conn.cursor()
        try:
            if input_sizes and _is_mssql_python_conn(target_conn):
                cursor.setinputsizes(input_sizes)
            cursor.execute("BEGIN TRANSACTION")
            try:
                cursor.executemany(insert_sql, rows)
                return True
            except Exception:
                return False
            finally:
                try:
                    cursor.execute("ROLLBACK TRANSACTION")
                except Exception:
                    pass
        finally:
            cursor.close()
    except Exception:
        return False


def _diagnose_insert_error(
    original_error: Exception,
    data: list[tuple],
    column_names: list[str],
    columns_info: list[tuple[str, Any, Any, Any, Any, Any]] | None,
    table_name: str,
    chunk_offset: int = 0,
    target_conn=None,
    insert_sql: str | None = None,
    input_sizes: list[tuple[int, int, int]] | None = None,
) -> Exception:
    """Scan *data* to identify which columns have bad values.

    Three-phase approach:
      1. Fast Python-side validation against columns_info.
      2. If nothing found and target_conn available, read actual DDL types from
         INFORMATION_SCHEMA and use TRY_CONVERT to probe individual columns.
      3. If TRY_CONVERT does not isolate the column, binary-search for the bad
         row then probe it column-by-column.

    Returns a RuntimeError whose message includes the original driver error
    plus a per-column diagnostic listing the offending rows and values.
    """
    all_column_names = list(column_names) + [
        "SourceSystem",
        "dw_load_timestamp",
        "dw_partition_date",
    ]
    audit_info: list[tuple[str, Any, Any, Any, Any, Any]] = [
        ("SourceSystem", "nvarchar", True, 50, None, None),
        ("dw_load_timestamp", "datetime2", True, None, None, None),
        ("dw_partition_date", "date", True, None, None, None),
    ]
    all_columns_info = list(columns_info or []) + audit_info

    # Phase 1: fast Python-side scan
    bad_columns: dict[str, list[tuple[int, Any]]] = {}
    max_rows_to_scan = min(len(data), 5000)
    for row_idx in range(max_rows_to_scan):
        row = data[row_idx]
        for col_idx, value in enumerate(row):
            if col_idx >= len(all_columns_info):
                break
            col_name = (
                all_column_names[col_idx] if col_idx < len(all_column_names) else f"col_{col_idx}"
            )
            _, sql_type, _, _, col_precision, col_scale = all_columns_info[col_idx]
            if not _is_valid_for_type(value, sql_type, col_precision, col_scale):
                bad_list = bad_columns.setdefault(col_name, [])
                if len(bad_list) < 5:
                    bad_list.append((chunk_offset + row_idx, value))

    # Phase 2 & 3: SQL Server-side probing when Python scan finds nothing
    column_ddl_types: dict[str, str] | None = None
    probed_row_idx: int | None = None
    probe_failures: list[tuple[str, str, Any]] = []

    if not bad_columns and target_conn is not None and data:
        column_ddl_types = _get_table_column_types(target_conn, table_name)

        if column_ddl_types:
            # Phase 2a: probe first row with TRY_CONVERT
            probe_failures = _probe_row_with_try_convert(
                target_conn,
                table_name,
                column_ddl_types,
                all_column_names,
                data[0],
            )

            if not probe_failures and insert_sql and len(data) > 1:
                # Phase 3: binary search for the bad row, then probe it
                bad_row_local = _find_bad_row_by_binary_search(
                    target_conn,
                    insert_sql,
                    data,
                    input_sizes,
                )
                if bad_row_local is not None and bad_row_local < len(data):
                    probed_row_idx = bad_row_local
                    probe_failures = _probe_row_with_try_convert(
                        target_conn,
                        table_name,
                        column_ddl_types,
                        all_column_names,
                        data[bad_row_local],
                    )

    # Build error message
    parts = [f"Insert into ext.{table_name} failed: {original_error}"]

    if bad_columns:
        parts.append(f"\n  Column-level diagnostics ({len(bad_columns)} suspect column(s)):")
        for col_name, samples in bad_columns.items():
            col_info_str = ""
            for ci in all_columns_info:
                if ci[0] == col_name:
                    col_info_str = f" (target type: {ci[1]})"
                    break
            sample_strs = [f"row {ridx}: {repr(val)}" for ridx, val in samples]
            parts.append(f"    [{col_name}]{col_info_str}: {', '.join(sample_strs)}")
    elif probe_failures:
        row_label = (
            f"row {chunk_offset + probed_row_idx}" if probed_row_idx is not None else "row 0"
        )
        parts.append(
            f"\n  SQL Server TRY_CONVERT diagnostics "
            f"({len(probe_failures)} failing column(s) in {row_label}):"
        )
        for col_name, ddl_type, value in probe_failures:
            parts.append(f"    [{col_name}] ({ddl_type}): {repr(value)}")
    else:
        if data:
            first_row = data[0]
            row_detail = ", ".join(
                f"{all_column_names[i] if i < len(all_column_names) else f'col_{i}'}="
                f"{repr(first_row[i])}"
                for i in range(min(len(first_row), len(all_column_names)))
            )
            parts.append(f"\n  First row: {row_detail}")
        parts.append("\n  Could not isolate a specific column. Check the data values above.")

    return RuntimeError("\n".join(parts))


def _detect_scheduled_catchup(context: AssetExecutionContext) -> bool:
    return (
        context.has_tag("erp_extraction_mode")
        and context.run.tags.get("erp_extraction_mode") == "scheduled_catchup"
    )


def _parse_partition_date(partition_key: str) -> date:
    if is_monthly_partition(partition_key):
        return date.fromisoformat(f"{partition_key}-01")
    return date.fromisoformat(partition_key)


def clean_ils_data(source_data: list, column_descriptions) -> list:
    """Clean ILS source data to fix data type conversion issues."""
    if not source_data:
        return source_data

    cleaned_data = []
    for row in source_data:
        cleaned_row = list(row)
        for col_idx, _ in enumerate(column_descriptions):
            value = cleaned_row[col_idx]
            if isinstance(value, str):
                if (
                    value.endswith(".0")
                    and value.replace(".", "")
                    .replace("-", "")
                    .replace("/", "")
                    .replace("+", "")
                    .replace(" ", "")
                    .isdigit()
                ):
                    try:
                        float_val = float(value)
                        if float_val == int(float_val):
                            without_decimal = value[:-2]
                            float(without_decimal)
                            cleaned_row[col_idx] = without_decimal
                    except (ValueError, IndexError):
                        pass
                elif value.replace(".", "").replace("-", "").isdigit() and value.count(".") == 1:
                    try:
                        float_val = float(value)
                        if float_val == int(float_val):
                            cleaned_row[col_idx] = int(float_val)
                    except ValueError:
                        pass
            elif isinstance(value, float) and value == int(value):
                cleaned_row[col_idx] = int(value)

        cleaned_data.append(tuple(cleaned_row))

    return cleaned_data


def map_sql_data_type_to_ddl(
    data_type: str,
    col_size: int | None,
    col_precision: int | None,
    col_scale: int | None,
    avoid_max_types: bool = False,
) -> str:
    data_type_lower = data_type.lower() if data_type else ""

    nvarchar_max = "NVARCHAR(4000)" if avoid_max_types else "NVARCHAR(MAX)"
    varchar_max = "VARCHAR(8000)" if avoid_max_types else "VARCHAR(MAX)"
    varbinary_max = "VARBINARY(8000)" if avoid_max_types else "VARBINARY(MAX)"

    if data_type_lower == "nvarchar":
        if col_size and 0 < col_size <= 4000:
            return f"NVARCHAR({col_size})"
        return nvarchar_max
    if data_type_lower == "varchar":
        if col_size and 0 < col_size <= 8000:
            return f"VARCHAR({col_size})"
        return varchar_max
    if data_type_lower == "nchar":
        return f"NCHAR({col_size})" if col_size else "NCHAR(1)"
    if data_type_lower == "char":
        return f"CHAR({col_size})" if col_size else "CHAR(1)"
    if data_type_lower in ("text", "ntext"):
        return nvarchar_max
    if data_type_lower == "bigint":
        return "BIGINT"
    if data_type_lower == "int":
        return "INT"
    if data_type_lower == "smallint":
        return "SMALLINT"
    if data_type_lower == "tinyint":
        return "TINYINT"
    if data_type_lower == "bit":
        return "BIT"
    if data_type_lower == "decimal":
        # Cap at 28: ODBC drivers (including msodbcsql) reliably handle up to
        # precision 28 with fast_executemany; precision 38 causes Numeric overflow HY000.
        precision = min(col_precision or 18, 28)
        scale = col_scale if col_scale is not None else 0
        return f"DECIMAL({precision},{scale})"
    if data_type_lower == "numeric":
        precision = min(col_precision or 18, 28)
        scale = col_scale if col_scale is not None else 0
        return f"NUMERIC({precision},{scale})"
    if data_type_lower == "money":
        return "MONEY"
    if data_type_lower == "smallmoney":
        return "SMALLMONEY"
    if data_type_lower == "float":
        return f"FLOAT({col_precision})" if col_precision else "FLOAT"
    if data_type_lower == "real":
        return "REAL"
    if data_type_lower == "datetime":
        if avoid_max_types:
            return "DATETIME2"
        return "DATETIME"
    if data_type_lower == "datetime2":
        return f"DATETIME2({col_scale})" if col_scale is not None else "DATETIME2"
    if data_type_lower == "smalldatetime":
        if avoid_max_types:
            return "DATETIME2"
        return "SMALLDATETIME"
    if data_type_lower == "date":
        return "DATE"
    if data_type_lower == "time":
        return f"TIME({col_scale})" if col_scale is not None else "TIME"
    if data_type_lower == "datetimeoffset":
        return f"DATETIMEOFFSET({col_scale})" if col_scale is not None else "DATETIMEOFFSET"
    if data_type_lower == "varbinary":
        if col_size and 0 < col_size <= 8000:
            return f"VARBINARY({col_size})"
        return varbinary_max
    if data_type_lower == "binary":
        return f"BINARY({col_size})" if col_size else "BINARY(1)"
    if data_type_lower == "image":
        return varbinary_max
    if data_type_lower == "uniqueidentifier":
        return "UNIQUEIDENTIFIER"
    if data_type_lower == "xml":
        return "XML"
    if data_type_lower == "sql_variant":
        return "SQL_VARIANT"
    return "NVARCHAR(255)"


def create_ext_table_ddl(
    table_name: str,
    columns_info: list[tuple[str, Any, Any, Any, Any, Any]],
    extra_columns: list[tuple[str, str, bool]],
    sample_types: list[type | None],
    avoid_max_types: bool = False,
) -> str:
    ddl_parts = [f"CREATE TABLE [ext].[{table_name}]("]
    column_defs: list[str] = []

    nvarchar_max = "NVARCHAR(4000)" if avoid_max_types else "NVARCHAR(MAX)"
    varbinary_max = "VARBINARY(8000)" if avoid_max_types else "VARBINARY(MAX)"

    for idx, (
        col_name,
        col_type,
        _col_nullable,
        col_size,
        col_precision,
        col_scale,
    ) in enumerate(columns_info):
        sample_type = sample_types[idx] if idx < len(sample_types) else None
        if isinstance(col_type, str) and col_type:
            sql_type = map_sql_data_type_to_ddl(
                col_type, col_size, col_precision, col_scale, avoid_max_types=avoid_max_types
            )
            if col_name == "FinansposteringId" and sql_type == "BIGINT":
                sql_type = "VARCHAR(50)"
        elif sample_type is not None:
            if issubclass(sample_type, bool):
                sql_type = "BIT"
            elif issubclass(sample_type, int):
                sql_type = "BIGINT"
            elif issubclass(sample_type, Decimal):
                precision = col_precision or 38
                scale = col_scale if col_scale is not None else min(10, precision - 1)
                sql_type = f"DECIMAL({precision},{scale})"
            elif issubclass(sample_type, float):
                sql_type = "FLOAT"
            elif issubclass(sample_type, datetime):
                sql_type = "DATETIME2"
            elif issubclass(sample_type, date):
                sql_type = "DATE"
            elif sample_type in (bytes, bytearray, memoryview):
                sql_type = varbinary_max
            else:
                max_len = col_size or 0
                column_name_lower = col_name.lower()
                is_description_column = any(
                    keyword in column_name_lower
                    for keyword in [
                        "navn",
                        "beskrivelse",
                        "tekst",
                        "description",
                        "name",
                        "text",
                        "item_",
                        "kunde",
                        "leverings",
                        "adresse",
                        "reference",
                    ]
                )
                if max_len <= 0:
                    sql_type = nvarchar_max if is_description_column else "NVARCHAR(1000)"
                elif max_len > 4000:
                    sql_type = nvarchar_max
                else:
                    min_size = 2000 if is_description_column else 255
                    sql_type = f"NVARCHAR({max(max_len, min_size)})"
        else:
            sql_type = "NVARCHAR(255)"

        column_defs.append(f"[{col_name}] {sql_type} NULL")

    for extra_name, extra_type, extra_nullable in extra_columns:
        nullable = "NULL" if extra_nullable else "NOT NULL"
        column_defs.append(f"[{extra_name}] {extra_type} {nullable}")

    ddl_parts.append(",\n    ".join(column_defs))
    ddl_parts.append(") ON [PRIMARY]")
    return "\n".join(ddl_parts)


class MssqlExtractor(SqlExtractor):
    """Extracts data from MSSQL databases (ILS, SydAX, MySupply).

    Extract returns ExtractResult with list[tuple] data. Insert handles DDL
    generation, chunked writes, metadata columns, and source-specific data
    cleaning (e.g., ILS numeric fixes).

    When ``bulkcopy_conn`` is provided (a ``mssql-python`` connection), the
    insert path uses the driver's native ``cursor.bulkcopy()`` API for
    significantly faster bulk loads.
    """

    def __init__(
        self,
        source_conn,
        target_conn=None,
        *,
        context: AssetExecutionContext,
        source_system: str,
        bulkcopy_conn=None,
        mssql_target_resource=None,
        bcp_max_chunk_size: int | None = None,
    ):
        super().__init__(source_conn, target_conn, context=context, source_system=source_system)
        self.bulkcopy_conn = bulkcopy_conn
        self.mssql_target_resource = mssql_target_resource
        # Optional upper bound on per-chunk row count for the bulk-copy path.
        # The default chunk size is computed from column widths; assets that
        # hit driver instability on large chunks (e.g. mssql-python's
        # cursor.bulkcopy() wedging on ~20k-row chunks in long extractions)
        # can cap it (e.g. 5k) without changing the global heuristic.
        self.bcp_max_chunk_size = bcp_max_chunk_size

    # ------------------------------------------------------------------
    # Extract (read-only, returns in-memory ExtractResult)
    # ------------------------------------------------------------------

    def extract(self, config, partition_key: str | None, partition_type: str) -> ExtractResult:
        is_scheduled_catchup = _detect_scheduled_catchup(self.context)
        if is_scheduled_catchup:
            self.context.log.info("Running in SCHEDULED CATCHUP mode: Extracting 25-hour window.")

        first_partition = bool(partition_key and _is_first_partition(partition_key))
        if first_partition:
            self.context.log.info(
                f"First partition detected ({partition_key}): using upper-bound-only filtering "
                "to include all historical data."
            )

        columns_info, column_names = get_table_columns_info(self.source_conn, config)

        where_clause = self._resolve_where_clause(
            config, partition_key, is_scheduled_catchup, first_partition
        )

        source_cursor = self.source_conn.cursor()
        try:
            self._execute_select(source_cursor, config, where_clause)

            if self.source_system == "ILS":
                column_names = [name.replace("__", "##") for name in column_names]

            filtered_indices = self._compute_filtered_indices(column_names, config)
            if filtered_indices is not None:
                column_names = [column_names[i] for i in filtered_indices]
                columns_info = [columns_info[i] for i in filtered_indices]

            all_rows = self._fetch_all_rows(source_cursor, filtered_indices)

            self.context.log.info(
                f"Extracted {len(all_rows)} rows from "
                f"{config.source_schema}.{config.source_table}"
            )

            return ExtractResult(
                data=all_rows,
                columns_info=columns_info,
                column_names=column_names,
                row_count=len(all_rows),
            )
        finally:
            source_cursor.close()

    # ------------------------------------------------------------------
    # Insert (write extracted data into ext.* table)
    # ------------------------------------------------------------------

    def insert(self, config, extract_result: ExtractResult, partition_key: str | None) -> int:
        if self.target_conn is None:
            raise ValueError("MssqlExtractor.insert() requires a target_conn")

        target_cursor = self.target_conn.cursor()
        row_count = 0
        try:
            self._drop_and_create_ext_table(
                target_cursor,
                config,
                extract_result.columns_info,
            )

            load_timestamp = datetime.utcnow().replace(microsecond=0)
            partition_date = _parse_partition_date(partition_key) if partition_key else None

            data = [
                tuple(row) + (self.source_system, load_timestamp, partition_date)
                for row in extract_result.data
            ]

            if self.bulkcopy_conn is not None:
                try:
                    chunk_count, _ = bulkcopy_insert_with_retry(
                        self.context,
                        self.bulkcopy_conn,
                        config,
                        extract_result.column_names,
                        data,
                        mssql_target_resource=self.mssql_target_resource,
                    )
                except Exception as exc:
                    raise _diagnose_insert_error(
                        exc,
                        data,
                        extract_result.column_names,
                        extract_result.columns_info,
                        config.target_table,
                        target_conn=self.target_conn,
                    ) from exc
                row_count = chunk_count
            else:
                row_count = self._executemany_insert_batch(
                    target_cursor,
                    config,
                    extract_result.column_names,
                    data,
                    columns_info=extract_result.columns_info,
                )

            self._log_insert_result(row_count, config, len(extract_result.columns_info))
            self.target_conn.commit()
        except BaseException:
            self._rollback_with_warning(config)
            raise
        finally:
            target_cursor.close()

        return row_count

    # ------------------------------------------------------------------
    # Relation extract (read-only, for dependent/bridge tables)
    # ------------------------------------------------------------------

    def relation_extract(
        self, config, partition_key: str | None, partition_type: str
    ) -> ExtractResult:
        is_scheduled_catchup = _detect_scheduled_catchup(self.context)
        first_partition = bool(partition_key and _is_first_partition(partition_key))

        columns_info, column_names = get_table_columns_info(self.source_conn, config)

        where_clause = build_relation_where_clause(
            config,
            partition_key or "",
            self.source_system,
            is_scheduled_catchup,
            first_partition,
        )

        source_cursor = self.source_conn.cursor()
        try:
            self._execute_select(source_cursor, config, where_clause, log_prefix="Relation")

            if self.source_system == "ILS":
                column_names = [name.replace("__", "##") for name in column_names]

            filtered_indices = self._compute_filtered_indices(column_names, config)
            if filtered_indices is not None:
                column_names = [column_names[i] for i in filtered_indices]
                columns_info = [columns_info[i] for i in filtered_indices]

            all_rows = self._fetch_all_rows(source_cursor, filtered_indices)

            self.context.log.info(
                f"Relation extracted {len(all_rows)} rows from "
                f"{config.source_schema}.{config.source_table}"
            )

            return ExtractResult(
                data=all_rows,
                columns_info=columns_info,
                column_names=column_names,
                row_count=len(all_rows),
            )
        finally:
            source_cursor.close()

    # ------------------------------------------------------------------
    # Streaming extract + insert (single pass, for dependent/bridge)
    # ------------------------------------------------------------------

    def stream_relation_extract_and_insert(
        self, config, partition_key: str | None, partition_type: str
    ) -> int:
        """Extract from a dependent/bridge table and insert in a single streaming pass."""
        if self.target_conn is None:
            raise ValueError(
                "MssqlExtractor.stream_relation_extract_and_insert() requires a target_conn"
            )

        is_scheduled_catchup = _detect_scheduled_catchup(self.context)
        first_partition = bool(partition_key and _is_first_partition(partition_key))

        columns_info, column_names = get_table_columns_info(self.source_conn, config)

        where_clause = build_relation_where_clause(
            config,
            partition_key or "",
            self.source_system,
            is_scheduled_catchup,
            first_partition,
        )

        source_cursor = self.source_conn.cursor()
        try:
            self._execute_select(
                source_cursor, config, where_clause, log_prefix="Streaming relation"
            )

            if self.source_system == "ILS":
                column_names = [name.replace("__", "##") for name in column_names]

            filtered_indices = self._compute_filtered_indices(column_names, config)
            if filtered_indices is not None:
                column_names = [column_names[i] for i in filtered_indices]
                columns_info = [columns_info[i] for i in filtered_indices]

            use_bulkcopy = self.bulkcopy_conn is not None
            target_cursor = self.target_conn.cursor()
            row_count = 0
            try:
                self._drop_and_create_ext_table(
                    target_cursor,
                    config,
                    columns_info,
                    avoid_max_types=use_bulkcopy,
                )

                load_timestamp = datetime.utcnow().replace(microsecond=0)
                partition_date = _parse_partition_date(partition_key) if partition_key else None

                if use_bulkcopy:
                    row_count = self._stream_via_bulkcopy(
                        source_cursor,
                        config,
                        columns_info,
                        column_names,
                        filtered_indices,
                        load_timestamp,
                        partition_date,
                    )
                else:
                    row_count = self._stream_via_executemany(
                        source_cursor,
                        target_cursor,
                        config,
                        column_names,
                        filtered_indices,
                        load_timestamp,
                        partition_date,
                    )

                self._log_insert_result(row_count, config, len(columns_info))
                self.target_conn.commit()
            except BaseException:
                self._rollback_with_warning(config)
                raise
            finally:
                target_cursor.close()

            return row_count
        finally:
            source_cursor.close()

    # ------------------------------------------------------------------
    # Full streaming extract + insert (for non-dependent tables via load_table_to_ext)
    # ------------------------------------------------------------------

    def stream_extract_and_insert(
        self, config: TableConfig, partition_key: str | None
    ) -> TableLoadResult:
        """Full streaming extract and insert for transactional / master-data tables.

        This is the primary entry point used by the asset factory for all
        non-bridge/non-dependent tables.  It incorporates consistency checking,
        bulk-copy with retry/refresh, and driver-aware executemany fallbacks.
        """
        if self.target_conn is None:
            raise ValueError("MssqlExtractor.stream_extract_and_insert() requires a target_conn")

        self.context.log.info(f"Extracting data from {config.source_schema}.{config.source_table}")

        is_scheduled_catchup = _detect_scheduled_catchup(self.context)
        if is_scheduled_catchup:
            self.context.log.info(
                "Running in SCHEDULED CATCHUP mode: Extracting 25-hour window "
                f"(from {partition_key} 05:00 yesterday to 06:00 today)."
            )

        if config.is_full_load and config.consistency_columns:
            if check_data_consistency(self.context, self.source_conn, self.target_conn, config):
                return TableLoadResult(
                    source_table=f"{config.source_schema}.{config.source_table}",
                    target_table=f"ext.{config.target_table}",
                    row_count=0,
                )

        columns_info, column_names = get_table_columns_info(self.source_conn, config)
        source_cursor = self.source_conn.cursor()
        try:
            first_partition = bool(partition_key and _is_first_partition(partition_key))

            where_clause = self._resolve_where_clause(
                config,
                partition_key,
                is_scheduled_catchup,
                first_partition,
            )
            self._execute_select(source_cursor, config, where_clause)

            if self.source_system == "ILS":
                column_names = [name.replace("__", "##") for name in column_names]

            filtered_indices = self._compute_filtered_indices(column_names, config)
            if filtered_indices is not None:
                column_names = [column_names[i] for i in filtered_indices]
                columns_info = [columns_info[i] for i in filtered_indices]

            use_bulkcopy = self.bulkcopy_conn is not None
            target_cursor = self.target_conn.cursor()
            row_count = 0
            try:
                self._drop_and_create_ext_table(
                    target_cursor,
                    config,
                    columns_info,
                    avoid_max_types=use_bulkcopy,
                )

                load_timestamp = datetime.utcnow().replace(microsecond=0)
                partition_date = _parse_partition_date(partition_key) if partition_key else None

                if use_bulkcopy:
                    row_count = self._stream_via_bulkcopy(
                        source_cursor,
                        config,
                        columns_info,
                        column_names,
                        filtered_indices,
                        load_timestamp,
                        partition_date,
                    )
                elif _is_mssql_python_conn(self.target_conn):
                    self.context.log.info(
                        "Using mssql-python executemany (100k-row chunks) for insert"
                    )
                    row_count = self._streaming_executemany(
                        source_cursor,
                        self.target_conn,
                        config,
                        column_names,
                        filtered_indices,
                        load_timestamp,
                        partition_date,
                        placeholder="?",
                        chunk_size=2_000,
                        columns_info=columns_info,
                    )
                else:
                    self.context.log.info("Using pymssql executemany (2k-row chunks) for insert")
                    row_count = self._streaming_executemany(
                        source_cursor,
                        self.target_conn,
                        config,
                        column_names,
                        filtered_indices,
                        load_timestamp,
                        partition_date,
                        placeholder="%s",
                        chunk_size=2_000,
                    )

                self._log_insert_result(row_count, config, len(columns_info))
                self.target_conn.commit()
            except BaseException:
                self._rollback_with_warning(config)
                raise
            finally:
                target_cursor.close()
        finally:
            source_cursor.close()

        return TableLoadResult(
            source_table=f"{config.source_schema}.{config.source_table}",
            target_table=f"ext.{config.target_table}",
            row_count=row_count,
        )

    # ------------------------------------------------------------------
    # Consistency check
    # ------------------------------------------------------------------

    def check_consistency(self, config) -> bool:
        if not config.consistency_columns:
            return False

        return check_data_consistency(self.context, self.source_conn, self.target_conn, config)

    # ------------------------------------------------------------------
    # Custom query
    # ------------------------------------------------------------------

    def custom_extract(self, query: str) -> ExtractResult:
        self.context.log.info("Executing custom MSSQL query")
        self.context.log.debug(f"Query: {query}")

        cursor = self.source_conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            columns_info = [
                self._map_cursor_desc_to_columns_info(desc) for desc in (cursor.description or [])
            ]

            all_rows: list[tuple] = []
            chunk_size = 2000
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break
                all_rows.extend(self._normalize_row_for_insert(row) for row in chunk)

            return ExtractResult(
                data=all_rows,
                columns_info=columns_info,
                column_names=columns,
                row_count=len(all_rows),
            )
        finally:
            cursor.close()

    def custom_stream_extract_and_insert(
        self,
        query: str,
        config: TableConfig,
        partition_key: str | None = None,
    ) -> int:
        """Execute a custom query and stream results directly into the target table.

        Combines custom query execution with the existing streaming insert
        infrastructure so that only one chunk of rows (~20-50k) is held in
        memory at any time.  No ``ExtractResult`` is created.
        """
        if self.target_conn is None:
            raise ValueError(
                "MssqlExtractor.custom_stream_extract_and_insert() requires a target_conn"
            )

        self.context.log.info("Executing custom streaming MSSQL query")
        self.context.log.debug(f"Query: {query}")

        source_cursor = self.source_conn.cursor()
        try:
            source_cursor.execute(query)
            columns_info = [
                self._map_cursor_desc_to_columns_info(desc)
                for desc in (source_cursor.description or [])
            ]
            column_names = [desc[0] for desc in source_cursor.description or []]

            use_bulkcopy = self.bulkcopy_conn is not None
            target_cursor = self.target_conn.cursor()
            row_count = 0
            try:
                self._drop_and_create_ext_table(
                    target_cursor,
                    config,
                    columns_info,
                    avoid_max_types=use_bulkcopy,
                )

                load_timestamp = datetime.utcnow().replace(microsecond=0)
                partition_date = _parse_partition_date(partition_key) if partition_key else None

                if use_bulkcopy:
                    row_count = self._stream_via_bulkcopy(
                        source_cursor,
                        config,
                        columns_info,
                        column_names,
                        None,
                        load_timestamp,
                        partition_date,
                    )
                else:
                    row_count = self._stream_via_executemany(
                        source_cursor,
                        target_cursor,
                        config,
                        column_names,
                        None,
                        load_timestamp,
                        partition_date,
                    )

                self._log_insert_result(row_count, config, len(columns_info))
                self.target_conn.commit()
            except BaseException:
                self._rollback_with_warning(config)
                raise
            finally:
                target_cursor.close()

            return row_count
        finally:
            source_cursor.close()

    @staticmethod
    def _map_cursor_desc_to_columns_info(
        desc: tuple,
    ) -> tuple[str, Any, Any, Any, Any, Any]:
        """Map a DB-API cursor.description entry to the columns_info tuple format.

        mssql-python provides type_code as Python type classes (int, str,
        datetime.date, decimal.Decimal, etc.).  This maps them to SQL type
        name strings so _build_input_sizes / create_ext_table_ddl work.

        Falls back to ODBC integer type constants and string type names
        when the driver does not supply Python type classes.
        """
        name = desc[0]
        type_code = desc[1]
        internal_size = desc[3] if len(desc) > 3 else None
        precision = desc[4] if len(desc) > 4 else None
        scale = desc[5] if len(desc) > 5 else None
        nullable = desc[6] if len(desc) > 6 else None

        _PYTHON_TYPE_TO_SQL: dict[type, str] = {
            int: "bigint",
            float: "float",
            Decimal: "decimal",
            str: "nvarchar",
            bool: "bit",
            datetime: "datetime2",
            date: "date",
            bytes: "varbinary",
            bytearray: "varbinary",
        }

        # ODBC SQL type constants (returned by some drivers as int type_code)
        _ODBC_TYPE_TO_SQL: dict[int, str] = {
            -9: "nvarchar",  # SQL_WVARCHAR
            -8: "nchar",  # SQL_WCHAR
            -10: "nvarchar",  # SQL_WLONGVARCHAR
            12: "varchar",  # SQL_VARCHAR
            1: "char",  # SQL_CHAR
            -1: "varchar",  # SQL_LONGVARCHAR
            -5: "bigint",  # SQL_BIGINT
            4: "int",  # SQL_INTEGER
            5: "smallint",  # SQL_SMALLINT
            -6: "tinyint",  # SQL_TINYINT
            -7: "bit",  # SQL_BIT
            2: "decimal",  # SQL_NUMERIC
            3: "decimal",  # SQL_DECIMAL
            6: "float",  # SQL_FLOAT
            7: "real",  # SQL_REAL
            8: "float",  # SQL_DOUBLE
            91: "date",  # SQL_TYPE_DATE
            93: "datetime2",  # SQL_TYPE_TIMESTAMP
            92: "time",  # SQL_TYPE_TIME
            -2: "varbinary",  # SQL_BINARY
            -3: "varbinary",  # SQL_VARBINARY
            -4: "varbinary",  # SQL_LONGVARBINARY
        }

        sql_type = None
        if isinstance(type_code, type):
            sql_type = _PYTHON_TYPE_TO_SQL.get(type_code)
        elif isinstance(type_code, int):
            sql_type = _ODBC_TYPE_TO_SQL.get(type_code)
        elif isinstance(type_code, str) and type_code:
            sql_type = type_code.lower()

        # ext is a staging heap, so fixed-width types buy nothing but padding.
        # Widening them also avoids map_sql_data_type_to_ddl's CHAR(1)/NCHAR(1)
        # fallback when the driver reports no size for a computed column.
        if sql_type == "nchar":
            sql_type = "nvarchar"
        elif sql_type == "char":
            sql_type = "varchar"

        col_size = None
        if sql_type == "nvarchar":
            col_size = internal_size if internal_size and 0 < internal_size <= 4000 else 4000
        elif sql_type == "varchar":
            col_size = internal_size if internal_size and 0 < internal_size <= 8000 else 8000
        elif sql_type == "varbinary":
            col_size = internal_size if internal_size and 0 < internal_size <= 8000 else 8000

        return (name, sql_type, nullable, col_size, precision, scale)

    @staticmethod
    def _normalize_row_for_insert(row: tuple) -> tuple:
        """Convert numeric Python types to strings for WVARCHAR-based insert.

        _build_input_sizes binds int/decimal/float columns as WVARCHAR
        (the mssql-python driver cannot accept raw numeric Python objects
        with that binding), so they must be stringified here.

        Decimal/float values use fixed-point notation (format ``f``) to
        avoid scientific notation (e.g. ``0E-12``) which SQL Server cannot
        parse when converting NVARCHAR to DECIMAL/NUMERIC.

        Date/datetime values are left as native Python objects because
        _build_input_sizes binds them as SQL_TYPE_DATE / SQL_TYPE_TIMESTAMP
        and the mssql-python driver requires real date/datetime instances
        for those bindings (accessing .year, .month, etc.).
        """
        return tuple(
            (
                format(v, "f")
                if v is not None and isinstance(v, Decimal | float)
                else (
                    str(int(v))
                    if v is not None and isinstance(v, bool)
                    else str(v) if v is not None and isinstance(v, int) else v
                )
            )
            for v in row
        )

    # ==================================================================
    # Private helpers
    # ==================================================================

    def _resolve_where_clause(
        self,
        config: TableConfig,
        partition_key: str | None,
        is_scheduled_catchup: bool,
        first_partition: bool,
    ) -> str | None:
        if config.is_full_load or config.table_type == TABLE_TYPE_MASTER_DATA:
            self.context.log.info(f"Full load mode for {config.table_type} table")
            return config.custom_where_clause
        elif partition_key:
            self.context.log.info(
                f"Partition mode: {config.table_type}, partition_key={partition_key}"
            )
            return build_partition_where_clause(
                config,
                partition_key,
                self.source_system,
                is_scheduled_catchup,
                first_partition,
            )
        return config.custom_where_clause

    def _execute_select(
        self,
        cursor,
        config: TableConfig,
        where_clause: str | None,
        log_prefix: str = "",
    ) -> None:
        prefix = f"{log_prefix} extract" if log_prefix else "Extracting"
        if where_clause:
            query = (
                f"SELECT * FROM [{config.source_schema}].[{config.source_table}] "
                f"WHERE {where_clause}"
            )
            self.context.log.info(f"{prefix} with filter: {where_clause}")
            cursor.execute(query)
        else:
            self.context.log.info(f"{prefix}: all rows")
            cursor.execute(f"SELECT * FROM [{config.source_schema}].[{config.source_table}]")

    @staticmethod
    def _compute_filtered_indices(column_names: list[str], config: TableConfig) -> list[int] | None:
        if not config.exclude_columns:
            return None
        exclude_set = set(config.exclude_columns)
        return [i for i, name in enumerate(column_names) if name not in exclude_set]

    def _fetch_all_rows(
        self,
        source_cursor,
        filtered_indices: list[int] | None,
    ) -> list[tuple]:
        all_rows: list[tuple] = []
        chunk_size = 2000
        while True:
            chunk = source_cursor.fetchmany(chunk_size)
            if not chunk:
                break
            if self.source_system == "ILS":
                chunk = clean_ils_data(chunk, source_cursor.description)
            if filtered_indices:
                chunk = [tuple(row[i] for i in filtered_indices) for row in chunk]
            all_rows.extend(chunk)
        return all_rows

    def _drop_and_create_ext_table(
        self,
        target_cursor,
        config: TableConfig,
        columns_info: list[tuple[str, Any, Any, Any, Any, Any]],
        avoid_max_types: bool = False,
    ) -> None:
        self.context.log.info(
            f"Global drop/recreate: dropping ext.{config.target_table} "
            "to ensure a clean snapshot each run."
        )
        target_cursor.execute(f"DROP TABLE IF EXISTS [ext].[{config.target_table}]")
        self.target_conn.commit()

        extra_columns_def = [
            ("SourceSystem", "NVARCHAR(50)", False),
            ("dw_load_timestamp", "DATETIME2", False),
            ("dw_partition_date", "DATE", True),
        ]

        sample_types: list[type | None] = [None] * len(columns_info)
        ddl = create_ext_table_ddl(
            config.target_table,
            columns_info,
            extra_columns_def,
            sample_types,
            avoid_max_types=avoid_max_types,
        )
        self.context.log.info(f"Creating ext table with DDL:\n{ddl}")
        target_cursor.execute(ddl)
        self.target_conn.commit()

    def _executemany_insert_batch(
        self,
        target_cursor,
        config: TableConfig,
        column_names: list[str],
        data: list[tuple],
        columns_info: list[tuple[str, Any, Any, Any, Any, Any]] | None = None,
    ) -> int:
        """Insert pre-collected data via executemany in chunked batches."""
        is_mssql_python = _is_mssql_python_conn(self.target_conn)
        insert_columns = [f"[{name}]" for name in column_names]
        insert_columns.extend(["[SourceSystem]", "[dw_load_timestamp]", "[dw_partition_date]"])

        placeholder = "?" if is_mssql_python else "%s"
        placeholders = [placeholder] * len(insert_columns)
        insert_sql = (
            f"INSERT INTO [ext].[{config.target_table}] "
            f"({', '.join(insert_columns)}) VALUES ({', '.join(placeholders)})"
        )

        input_sizes = None
        if is_mssql_python:
            sample_row = data[0] if data else None
            input_sizes = _build_input_sizes(columns_info, len(column_names), sample_row=sample_row)
            target_cursor.setinputsizes(input_sizes)

        chunk_size = 100_000 if is_mssql_python else 2_000
        row_count = 0
        for i in range(0, max(len(data), 1), chunk_size):
            chunk = data[i : i + chunk_size]
            if not chunk:
                break
            try:
                target_cursor.executemany(insert_sql, chunk)
            except Exception as exc:
                raise _diagnose_insert_error(
                    exc,
                    chunk,
                    column_names,
                    columns_info,
                    config.target_table,
                    chunk_offset=i,
                    target_conn=self.target_conn,
                    insert_sql=insert_sql,
                    input_sizes=input_sizes,
                ) from exc
            self.target_conn.commit()
            row_count += len(chunk)
        return row_count

    def _stream_via_bulkcopy(
        self,
        source_cursor,
        config: TableConfig,
        columns_info: list[tuple[str, Any, Any, Any, Any, Any]],
        column_names: list[str],
        filtered_indices: list[int] | None,
        load_timestamp: datetime,
        partition_date: date | None,
    ) -> int:
        """Stream from source and insert via bulk-copy with retry and refresh."""
        self.context.log.info("Using mssql-python bulk-copy for insert")
        bcp_limits = build_bcp_truncation_limits(columns_info)
        num_cols = len(column_names) + 3
        fetch_chunk_size, estimated_row_bytes = compute_streaming_chunk_size(columns_info)
        if self.bcp_max_chunk_size is not None and fetch_chunk_size > self.bcp_max_chunk_size:
            self.context.log.info(
                f"Capping BCP chunk size {fetch_chunk_size:,} -> {self.bcp_max_chunk_size:,} "
                f"per extractor override"
            )
            fetch_chunk_size = self.bcp_max_chunk_size
        self.context.log.info(
            f"BCP chunk size: {fetch_chunk_size:,} rows "
            f"({num_cols} cols, ~{fetch_chunk_size * num_cols:,} cells/chunk, "
            f"~{estimated_row_bytes:,} bytes/row est.)"
        )

        def _transform(chunk, description):
            if self.source_system == "ILS":
                chunk = clean_ils_data(chunk, description)
            if filtered_indices:
                chunk = [tuple(row[i] for i in filtered_indices) for row in chunk]
            return chunk

        def _on_insert_error(exc, data, chunk_offset):
            return _diagnose_insert_error(
                exc,
                data,
                column_names,
                columns_info,
                config.target_table,
                chunk_offset=chunk_offset,
                target_conn=self.target_conn,
            )

        stats = stream_bulkcopy_chunks(
            context=self.context,
            source_cursor=source_cursor,
            bc_conn=self.bulkcopy_conn,
            target_resource=self.mssql_target_resource,
            config=config,
            column_names=column_names,
            bcp_limits=bcp_limits,
            chunk_size=fetch_chunk_size,
            source_system=self.source_system,
            load_timestamp=load_timestamp,
            partition_date=partition_date,
            transform_chunk=_transform,
            on_insert_error=_on_insert_error,
        )
        if stats.truncations > 0:
            self.context.log.warning(
                f"Total: {stats.truncations} value(s) truncated across all chunks "
                f"for ext.{config.target_table}"
            )
        return stats.rows

    def _stream_via_executemany(
        self,
        source_cursor,
        target_cursor,
        config: TableConfig,
        column_names: list[str],
        filtered_indices: list[int] | None,
        load_timestamp: datetime,
        partition_date: date | None,
    ) -> int:
        """Stream from source and insert via executemany (driver-aware)."""
        is_mssql_python = _is_mssql_python_conn(self.target_conn)
        chunk_size = 100_000 if is_mssql_python else 2_000
        placeholder = "?" if is_mssql_python else "%s"
        self.context.log.info(
            f"Streaming relation extract + insert via executemany " f"({chunk_size:,}-row chunks)"
        )
        return self._streaming_executemany(
            source_cursor,
            self.target_conn,
            config,
            column_names,
            filtered_indices,
            load_timestamp,
            partition_date,
            placeholder=placeholder,
            chunk_size=chunk_size,
        )

    def _streaming_executemany(
        self,
        source_cursor,
        target_conn,
        config: TableConfig,
        column_names: list[str],
        filtered_indices: list[int] | None,
        load_timestamp: datetime,
        partition_date: date | None,
        *,
        placeholder: str,
        chunk_size: int,
        columns_info: list[tuple[str, Any, Any, Any, Any, Any]] | None = None,
    ) -> int:
        """Generic streaming executemany loop for any MSSQL driver."""
        insert_columns = [f"[{name}]" for name in column_names]
        insert_columns.extend(["[SourceSystem]", "[dw_load_timestamp]", "[dw_partition_date]"])

        placeholders = [placeholder] * len(insert_columns)
        insert_sql = (
            f"INSERT INTO [ext].[{config.target_table}] "
            f"({', '.join(insert_columns)}) VALUES ({', '.join(placeholders)})"
        )

        target_cursor = target_conn.cursor()
        is_mssql_python = _is_mssql_python_conn(target_conn)
        input_sizes_set = False
        input_sizes = None
        row_count = 0
        chunk_num = 0

        try:
            while True:
                t_fetch = time.perf_counter()
                chunk = source_cursor.fetchmany(chunk_size)
                fetch_elapsed = time.perf_counter() - t_fetch
                if not chunk:
                    break
                if self.source_system == "ILS":
                    chunk = clean_ils_data(chunk, source_cursor.description)
                if filtered_indices:
                    chunk = [tuple(row[i] for i in filtered_indices) for row in chunk]

                rows_to_insert = [
                    self._normalize_row_for_insert(tuple(row))
                    + (self.source_system, load_timestamp, partition_date)
                    for row in chunk
                ]

                if is_mssql_python and not input_sizes_set:
                    input_sizes = _build_input_sizes(
                        columns_info,
                        len(column_names),
                        sample_row=rows_to_insert[0] if rows_to_insert else None,
                    )
                    target_cursor.setinputsizes(input_sizes)
                    input_sizes_set = True
                del chunk

                self.context.log.info(f"Rows to insert: {len(rows_to_insert)}")

                t_insert = time.perf_counter()
                try:
                    target_cursor.executemany(insert_sql, rows_to_insert)
                except Exception as exc:
                    raise _diagnose_insert_error(
                        exc,
                        rows_to_insert,
                        column_names,
                        columns_info,
                        config.target_table,
                        chunk_offset=row_count,
                        target_conn=target_conn,
                        insert_sql=insert_sql,
                        input_sizes=input_sizes,
                    ) from exc
                target_conn.commit()
                insert_elapsed = time.perf_counter() - t_insert
                row_count += len(rows_to_insert)
                chunk_num += 1
                if _should_log_chunk(chunk_num, fetch_elapsed, insert_elapsed):
                    slow = (
                        " [SLOW]"
                        if max(fetch_elapsed, insert_elapsed) >= _SLOW_CHUNK_SECONDS
                        else ""
                    )
                    self.context.log.info(
                        f"Chunk {chunk_num}: {row_count:,} rows so far | "
                        f"fetch={fetch_elapsed:.1f}s insert={insert_elapsed:.1f}s{slow}"
                    )
                del rows_to_insert
        finally:
            target_cursor.close()

        return row_count

    def _log_insert_result(self, row_count: int, config: TableConfig, num_columns: int) -> None:
        if row_count == 0:
            self.context.log.info(
                f"No rows returned from {config.source_schema}.{config.source_table}. "
                "Created empty ext table."
            )
        else:
            self.context.log.info(
                f"Extracted and inserted {row_count:,} rows with {num_columns} columns "
                f"from {config.source_table}"
            )

    def _rollback_with_warning(self, config: TableConfig) -> None:
        self.context.log.warning(f"Load interrupted for ext.{config.target_table} — rolling back.")
        try:
            self.target_conn.rollback()
        except Exception:
            self.context.log.warning("Rollback failed (connection may already be closed).")
