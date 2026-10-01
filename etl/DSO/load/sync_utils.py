# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Sync Utilities

Provides reusable functions for syncing data from staging (DSI/DSA) to DSO database
using SQL Server Linked Server for optimal performance.

The linked server approach executes MERGE statements entirely on the DSO database
server, pulling data from staging via the linked server connection. This eliminates
Python as an intermediary for large data transfers.

Architecture:
    Dagster → mssql_target_dso → MERGE using STAGING_LINK.DW.schema.table
"""

import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from dataclasses import replace as dc_replace
from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

import dagster as dg
from core.resources import MSSQLResource, get_linked_server_config

# =============================================================================
# Constants
# =============================================================================

_LINKED_SERVER_CONFIG = get_linked_server_config()
LINKED_SERVER_NAME = _LINKED_SERVER_CONFIG["name"]
STAGING_DATABASE = _LINKED_SERVER_CONFIG["database"]

# Sync engine and fallback reason (for config/metadata and filtering runs)
SYNC_ENGINE_LINKED_SERVER = "linked_server"
SYNC_ENGINE_PYTHON_FALLBACK = "python_fallback"
SYNC_ENGINE_SAME_DATABASE = "same_database"
SYNC_ENGINE_LOCAL_DEV_SKIP = "local_dev_skip"

FALLBACK_REASON_LINKED_SERVER_NOT_AVAILABLE = "linked_server_not_available"
FALLBACK_REASON_LINKED_SERVER_VALIDATION_FAILED = "linked_server_validation_failed"
FALLBACK_REASON_CONNECTIVITY = "connectivity"

# Env: Python fallback is default (allowed when linked server unavailable).
# Set to "false" or "0" to require linked server and fail the run if unavailable (e.g. in closed/prod).
DSO_SYNC_ALLOW_PYTHON_FALLBACK_ENV = "DSO_SYNC_ALLOW_PYTHON_FALLBACK"

# Cached collation mismatch result (None = not checked yet).
_collation_mismatch_cache: bool | None = None


_RETRYABLE_ERROR_PATTERNS = (
    "Communication link failure",
    "TCP Provider",
    "Error code 0x2749",
    "connection was closed",
    # SQL Server 7359: linked-server plan compiled against a stale remote schema; recompile heals it.
    "reported a change in schema version",
)

# mssql_python DDBC SQL type constants used by cursor.setinputsizes().
# We bind string/date-like values explicitly to avoid first-row width inference.
_SQL_WVARCHAR = -9
_SQL_TYPE_TIMESTAMP = 93
_SQL_TYPE_DATE = 91
_SQL_INTEGER = 4
_SQL_BIGINT = -5
_SQL_DOUBLE = 8
_SQL_BIT = -7
_SQL_VARBINARY = -3
_SQL_NUMERIC = 2


_SQL_MAX_NUMERIC_PRECISION = 38


def _is_mssql_python_connection(conn: object) -> bool:
    """Return True when the given connection is created by mssql_python."""
    return type(conn).__module__.startswith("mssql_python")


def _decimal_scale_and_int_digits(value: Decimal) -> tuple[int, int]:
    """Return (scale, integer_digits) needed to declare *value* in SQL Server.

    Scale comes from the exponent, not the visible digits: Decimal("0E-12") is a
    zero that still needs scale 12. Non-finite values have a string exponent and
    cannot be sized, so they report (0, 0).
    """
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        return (0, 0)
    if exponent < 0:
        scale = -exponent
        int_digits = max(0, len(digits) - scale)
    else:
        scale = 0
        int_digits = len(digits) + exponent
    return (scale, int_digits)


def _numeric_spec(int_digits: int, scale: int) -> tuple[int, int, int]:
    """Build a SQL_NUMERIC setinputsizes entry holding int_digits + scale.

    Integer digits win over scale at the 38-digit ceiling: dropping fractional
    digits rounds, dropping integer digits changes the magnitude.
    """
    int_digits = min(int_digits, _SQL_MAX_NUMERIC_PRECISION)
    scale = min(scale, _SQL_MAX_NUMERIC_PRECISION - int_digits)
    return (_SQL_NUMERIC, _SQL_MAX_NUMERIC_PRECISION, scale)


def _build_mssql_python_chunk_input_sizes(
    rows: list[tuple],
) -> list[tuple[int, int, int]] | None:
    """Build per-parameter input sizes for one executemany() chunk.

    mssql_python infers string parameter widths from early rows in a batch.
    If a later row is longer, executemany() can fail with
    "UTF-16 length exceeds allowed column size". This helper computes widths
    from the entire chunk and returns a setinputsizes()-compatible spec.

    Every column must get an explicit type entry when any column requires one,
    because mssql_python's setinputsizes() does not accept None entries.
    """
    if not rows:
        return None

    parameter_count = len(rows[0])
    if parameter_count == 0:
        return None

    sizes: list[tuple[int, int, int] | None] = [None] * parameter_count
    has_explicit_sizes = False

    for idx in range(parameter_count):
        saw_datetime = False
        saw_date = False
        saw_bool = False
        saw_int = False
        saw_float = False
        saw_decimal = False
        saw_bytes = False
        saw_uuid = False
        max_text_length = 0
        max_bytes_length = 0
        max_decimal_scale = 0
        max_decimal_int_digits = 0

        for row in rows:
            if idx >= len(row):
                continue
            value = row[idx]
            if value is None:
                continue
            if isinstance(value, datetime):
                saw_datetime = True
                break
            if isinstance(value, date):
                saw_date = True
                continue
            if isinstance(value, bool):
                saw_bool = True
                continue
            if isinstance(value, int):
                saw_int = True
                continue
            if isinstance(value, float):
                saw_float = True
                continue
            if isinstance(value, Decimal):
                saw_decimal = True
                scale, int_digits = _decimal_scale_and_int_digits(value)
                max_decimal_scale = max(max_decimal_scale, scale)
                max_decimal_int_digits = max(max_decimal_int_digits, int_digits)
                continue
            if isinstance(value, str):
                max_text_length = max(max_text_length, len(value))
            if isinstance(value, bytes | bytearray):
                saw_bytes = True
                max_bytes_length = max(max_bytes_length, len(value))
            if isinstance(value, UUID):
                saw_uuid = True

        if saw_datetime:
            sizes[idx] = (_SQL_TYPE_TIMESTAMP, 0, 0)
            has_explicit_sizes = True
        elif saw_date:
            sizes[idx] = (_SQL_TYPE_DATE, 0, 0)
            has_explicit_sizes = True
        elif max_text_length > 0:
            sizes[idx] = (_SQL_WVARCHAR, max_text_length, 0)
            has_explicit_sizes = True
        elif saw_bool:
            sizes[idx] = (_SQL_BIT, 0, 0)
            has_explicit_sizes = True
        elif saw_decimal:
            sizes[idx] = _numeric_spec(max_decimal_int_digits, max_decimal_scale)
            has_explicit_sizes = True
        elif saw_int:
            sizes[idx] = (_SQL_BIGINT, 0, 0)
            has_explicit_sizes = True
        elif saw_float:
            sizes[idx] = (_SQL_DOUBLE, 0, 0)
            has_explicit_sizes = True
        elif saw_bytes:
            sizes[idx] = (_SQL_VARBINARY, max_bytes_length, 0)
            has_explicit_sizes = True
        elif saw_uuid:
            sizes[idx] = (_SQL_WVARCHAR, 36, 0)
            has_explicit_sizes = True

    if not has_explicit_sizes:
        return None

    # Fill any remaining None entries with WVARCHAR(1) so every column
    # has a valid type — mssql_python rejects None in the sizes list.
    for idx in range(parameter_count):
        if sizes[idx] is None:
            sizes[idx] = (_SQL_WVARCHAR, 1, 0)

    return sizes


def _set_mssql_python_chunk_input_sizes(conn: object, cursor, rows: list[tuple]) -> None:
    """Apply setinputsizes() for mssql_python chunks when possible."""
    if not rows or not _is_mssql_python_connection(conn):
        return

    setinputsizes = getattr(cursor, "setinputsizes", None)
    if setinputsizes is None:
        return

    input_sizes = _build_mssql_python_chunk_input_sizes(rows)
    if input_sizes is not None:
        setinputsizes(input_sizes)


def _is_retryable_connectivity_error(exc: BaseException) -> bool:
    """True for transient connectivity or linked-server plan-staleness errors,
    matched by message across any exception class (mssql-python raises
    ProgrammingError, not RuntimeError, for 'TCP Provider ... actively refused
    it'). Patterns are specific enough not to match genuine query errors.
    """
    msg = str(exc)
    return any(pattern in msg for pattern in _RETRYABLE_ERROR_PATTERNS)


def _safe_close(*objs: object) -> None:
    """Close cursors/connections, swallowing errors (used during reconnect)."""
    for obj in objs:
        if obj is None:
            continue
        try:
            obj.close()  # type: ignore[attr-defined]
        except Exception:
            pass


def _stage_batches_via_openquery(
    context: dg.AssetExecutionContext,
    full_table: str,
    conn: object,
    cursor: object,
    batch_sql_for: Callable[[object], str],
    next_pk_for: Callable[[object, object], object],
    reopen: Callable[[], tuple],
    estimated_batches: int | None,
    sync_start: float,
) -> tuple:
    """Phase 1: stage linked-server rows into #_sync_staging via OPENQUERY with
    keyset pagination, retrying a transient connectivity error by closing,
    reopening (which recreates the temp table) and re-staging from scratch.

    Shared by the single-PK and composite-PK paths; they differ only in how a
    batch is selected (``batch_sql_for``), how the next keyset cursor is resolved
    (``next_pk_for`` -> next pk, or None to stop) and how a fresh connection is
    opened (``reopen``). Returns ``(total_rows, conn, cursor)`` — conn/cursor may
    be new after a reconnect; on error the in-flight connection is closed so it
    cannot leak.
    """
    backoff_seconds = (5, 15, 45)
    last_pk: object = None
    total_rows = 0
    batch_num = 0
    try:
        while True:
            for attempt in range(len(backoff_seconds) + 1):
                try:
                    cursor.execute(batch_sql_for(last_pk))
                    rows_inserted = cursor.rowcount
                    break
                except Exception as e:
                    if attempt < len(backoff_seconds) and _is_retryable_connectivity_error(e):
                        wait = backoff_seconds[attempt]
                        context.log.warning(
                            f"Batch {batch_num + 1} failed (attempt {attempt + 1}/"
                            f"{len(backoff_seconds) + 1}): {str(e)[:200]}. Retrying in {wait}s..."
                        )
                        time.sleep(wait)
                        _safe_close(cursor, conn)
                        conn, cursor = reopen()
                        last_pk, total_rows, batch_num = None, 0, 0
                        continue
                    raise

            if rows_inserted == 0:
                break

            prev_last_pk = last_pk
            last_pk = next_pk_for(cursor, last_pk)
            if last_pk is None:
                break
            if last_pk == prev_last_pk:
                raise RuntimeError(
                    f"Batched sync {full_table}: keyset pagination cursor "
                    f"did not advance past {last_pk!r}"
                )

            total_rows += rows_inserted
            batch_num += 1
            progress = f"batch {batch_num}"
            if estimated_batches:
                progress += f"/{estimated_batches}"
            elapsed = time.monotonic() - sync_start
            rps = total_rows / elapsed if elapsed > 0 else 0
            context.log.info(
                f"Phase 1 progress: {progress}, {total_rows:,} rows staged ({rps:,.0f} rows/s)"
            )
    except BaseException:
        _safe_close(cursor, conn)
        raise

    context.log.info(f"Phase 1 complete: {total_rows:,} rows staged in #_sync_staging")
    return total_rows, conn, cursor


def _is_integrity_error(exc: BaseException) -> bool:
    """Check if an exception is a SQL integrity constraint violation from any driver."""
    type_name = type(exc).__name__
    return type_name == "IntegrityError" or (
        type_name == "DatabaseError" and "IntegrityError" in str(exc)
    )


def _get_database_collation(mssql_resource: MSSQLResource) -> str:
    """Query the default collation of the database the resource connects to."""
    with mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS NVARCHAR(128))"
            )
            return cursor.fetchone()[0]
        finally:
            cursor.close()


def check_collation_mismatch(
    mssql_source_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
    context: dg.AssetExecutionContext | None = None,
) -> bool:
    """Return True if staging and DSO databases use different default collations.

    The result is cached for the lifetime of the process so the two extra
    queries only run once (on the first sync asset in a Dagster run).
    """
    global _collation_mismatch_cache  # noqa: PLW0603

    if _collation_mismatch_cache is not None:
        return _collation_mismatch_cache

    staging_collation = _get_database_collation(mssql_source_staging)
    dso_collation = _get_database_collation(mssql_target_dso)
    mismatch = staging_collation != dso_collation

    if context is not None:
        if mismatch:
            context.log.info(
                f"Collation check: staging={staging_collation}, "
                f"dso={dso_collation} -> COLLATE needed"
            )
        else:
            context.log.info(
                f"Collation check: both databases use {dso_collation} -> COLLATE skipped"
            )

    _collation_mismatch_cache = mismatch
    return mismatch


def get_param_placeholder(mssql_resource: MSSQLResource) -> str:
    """Get the SQL parameter placeholder for mssql-python ('?')."""
    return "?"


def is_same_database_mode(
    mssql_target_dso: MSSQLResource | None = None,
    mssql_source_staging: MSSQLResource | None = None,
) -> bool:
    """
    Check if staging and DSO are using the same database.

    In local development, both may point to the same DB, making
    linked server sync unnecessary (data is already in place).

    Args:
        mssql_target_dso: Optional DSO resource to check configuration from
        mssql_source_staging: Optional staging resource to check configuration from

    Returns:
        True if staging and DSO share the same host and database
    """
    # Get staging config from resource first, then fall back to env vars
    if mssql_source_staging is not None:
        staging_host = mssql_source_staging.host
        staging_db = mssql_source_staging.database
    else:
        staging_host = os.environ.get("MSSQL_SOURCE_STAGING_HOST") or os.environ.get(
            "MSSQL_TARGET_STAGING_HOST", "localhost"
        )
        staging_db = os.environ.get("MSSQL_SOURCE_STAGING_DB") or os.environ.get(
            "MSSQL_TARGET_STAGING_DB", "DW"
        )

    # Get DSO config from resource first, then fall back to env vars
    if mssql_target_dso is not None:
        dso_host = mssql_target_dso.host
        dso_db = mssql_target_dso.database
    else:
        dso_host = os.environ.get("MSSQL_TARGET_DSO_HOST", "localhost")
        dso_db = os.environ.get("MSSQL_TARGET_DSO_DB", "DW")

    return staging_host == dso_host and staging_db == dso_db


def can_connect_to_staging(mssql_source_staging: MSSQLResource) -> bool:
    """
    Test if we can actually connect to the staging database.

    Returns:
        True if connection succeeds, False otherwise
    """
    try:
        with mssql_source_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1")
                return True
            finally:
                cursor.close()
    except Exception:
        return False


# =============================================================================
# Configuration Classes
# =============================================================================


@dataclass
class SyncTableConfig:
    """Configuration for a DSO sync operation."""

    schema: str
    table: str
    primary_key: str | list[str]
    # 'merge' = full MERGE of all rows via build_merge_statement()
    # 'watermark' = delta MERGE of changed rows via build_delta_merge_statement();
    #               falls back to full merge on first run (no previous timestamp)
    # 'truncate_insert' = TRUNCATE + INSERT (full refresh, e.g. Type 2 SCD)
    mode: Literal["merge", "watermark", "truncate_insert"] = "merge"
    exclude_columns: list[str] = field(default_factory=list)
    # Columns that should only be set on INSERT, not UPDATE
    insert_only_columns: list[str] = field(default_factory=lambda: ["dw_timestamp_insert"])
    # Set to True when the PK is an IDENTITY column and we need to preserve
    # the identity values from staging (requires SET IDENTITY_INSERT ON)
    identity_insert: bool = False
    # Column used as watermark for delta sync, e.g. 'dw_timestamp_update' or 'dw_timestamp_insert'.
    # Required when mode='watermark', ignored otherwise.
    watermark_column: str | None = None
    # When True, disable nonclustered (non-PK) indexes on target before MERGE and rebuild after.
    # Use only for very large fact tables where index maintenance during MERGE dominates; adds risk
    # (rebuild must run in finally). Source (staging) indexes are never disabled.
    disable_target_indexes_during_sync: bool = False
    # Rows per chunk when using the Python fallback path (_sync_via_python).
    # Controls fetchmany() size and executemany() batch size to keep memory constant.
    # Default 5k keeps memory <10 MB even for wide dims with NVARCHAR(MAX).
    # Facts (narrow rows) override to 20k. Tune per table via sync_configs.
    chunk_size: int = 5_000
    # PK columns that need COLLATE DATABASE_DEFAULT in MERGE ON clauses.
    # Required when staging and DSO databases use different collations and
    # the PK contains string columns (COLLATE is invalid on integer types).
    collate_columns: list[str] = field(default_factory=list)
    # Column that must be unique among rows with dw_is_current = 1
    # (filtered unique index on the target). MERGE checks unique indexes
    # per-row, so the outgoing current row is cleared before promoting the
    # incoming one. None = no current-flag rotation (default).
    unique_current_key: str | None = None
    # Batch size for linked server operations. Data is staged into a local
    # temp table in batches of this size via OPENQUERY, then one local MERGE
    # is performed. Applies to single-PK tables only; composite PK tables
    # use the existing single-statement path.
    linked_server_batch_size: int = 100_000
    # Optional staging source when it differs from DSO target (e.g. conceptmap.vw_* -> conceptmap.*)
    source_schema: str | None = None
    source_table: str | None = None

    @property
    def staging_schema(self) -> str:
        return self.source_schema if self.source_schema is not None else self.schema

    @property
    def staging_table(self) -> str:
        return self.source_table if self.source_table is not None else self.table

    def __post_init__(self) -> None:
        if self.mode == "watermark" and not self.watermark_column:
            raise ValueError(
                f"watermark_column must be set when mode='watermark' "
                f"(table: {self.schema}.{self.table})"
            )


def _build_on_clause(
    pk_cols: list[str],
    config: SyncTableConfig,
    string_pk_cols: set[str] | None = None,
) -> str:
    """Build MERGE ON clause, adding COLLATE DATABASE_DEFAULT for string PK columns.

    When *string_pk_cols* is provided (runtime, from DB metadata), it takes
    precedence.  Otherwise falls back to ``config.collate_columns`` (useful in
    unit tests without a live DB connection).
    """
    collate_set = string_pk_cols if string_pk_cols is not None else set(config.collate_columns)
    parts = []
    for pk in pk_cols:
        if pk in collate_set:
            parts.append(f"tgt.[{pk}] = src.[{pk}] COLLATE DATABASE_DEFAULT")
        else:
            parts.append(f"tgt.[{pk}] = src.[{pk}]")
    return " AND ".join(parts)


def build_clear_stale_current_sql(
    config: SyncTableConfig,
    source_from: str,
    string_pk_cols: set[str] | None = None,
) -> str | None:
    """SQL that zeros dw_is_current on target rows whose current version moved.

    Returns None when the table has no filtered unique-current index.
    ``source_from`` is the MERGE source (4-part name, temp table, or subquery)
    and is aliased as ``src`` in the join.
    """
    key = config.unique_current_key
    if not key:
        return None

    pk_cols = (
        [config.primary_key] if isinstance(config.primary_key, str) else list(config.primary_key)
    )
    collate_set = string_pk_cols if string_pk_cols is not None else set(config.collate_columns)
    if key in collate_set:
        key_join = f"tgt.[{key}] = src.[{key}] COLLATE DATABASE_DEFAULT"
    else:
        key_join = f"tgt.[{key}] = src.[{key}]"
    pk_mismatch = " OR ".join(f"tgt.[{c}] <> src.[{c}]" for c in pk_cols)

    return f"""
        UPDATE tgt
        SET tgt.dw_is_current = 0
        FROM [{config.schema}].[{config.table}] AS tgt
        INNER JOIN {source_from} AS src
            ON {key_join}
           AND src.dw_is_current = 1
        WHERE tgt.dw_is_current = 1
          AND ({pk_mismatch})
    """


def _clear_stale_current_before_merge(
    cursor,
    config: SyncTableConfig,
    source_from: str,
    string_pk_cols: set[str] | None = None,
) -> None:
    sql = build_clear_stale_current_sql(config, source_from, string_pk_cols)
    if sql:
        cursor.execute(sql)


def _find_update_timestamp_col(columns: list[str]) -> str | None:
    """Find the update timestamp column from the actual column list."""
    for col in columns:
        if col in ("dw_timestamp_update",):
            return col
    return None


def _find_insert_timestamp_col(columns: list[str]) -> str | None:
    """Find the insert timestamp column from the actual column list."""
    for col in columns:
        if col in ("dw_timestamp_insert",):
            return col
    return None


# =============================================================================
# Core Sync Functions
# =============================================================================


def check_linked_server_support(mssql_target_dso: MSSQLResource) -> bool:
    """
    Check if the target database supports linked servers.

    Azure SQL Edge and some Azure SQL configurations do not support linked servers.

    Returns:
        True if linked servers are supported
    """
    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Try to check for linked server stored procedures
            cursor.execute("SELECT COUNT(*) FROM sys.servers")
            return True
        except Exception:
            return False
        finally:
            cursor.close()


def validate_linked_server(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> bool:
    """
    Validate that the linked server is accessible and can read from staging.

    In same-database mode (local development), this check is skipped since
    staging and DSO share the same database.

    Args:
        context: Dagster asset execution context
        mssql_target_dso: DSO database resource

    Returns:
        True if linked server is valid and accessible (or same-database mode)

    Raises:
        RuntimeError: If linked server is not accessible (in linked server mode)
    """
    # In same-database mode, no linked server is needed
    if is_same_database_mode(mssql_target_dso):
        context.log.info("📦 Same-database mode: staging and DSO share the same database")
        context.log.info("   -> Linked server validation skipped (not needed)")
        return True

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Check linked server exists
            # Use string formatting since LINKED_SERVER_NAME is a safe constant
            cursor.execute(f"SELECT COUNT(*) FROM sys.servers WHERE name = '{LINKED_SERVER_NAME}'")
            if cursor.fetchone()[0] == 0:
                raise RuntimeError(
                    f"Linked server '{LINKED_SERVER_NAME}' does not exist on DSO database. "
                    f"Run the dso_infra_linked_server asset first."
                )

            # Test connectivity
            try:
                cursor.execute(f"EXEC sp_testlinkedserver @servername = N'{LINKED_SERVER_NAME}'")
                context.log.info(f"✅ Linked server '{LINKED_SERVER_NAME}' is accessible")
                return True
            except Exception as e:
                raise RuntimeError(
                    f"Linked server '{LINKED_SERVER_NAME}' exists but is not accessible: {e}"
                ) from e
        finally:
            cursor.close()


def get_staging_table_columns(
    mssql_target_dso: MSSQLResource,
    schema: str,
    table: str,
    linked_server: str = LINKED_SERVER_NAME,
    staging_db: str = STAGING_DATABASE,
    mssql_source_staging: MSSQLResource | None = None,
) -> list[str]:
    """
    Get column names for a table from the staging database via linked server or direct connection.

    Args:
        mssql_target_dso: DSO database resource (for linked server queries)
        schema: Schema name
        table: Table name
        linked_server: Name of the linked server
        staging_db: Name of the staging database
        mssql_source_staging: Optional staging resource for direct query fallback

    Returns:
        List of column names from staging table
    """
    # Try linked server first
    source_table = f"[{linked_server}].[{staging_db}].[{schema}].[{table}]"
    try:
        with mssql_target_dso.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Use string formatting for schema/table - they're safe config values
                cursor.execute(
                    f"""
                SELECT COLUMN_NAME
                FROM {source_table}.sys.columns c
                INNER JOIN {source_table}.sys.tables t ON c.object_id = t.object_id
                INNER JOIN {source_table}.sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{schema}' AND t.name = '{table}'
                ORDER BY c.column_id
                """
                )
                return [row[0] for row in cursor.fetchall()]
            finally:
                cursor.close()
    except Exception:
        # Linked server failed - try direct staging connection if available
        if mssql_source_staging is not None:
            with mssql_source_staging.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    # Use string formatting for schema/table - they're safe config values
                    cursor.execute(
                        f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                    ORDER BY ORDINAL_POSITION
                    """
                    )
                    return [row[0] for row in cursor.fetchall()]
                finally:
                    cursor.close()
        else:
            # No fallback available, re-raise
            raise


class SchemaValidationError(Exception):
    """Raised when DSA (staging) and DSO table schemas do not match.

    This error indicates a schema drift between staging and DSO databases.
    The fix should be applied to the DDL files in infrastructure/ddl/,
    then the appropriate infrastructure assets should be re-run.

    DSO layer should NEVER modify DSA layer schemas - data flows one way:
    DSA -> DSO.
    """

    pass


def _get_column_details(
    mssql_resource: MSSQLResource,
    schema: str,
    table: str,
) -> dict[str, dict]:
    """
    Get detailed column information including types, nullability, and identity.

    Args:
        mssql_resource: Database resource to query
        schema: Schema name
        table: Table name

    Returns:
        Dictionary mapping column name to column details dict
    """
    # Use string formatting for schema/table names since they're safe values.
    # This avoids cross-driver placeholder differences when querying metadata.
    query = f"""
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME),
                           c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = '{schema}' AND c.TABLE_NAME = '{table}'
        ORDER BY c.ORDINAL_POSITION
    """

    with mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)

            columns = {}
            for row in cursor.fetchall():
                columns[row[0]] = {
                    "data_type": row[1],
                    "max_length": row[2],
                    "precision": row[3],
                    "scale": row[4],
                    "nullable": row[5],
                    "is_identity": row[6] == 1,
                }
            return columns
        finally:
            cursor.close()


def _compare_schemas(
    staging: dict[str, dict],
    dso: dict[str, dict],
) -> list[str]:
    """
    Compare two schema dictionaries and return list of differences.

    Args:
        staging: Column details from staging database
        dso: Column details from DSO database

    Returns:
        List of human-readable difference descriptions
    """
    differences = []

    # Check for missing columns
    staging_only = set(staging.keys()) - set(dso.keys())
    dso_only = set(dso.keys()) - set(staging.keys())

    if staging_only:
        differences.append(f"Columns in STAGING but not DSO: {sorted(staging_only)}")
    if dso_only:
        differences.append(f"Columns in DSO but not STAGING: {sorted(dso_only)}")

    # Check for type/property mismatches on common columns
    common = set(staging.keys()) & set(dso.keys())
    for col in sorted(common):
        s, d = staging[col], dso[col]

        if s["data_type"] != d["data_type"]:
            differences.append(
                f"Column '{col}' type mismatch: staging={s['data_type']}, dso={d['data_type']}"
            )

        if s["is_identity"] != d["is_identity"]:
            differences.append(
                f"Column '{col}' IDENTITY mismatch: staging={s['is_identity']}, dso={d['is_identity']}"
            )

        if s["nullable"] != d["nullable"]:
            differences.append(
                f"Column '{col}' nullability mismatch: staging={s['nullable']}, dso={d['nullable']}"
            )

    return differences


def validate_table_schemas_match(
    context: dg.AssetExecutionContext,
    mssql_source_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
    schema: str,
    table: str,
    staging_schema: str | None = None,
    staging_table: str | None = None,
) -> None:
    """
    Validate that staging (DSA) and DSO tables have identical schemas.

    This validation is strict - any mismatch stops the sync with a detailed
    error message explaining the differences and remediation steps.

    DSO layer should NEVER modify DSA layer schemas. If schemas don't match,
    the fix must be applied to the DDL files and infrastructure assets re-run.

    Args:
        context: Dagster asset execution context
        mssql_source_staging: Staging database resource (source of truth for data)
        mssql_target_dso: DSO database resource (target for sync)
        schema: DSO schema name
        table: DSO table name
        staging_schema: Staging schema when it differs from DSO (defaults to schema)
        staging_table: Staging table/view when it differs from DSO (defaults to table)

    Raises:
        SchemaValidationError: If schemas don't match with detailed diff
    """
    src_schema = staging_schema if staging_schema is not None else schema
    src_table = staging_table if staging_table is not None else table
    full_table = f"{schema}.{table}"

    try:
        # Get detailed column info from both databases
        staging_columns = _get_column_details(mssql_source_staging, src_schema, src_table)
        dso_columns = _get_column_details(mssql_target_dso, schema, table)

        # Check if table exists in both databases
        if not staging_columns:
            raise SchemaValidationError(
                f"Table {full_table} does not exist in staging database. "
                "Run staging infrastructure assets first."
            )

        if not dso_columns:
            raise SchemaValidationError(
                f"Table {full_table} does not exist in DSO database. "
                "Run DSO infrastructure assets first."
            )

        # Compare schemas
        differences = _compare_schemas(staging_columns, dso_columns)

        if differences:
            error_lines = [
                f"Schema mismatch detected for {full_table}:",
                "",
                "DIFFERENCES:",
            ]
            for diff in differences:
                error_lines.append(f"  - {diff}")

            error_lines.extend(
                [
                    "",
                    "REMEDIATION:",
                    "  1. Check if infrastructure assets need to be re-run",
                    "  2. Run 'staging_infra_*' assets to update staging schema",
                    "  3. Run 'dso_infra_*' assets to update DSO schema",
                    "  4. Both databases must use the same DDL from infrastructure/ddl/",
                    "",
                    "DO NOT manually modify table schemas - update DDL files instead.",
                ]
            )

            error_msg = "\n".join(error_lines)
            context.log.error(f"\n{error_msg}")
            raise SchemaValidationError(error_msg)

        context.log.info(
            f"✅ Schema validation passed: {full_table} ({len(staging_columns)} columns)"
        )

    except SchemaValidationError:
        raise  # Re-raise schema validation errors
    except Exception as e:
        # Connection or other errors - fail loudly, don't silently proceed
        error_msg = f"Failed to validate schemas for {full_table}: {e}"
        context.log.error(f"❌ {error_msg}")
        raise SchemaValidationError(error_msg) from e


def get_table_columns(
    mssql_target_dso: MSSQLResource,
    schema: str,
    table: str,
) -> list[str]:
    """
    Get column names for a table from the DSO database.

    Args:
        mssql_target_dso: DSO database resource
        schema: Schema name
        table: Table name

    Returns:
        List of column names
    """
    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
            """
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()


# Character SQL Server data types whose comparisons carry a collation, so a cross-
# database join/MERGE on two such columns needs COLLATE DATABASE_DEFAULT when the
# collations differ. Shared with the DQA dso_reconciliation check (single source of
# truth) so the two collation-handling paths can't drift.
STRING_DATA_TYPES = frozenset(
    {
        "char",
        "varchar",
        "nchar",
        "nvarchar",
        "text",
        "ntext",
    }
)


def get_string_pk_columns(
    mssql_target_dso: MSSQLResource,
    schema: str,
    table: str,
    pk_cols: list[str],
) -> set[str]:
    """Return the subset of *pk_cols* whose SQL Server data type is a string type.

    Used to decide which PK columns need COLLATE DATABASE_DEFAULT in MERGE ON
    clauses when staging and DSO databases use different collations.
    """
    if not pk_cols:
        return set()
    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            pk_in = ", ".join(f"'{c}'" for c in pk_cols)
            cursor.execute(
                f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table}'
              AND COLUMN_NAME IN ({pk_in})
            """
            )
            return {row[0] for row in cursor.fetchall() if row[1].lower() in STRING_DATA_TYPES}
        finally:
            cursor.close()


def build_merge_statement(
    config: SyncTableConfig,
    columns: list[str],
    linked_server: str = LINKED_SERVER_NAME,
    staging_db: str = STAGING_DATABASE,
    string_pk_cols: set[str] | None = None,
) -> str:
    """
    Build a MERGE SQL statement for syncing from staging to DSO via linked server.

    Args:
        config: Sync configuration
        columns: List of column names to sync
        linked_server: Name of the linked server
        staging_db: Name of the staging database
        string_pk_cols: PK columns with string data types (auto-detected at runtime).
            When provided, COLLATE DATABASE_DEFAULT is added for these columns.

    Returns:
        MERGE SQL statement string
    """
    # Normalize primary key to list
    pk_cols = [config.primary_key] if isinstance(config.primary_key, str) else config.primary_key

    # Filter out excluded columns
    sync_columns = [c for c in columns if c not in config.exclude_columns]

    # Detect the actual update timestamp column name
    update_ts_col = _find_update_timestamp_col(sync_columns)

    # Build column lists (exclude update timestamp -- set to SYSDATETIME() separately)
    update_columns = [
        c
        for c in sync_columns
        if c not in pk_cols and c not in config.insert_only_columns and c != update_ts_col
    ]

    # Source table reference (4-part naming)
    source_table = (
        f"[{linked_server}].[{staging_db}].[{config.staging_schema}].[{config.staging_table}]"
    )
    target_table = f"[{config.schema}].[{config.table}]"

    # Build ON clause for MERGE matching
    on_clause = _build_on_clause(pk_cols, config, string_pk_cols)

    # Build UPDATE SET clause
    update_set = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in update_columns)
    if update_ts_col:
        update_set += f", tgt.[{update_ts_col}] = CAST(SYSDATETIME() AS datetime2(0))"

    # Build INSERT column list and values
    insert_cols = ", ".join(f"[{c}]" for c in sync_columns)
    insert_vals = ", ".join(f"src.[{c}]" for c in sync_columns)

    merge_sql = f"""
        MERGE {target_table} AS tgt
        USING {source_table} AS src
        ON {on_clause}

        WHEN MATCHED THEN
            UPDATE SET {update_set}

        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
    """

    return merge_sql


def build_delta_merge_statement(
    config: SyncTableConfig,
    columns: list[str],
    last_sync_timestamp: datetime | None = None,
    linked_server: str = LINKED_SERVER_NAME,
    staging_db: str = STAGING_DATABASE,
    string_pk_cols: set[str] | None = None,
) -> str:
    """
    Build a delta MERGE SQL statement with watermark-based WHERE filter.

    Generates a MERGE that only processes rows changed since the last sync,
    using COALESCE on the watermark column with dw_timestamp_insert as fallback.
    This ensures rows are captured even if the primary watermark column is NULL.

    When last_sync_timestamp is None, falls back to build_merge_statement()
    for a full sync (used on first run when no previous watermark exists).

    Args:
        config: Sync configuration (must have watermark_column set for delta)
        columns: List of column names to sync
        last_sync_timestamp: Timestamp of last successful sync, or None for full sync
        linked_server: Name of the linked server
        staging_db: Name of the staging database
        string_pk_cols: PK columns with string data types (auto-detected at runtime).

    Returns:
        MERGE SQL statement string with delta WHERE filter
    """
    # Fall back to full merge when no timestamp is provided
    if last_sync_timestamp is None:
        return build_merge_statement(config, columns, linked_server, staging_db, string_pk_cols)

    # Normalize primary key to list
    pk_cols = [config.primary_key] if isinstance(config.primary_key, str) else config.primary_key

    # Filter out excluded columns
    sync_columns = [c for c in columns if c not in config.exclude_columns]

    # Detect the actual timestamp column names
    update_ts_col = _find_update_timestamp_col(sync_columns)
    insert_ts_col = _find_insert_timestamp_col(sync_columns)

    # Build column lists (exclude update timestamp -- set to SYSDATETIME() separately)
    update_columns = [
        c
        for c in sync_columns
        if c not in pk_cols and c not in config.insert_only_columns and c != update_ts_col
    ]

    # Source table reference (4-part naming)
    source_table = (
        f"[{linked_server}].[{staging_db}].[{config.staging_schema}].[{config.staging_table}]"
    )
    target_table = f"[{config.schema}].[{config.table}]"

    # Build watermark WHERE filter using COALESCE for NULL safety
    watermark_col = config.watermark_column or update_ts_col or "dw_timestamp_update"
    insert_fallback = insert_ts_col or "dw_timestamp_insert"
    timestamp_str = last_sync_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    where_clause = f"COALESCE([{watermark_col}], [{insert_fallback}]) >= '{timestamp_str}'"

    # Build ON clause for MERGE matching
    on_clause = _build_on_clause(pk_cols, config, string_pk_cols)

    # Build UPDATE SET clause
    update_set = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in update_columns)
    if update_ts_col:
        update_set += f", tgt.[{update_ts_col}] = CAST(SYSDATETIME() AS datetime2(0))"

    # Build INSERT column list and values
    insert_cols = ", ".join(f"[{c}]" for c in sync_columns)
    insert_vals = ", ".join(f"src.[{c}]" for c in sync_columns)

    merge_sql = f"""
        MERGE {target_table} AS tgt
        USING (
            SELECT * FROM {source_table}
            WHERE {where_clause}
        ) AS src
        ON {on_clause}

        WHEN MATCHED THEN
            UPDATE SET {update_set}

        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
    """

    return merge_sql


def _get_non_pk_nonclustered_index_names(
    cursor,
    schema: str,
    table: str,
) -> list[str]:
    """Return names of nonclustered, non-PK indexes on the given table (for disable/rebuild)."""
    cursor.execute(
        f"""
        SELECT i.name
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'{schema.replace("'", "''")}' AND t.name = N'{table.replace("'", "''")}'
          AND i.is_primary_key = 0 AND i.type = 2
        """
    )
    return [row[0] for row in cursor.fetchall()]


def build_truncate_insert_statement(
    config: SyncTableConfig,
    columns: list[str],
    linked_server: str = LINKED_SERVER_NAME,
    staging_db: str = STAGING_DATABASE,
) -> tuple[str, str]:
    """
    Build TRUNCATE and INSERT statements for full table refresh.

    Args:
        config: Sync configuration
        columns: List of column names to sync
        linked_server: Name of the linked server
        staging_db: Name of the staging database

    Returns:
        Tuple of (truncate_sql, insert_sql)
    """
    # Filter out excluded columns
    sync_columns = [c for c in columns if c not in config.exclude_columns]

    source_table = (
        f"[{linked_server}].[{staging_db}].[{config.staging_schema}].[{config.staging_table}]"
    )
    target_table = f"[{config.schema}].[{config.table}]"

    truncate_sql = f"TRUNCATE TABLE {target_table}"

    col_list = ", ".join(f"[{c}]" for c in sync_columns)
    insert_sql = f"""
        INSERT INTO {target_table} ({col_list})
        SELECT {col_list}
        FROM {source_table}
    """

    return truncate_sql, insert_sql


def sync_table_via_linked_server(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    config: SyncTableConfig,
    mssql_source_staging: MSSQLResource | None = None,
    validate_schemas: bool = True,
    auto_recover_mismatch: bool = True,
) -> dict:
    """
    Sync a table from staging to DSO using the linked server or Python fallback.

    This function:
    1. Validates the linked server is accessible (or same-database mode)
    2. Gets the table column structure
    3. Executes MERGE or TRUNCATE+INSERT based on config
    4. Returns sync statistics

    In same-database mode (staging and DSO share the same database),
    this function verifies data exists but skips the actual sync
    since both "databases" contain the same data.

    If linked servers are not available (e.g., Azure SQL Edge), falls back
    to Python-based data transfer when mssql_source_staging is provided.

    Args:
        context: Dagster asset execution context
        mssql_target_dso: DSO database resource
        config: Sync configuration
        mssql_source_staging: Optional staging resource for Python-based fallback
        auto_recover_mismatch: If True (default), compare row counts after watermark
            delta syncs and re-run as full MERGE if a mismatch is detected.

    Returns:
        Dictionary with sync statistics:
        - rows_affected: Number of rows inserted/updated
        - mode: Sync mode used
        - table: Full table name
    """
    full_table = f"[{config.schema}].[{config.table}]"

    # Check if we're in same-database mode (using resources if provided)
    same_db_mode = is_same_database_mode(mssql_target_dso, mssql_source_staging)

    if same_db_mode:
        # In same-database mode, staging and DSO share the same DB
        # No actual sync is needed - just verify data exists
        context.log.info(f"📦 Same-database mode: {full_table}")
        context.log.info("   -> Staging and DSO share the same database")

        # Get current row count
        with mssql_target_dso.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
                row_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"✅ Table {full_table} has {row_count} rows (sync skipped - same DB)")

        return {
            "rows_affected": 0,
            "rows_in_table": row_count,
            "mode": "same_database",
            "sync_engine": SYNC_ENGINE_SAME_DATABASE,
            "table": full_table,
            "columns_synced": 0,
            "sync_duration_seconds": 0.0,
            "rows_per_second": 0.0,
            "note": "Staging and DSO share the same database - no sync needed",
        }

    # Check if staging is reachable - if not and linked server unavailable,
    # this might be a local dev setup where all data is already in DSO
    staging_reachable = mssql_source_staging is not None and can_connect_to_staging(
        mssql_source_staging
    )

    if not staging_reachable:
        context.log.warning("⚠️ Staging database is not reachable")

        # Check if data already exists in DSO (local dev with shared database)
        with mssql_target_dso.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
                row_count = cursor.fetchone()[0]

                if row_count > 0:
                    context.log.info(f"📦 Table {full_table} already has {row_count} rows in DSO")
                    context.log.info(
                        "   -> Staging unreachable but DSO has data - treating as local dev mode"
                    )
                    return {
                        "rows_affected": 0,
                        "rows_in_table": row_count,
                        "mode": "local_dev_skip",
                        "sync_engine": SYNC_ENGINE_LOCAL_DEV_SKIP,
                        "table": full_table,
                        "columns_synced": 0,
                        "sync_duration_seconds": 0.0,
                        "rows_per_second": 0.0,
                        "note": "Staging unreachable, but DSO already has data",
                    }
            except Exception:
                pass  # Table might not exist in DSO yet
            finally:
                cursor.close()

        # No staging access and no data in DSO - this is a problem
        context.log.error("❌ Staging database unreachable and no data in DSO")
        raise RuntimeError(
            f"Cannot sync {full_table}: staging database unreachable and table doesn't exist in DSO. "
            "Check database configuration."
        )

    # STRICT VALIDATION - fail if schemas don't match
    # DSO layer should NEVER modify DSA layer schemas
    if mssql_source_staging is not None and validate_schemas:
        validate_table_schemas_match(
            context,
            mssql_source_staging,
            mssql_target_dso,
            config.schema,
            config.table,
            staging_schema=config.staging_schema,
            staging_table=config.staging_table,
        )

    # Try linked server mode first
    try:
        validate_linked_server(context, mssql_target_dso)
        result = _sync_via_linked_server(
            context, mssql_target_dso, config, mssql_source_staging, validate_schemas=False
        )
    except Exception as e:
        if _is_integrity_error(e):
            context.log.warning(
                f"⚠️ INTEGRITY_CONFLICT_RECOVERY: {full_table} — MERGE hit a unique "
                f"constraint violation: {str(e)[:300]}. "
                f"Falling back to TRUNCATE+INSERT for a clean full refresh."
            )
            recovery_config = dc_replace(config, mode="truncate_insert")
            try:
                result = _sync_via_linked_server(
                    context,
                    mssql_target_dso,
                    recovery_config,
                    mssql_source_staging=mssql_source_staging,
                    validate_schemas=False,
                )
            except RuntimeError:
                if mssql_source_staging is not None and staging_reachable:
                    result = _sync_via_python(
                        context,
                        mssql_source_staging,
                        mssql_target_dso,
                        recovery_config,
                    )
                else:
                    raise

            if config.mode == "watermark":
                from DSO.load.sync_state import SyncStateManager

                state_mgr = SyncStateManager(mssql_target_dso, context)
                state_mgr.update_sync_state(
                    config.schema,
                    config.table,
                    rows_affected=result["rows_affected"],
                    duration_ms=result.get("duration_ms", 0),
                    status="recovery_truncate_insert",
                )

            result["recovery_truncate_insert"] = True
            result["recovery_reason"] = "integrity_constraint_violation"
            context.log.info(
                f"✅ INTEGRITY_CONFLICT_RECOVERY COMPLETE: {full_table} — "
                f"synced {result['rows_affected']} rows via TRUNCATE+INSERT"
            )
        elif isinstance(e, RuntimeError) or _is_retryable_connectivity_error(e):
            # The retry-exhausted linked-server path re-raises its original
            # exception unchanged, which for mssql_python is ProgrammingError
            # (not RuntimeError) — match it the same way the retry classifier does
            # so a connectivity fault still falls back instead of failing hard.
            err_msg = str(e)
            if "does not exist" in err_msg:
                fallback_reason = FALLBACK_REASON_LINKED_SERVER_NOT_AVAILABLE
            elif "not accessible" in err_msg:
                fallback_reason = FALLBACK_REASON_LINKED_SERVER_VALIDATION_FAILED
            else:
                fallback_reason = FALLBACK_REASON_CONNECTIVITY

            allow_fallback = os.environ.get(
                DSO_SYNC_ALLOW_PYTHON_FALLBACK_ENV, "true"
            ).lower() not in ("false", "0")
            if not allow_fallback:
                context.log.error(
                    f"Linked server unavailable (fallback disabled): reason={fallback_reason}, "
                    f"error={err_msg}. "
                    f"Set {DSO_SYNC_ALLOW_PYTHON_FALLBACK_ENV}=true to allow Python fallback."
                )
                raise

            if mssql_source_staging is not None and staging_reachable:
                context.log.error(
                    f"DSO_SYNC_FALLBACK_ACTIVE: using Python fallback for {full_table}; "
                    f"reason={fallback_reason}, linked_server_error={err_msg[:200]}"
                )
                context.log.info("Using Python-based data transfer as fallback...")
                result = _sync_via_python(
                    context,
                    mssql_source_staging,
                    mssql_target_dso,
                    config,
                    fallback_reason=fallback_reason,
                )
            else:
                raise
        else:
            raise

    # --- Mismatch recovery for watermark delta syncs ---
    if (
        auto_recover_mismatch
        and config.mode == "watermark"
        and result.get("mode") == "watermark_delta"
    ):
        from DSO.load.sync_state import SyncStateManager

        counts = compare_row_counts(
            context,
            mssql_target_dso,
            config.schema,
            config.table,
            mssql_source_staging=mssql_source_staging,
        )
        if not counts["match"]:
            context.log.warning(
                f"⚠️ ROW_COUNT_MISMATCH_RECOVERY: {full_table} "
                f"staging={counts['staging_count']}, dso={counts['dso_count']}. "
                f"Re-running as full MERGE to recover."
            )
            recovery_config = dc_replace(config, mode="merge")

            # Re-run through the same engine path
            try:
                validate_linked_server(context, mssql_target_dso)
                result = _sync_via_linked_server(
                    context,
                    mssql_target_dso,
                    recovery_config,
                    mssql_source_staging=mssql_source_staging,
                    validate_schemas=False,
                )
            except RuntimeError:
                if mssql_source_staging is not None and staging_reachable:
                    result = _sync_via_python(
                        context,
                        mssql_source_staging,
                        mssql_target_dso,
                        recovery_config,
                    )
                else:
                    raise

            # Advance watermark after recovery (mode="merge" doesn't auto-advance)
            state_mgr = SyncStateManager(mssql_target_dso, context)
            state_mgr.update_sync_state(
                config.schema,
                config.table,
                rows_affected=result["rows_affected"],
                duration_ms=result.get("duration_ms", 0),
                status="recovery_full_merge",
            )

            result["recovery_full_merge"] = True
            result["pre_recovery_staging_count"] = counts["staging_count"]
            result["pre_recovery_dso_count"] = counts["dso_count"]

            # Verify recovery succeeded — no retry, just report
            post_counts = compare_row_counts(
                context,
                mssql_target_dso,
                config.schema,
                config.table,
                mssql_source_staging=mssql_source_staging,
            )
            if not post_counts["match"]:
                context.log.error(
                    f"❌ RECOVERY FAILED: {full_table} — row counts still "
                    f"mismatched after full MERGE "
                    f"(staging={post_counts['staging_count']}, "
                    f"dso={post_counts['dso_count']}). "
                    f"Will NOT retry — investigate manually."
                )
                result["recovery_failed"] = True
            else:
                context.log.info(
                    f"✅ RECOVERY COMPLETE: {full_table} — "
                    f"synced {result['rows_affected']} rows via full MERGE"
                )

    return result


def _sync_via_linked_server_batched(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    config: SyncTableConfig,
    last_sync_timestamp: datetime | None,
    columns: list[str],
    string_pk_cols: set[str],
) -> dict:
    """Execute sync by staging linked server data locally, then merging.

    Phase 1: Batch-INSERT rows from the linked server into a local
    ``#_sync_staging`` temp table using OPENQUERY with keyset pagination.
    Phase 2: Perform a single local MERGE (or INSERT for truncate_insert)
    from the temp table into the target.

    This eliminates the cross-network MERGE entirely.  The network-dependent
    part is simple SELECTs; the MERGE runs locally.
    """
    full_table = f"[{config.schema}].[{config.table}]"
    pk_col = config.primary_key  # guaranteed str by caller
    batch_size = config.linked_server_batch_size

    sync_columns = [c for c in columns if c not in config.exclude_columns]
    col_list = ", ".join(f"[{c}]" for c in sync_columns)

    update_ts_col = _find_update_timestamp_col(sync_columns)
    insert_ts_col = _find_insert_timestamp_col(sync_columns)

    watermark_where = ""
    if config.mode == "watermark" and last_sync_timestamp is not None:
        wm_col = config.watermark_column or update_ts_col or "dw_timestamp_update"
        ins_col = insert_ts_col or "dw_timestamp_insert"
        ts_str = last_sync_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        watermark_where = f" AND COALESCE([{wm_col}], [{ins_col}]) >= ''{ts_str}''"

    def _batch_select_sql(last_pk_value) -> str:
        """Return INSERT INTO #_sync_staging ... OPENQUERY(...) for one batch."""
        if last_pk_value is None:
            pk_filter = ""
        elif isinstance(last_pk_value, str):
            escaped = last_pk_value.replace("'", "''''")
            pk_filter = f" AND [{pk_col}] > ''{escaped}''"
        else:
            pk_filter = f" AND [{pk_col}] > {last_pk_value}"

        inner_sql = (
            f"SELECT TOP({batch_size}) {col_list} "
            f"FROM [{STAGING_DATABASE}].[{config.staging_schema}].[{config.staging_table}] "
            f"WHERE 1=1{pk_filter}{watermark_where} "
            f"ORDER BY [{pk_col}]"
        )
        return (
            f"INSERT INTO #_sync_staging ({col_list}) "
            f"SELECT * FROM OPENQUERY([{LINKED_SERVER_NAME}], '{inner_sql}')"
        )

    def _max_pk_sql(last_pk_value) -> str:
        """Return an OPENQUERY that fetches the MAX PK from the same batch window.

        Uses the staging server's collation for MAX(), which must match the
        ORDER BY collation used in _batch_select_sql to keep keyset pagination
        monotonically advancing.
        """
        if last_pk_value is None:
            pk_filter = ""
        elif isinstance(last_pk_value, str):
            escaped = last_pk_value.replace("'", "''''")
            pk_filter = f" AND [{pk_col}] > ''{escaped}''"
        else:
            pk_filter = f" AND [{pk_col}] > {last_pk_value}"

        inner = (
            f"SELECT MAX([{pk_col}]) FROM ("
            f"SELECT TOP({batch_size}) [{pk_col}] "
            f"FROM [{STAGING_DATABASE}].[{config.staging_schema}].[{config.staging_table}] "
            f"WHERE 1=1{pk_filter}{watermark_where} "
            f"ORDER BY [{pk_col}]"
            f") sub"
        )
        return f"SELECT * FROM OPENQUERY([{LINKED_SERVER_NAME}], '{inner}')"

    def _count_sql() -> str:
        inner = (
            f"SELECT COUNT(*) FROM [{STAGING_DATABASE}].[{config.staging_schema}].[{config.staging_table}] "
            f"WHERE 1=1{watermark_where}"
        )
        return f"SELECT * FROM OPENQUERY([{LINKED_SERVER_NAME}], '{inner}')"

    def _build_local_merge_sql() -> str:
        pk_cols = [pk_col]
        on_clause = _build_on_clause(pk_cols, config, string_pk_cols)

        update_columns = [
            c
            for c in sync_columns
            if c not in pk_cols and c not in config.insert_only_columns and c != update_ts_col
        ]
        update_set = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in update_columns)
        if update_ts_col:
            update_set += f", tgt.[{update_ts_col}] = CAST(SYSDATETIME() AS datetime2(0))"

        insert_cols = ", ".join(f"[{c}]" for c in sync_columns)
        insert_vals = ", ".join(f"src.[{c}]" for c in sync_columns)

        return (
            f"MERGE {full_table} AS tgt "
            f"USING #_sync_staging AS src "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN "
            f"UPDATE SET {update_set} "
            f"WHEN NOT MATCHED BY TARGET THEN "
            f"INSERT ({insert_cols}) "
            f"VALUES ({insert_vals});"
        )

    def _open_conn_and_create_temp(
        pk_cols_list: list[str] | None = None,
    ) -> tuple:
        """Open a fresh DSO connection and create the #_sync_staging temp table.

        Uses the resource's raw-connection builder (not the get_connection()
        contextmanager) on purpose: the batch retry loop must close and reopen
        this connection on a transient connectivity error while recreating the
        temp table — which the contextmanager (auto-close on exception) cannot
        express. Routing through the resource keeps the connection string,
        login-timeout, and Dagster-secret resolution identical to every other
        path. The caller owns the lifecycle and closes it in the finally block.
        """
        conn = mssql_target_dso.get_raw_connection()
        try:
            cursor = conn.cursor()
            if config.identity_insert and pk_cols_list:
                cols_expr = ", ".join(
                    f"([{c}] + 0) AS [{c}]" if c in pk_cols_list else f"[{c}]" for c in sync_columns
                )
                cursor.execute(f"SELECT TOP 0 {cols_expr} INTO #_sync_staging FROM {full_table}")
            else:
                cursor.execute(f"SELECT TOP 0 {col_list} INTO #_sync_staging FROM {full_table}")
        except BaseException:
            # The caller only owns this connection once we return it; if temp-table
            # setup fails first, close it here so a reconnect attempt cannot leak it.
            try:
                conn.close()
            except Exception:
                pass
            raise
        return conn, cursor

    # ── main execution ──────────────────────────────────────────────
    sync_start = time.monotonic()
    pk_cols_list = [pk_col] if config.identity_insert else None

    conn, cursor = _open_conn_and_create_temp(pk_cols_list)

    try:
        # Phase 0: count source rows. On a transient connectivity error, reconnect
        # + recreate the temp so Phase 1 starts on a healthy session (else it dies
        # on a misleading "Invalid object name '#_sync_staging'").
        try:
            cursor.execute(_count_sql())
            source_count = cursor.fetchone()[0]
            estimated_batches = math.ceil(source_count / batch_size) if source_count > 0 else 1
            context.log.info(
                f"Batched sync {full_table}: {source_count:,} source rows, "
                f"~{estimated_batches} batches of {batch_size:,}"
            )
        except Exception as e:
            source_count = None
            estimated_batches = None
            if _is_retryable_connectivity_error(e):
                context.log.warning(
                    f"Count for {full_table} failed with a transient connectivity "
                    f"error ({str(e)[:200]}); reconnecting before Phase 1."
                )
                _safe_close(cursor, conn)
                conn, cursor = _open_conn_and_create_temp(pk_cols_list)
            else:
                context.log.warning(f"Could not count source rows: {e}")

        # Phase 1: batch-INSERT via OPENQUERY (keyset paginated, reconnecting on a
        # transient connectivity error). The MAX(pk) cursor is resolved on the
        # *staging* server so it uses the staging collation, matching the batch
        # ORDER BY — a DSO-side MAX() could differ and never advance.
        def _next_pk(cur: object, last_pk_value: object) -> object:
            cur.execute(_max_pk_sql(last_pk_value))
            row = cur.fetchone()
            if row is None or row[0] is None:
                return None
            return row[0]

        total_rows, conn, cursor = _stage_batches_via_openquery(
            context,
            full_table,
            conn,
            cursor,
            _batch_select_sql,
            _next_pk,
            lambda: _open_conn_and_create_temp(pk_cols_list),
            estimated_batches,
            sync_start,
        )

        if total_rows == 0:
            duration_ms = int((time.monotonic() - sync_start) * 1000)
            effective_mode = config.mode
            if config.mode == "watermark":
                effective_mode = (
                    "watermark_delta" if last_sync_timestamp is not None else "watermark_full"
                )
            return {
                "rows_affected": 0,
                "mode": effective_mode,
                "sync_engine": SYNC_ENGINE_LINKED_SERVER,
                "table": full_table,
                "columns_synced": len(sync_columns),
                "duration_ms": duration_ms,
                "sync_duration_seconds": round(duration_ms / 1000.0, 2),
                "rows_per_second": 0.0,
            }

        # Phase 2: Local MERGE (or INSERT for truncate_insert)
        disabled_indexes: list[str] = []
        if config.disable_target_indexes_during_sync:
            disabled_indexes = _get_non_pk_nonclustered_index_names(
                cursor, config.schema, config.table
            )
            for idx_name in disabled_indexes:
                cursor.execute(f"ALTER INDEX [{idx_name}] ON {full_table} DISABLE")
            if disabled_indexes:
                context.log.info(
                    f"Disabled {len(disabled_indexes)} nonclustered indexes on {full_table}"
                )

        if config.identity_insert:
            context.log.info(f"Enabling IDENTITY_INSERT for {full_table}")
            cursor.execute(f"SET IDENTITY_INSERT {full_table} ON")

        try:
            if config.mode == "truncate_insert":
                context.log.info(f"Phase 2: TRUNCATE + INSERT into {full_table}")
                cursor.execute(f"TRUNCATE TABLE {full_table}")
                cursor.execute(
                    f"INSERT INTO {full_table} ({col_list}) "
                    f"SELECT {col_list} FROM #_sync_staging"
                )
                rows_affected = cursor.rowcount
            else:
                # Empty-target short-circuit: MERGE is always fully logged, but a
                # MERGE into an empty target is a pure insert. When the DSO target
                # has no rows (first load / rebuilt table), bulk INSERT WITH
                # (TABLOCK) instead — minimally logged under bulk-logged/simple
                # recovery. Unlike the DSA staging temp tables, the staged snapshot
                # here carries its own keys (incl. any -1 sentinel row), so the
                # guard is a plain emptiness check, NOT sentinel-aware (inserting
                # the sentinel into a sentinel-only target would collide).
                cursor.execute(
                    f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {full_table}) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    merge_sql = _build_local_merge_sql()
                    context.log.info(f"Phase 2: Local MERGE into {full_table}")
                    _clear_stale_current_before_merge(
                        cursor, config, "#_sync_staging", string_pk_cols
                    )
                    cursor.execute(merge_sql)
                    rows_affected = cursor.rowcount
                else:
                    context.log.info(
                        f"Phase 2: target empty - bulk INSERT WITH (TABLOCK) into {full_table} "
                        "(minimally logged)"
                    )
                    cursor.execute(
                        f"INSERT INTO {full_table} WITH (TABLOCK) ({col_list}) "
                        f"SELECT {col_list} FROM #_sync_staging"
                    )
                    rows_affected = cursor.rowcount

        finally:
            if config.identity_insert:
                context.log.info(f"Disabling IDENTITY_INSERT for {full_table}")
                cursor.execute(f"SET IDENTITY_INSERT {full_table} OFF")
            for idx_name in disabled_indexes:
                try:
                    cursor.execute(f"ALTER INDEX [{idx_name}] ON {full_table} REBUILD")
                except Exception as e:
                    context.log.error(f"Failed to rebuild index [{idx_name}] on {full_table}: {e}")
                    raise
            if disabled_indexes:
                context.log.info(f"Rebuilt {len(disabled_indexes)} indexes on {full_table}")

        conn.commit()

    finally:
        try:
            cursor.execute(
                "IF OBJECT_ID('tempdb..#_sync_staging') IS NOT NULL " "DROP TABLE #_sync_staging"
            )
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    duration_ms = int((time.monotonic() - sync_start) * 1000)
    sync_duration_seconds = duration_ms / 1000.0
    rows_per_second = rows_affected / sync_duration_seconds if sync_duration_seconds > 0 else 0.0

    effective_mode = config.mode
    if config.mode == "watermark" and last_sync_timestamp is not None:
        effective_mode = "watermark_delta"
    elif config.mode == "watermark":
        effective_mode = "watermark_full"

    context.log.info(
        f"Synced {rows_affected:,} rows to {full_table} via batched linked server "
        f"(mode={effective_mode}, duration={duration_ms}ms, rows/s={rows_per_second:,.0f})"
    )

    return {
        "rows_affected": rows_affected,
        "mode": effective_mode,
        "sync_engine": SYNC_ENGINE_LINKED_SERVER,
        "table": full_table,
        "columns_synced": len(sync_columns),
        "duration_ms": duration_ms,
        "sync_duration_seconds": sync_duration_seconds,
        "rows_per_second": round(rows_per_second, 1),
    }


def _sql_literal(value) -> str:
    """Render a Python value as a SQL literal for OPENQUERY inner SQL."""
    if value is None:
        return "NULL"
    if isinstance(value, int | float | Decimal):
        return str(value)
    if isinstance(value, datetime):
        return f"''{value.strftime('%Y-%m-%d %H:%M:%S')}''"
    if isinstance(value, date):
        return f"''{value.strftime('%Y-%m-%d')}''"
    escaped = str(value).replace("'", "''''")
    return f"''{escaped}''"


def _build_composite_keyset_predicate(
    pk_cols: list[str],
    last_pk_values: tuple | None,
) -> str:
    """Build lexicographic keyset pagination predicate for composite PKs."""
    if last_pk_values is None:
        return ""

    disjuncts: list[str] = []
    for idx, col in enumerate(pk_cols):
        equal_prefix = " AND ".join(
            f"[{pk_cols[prefix_idx]}] = {_sql_literal(last_pk_values[prefix_idx])}"
            for prefix_idx in range(idx)
        )
        greater_than = f"[{col}] > {_sql_literal(last_pk_values[idx])}"
        disjuncts.append(
            f"({equal_prefix} AND {greater_than})" if equal_prefix else f"({greater_than})"
        )

    return " AND (" + " OR ".join(disjuncts) + ")"


def _sync_via_linked_server_batched_composite(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    config: SyncTableConfig,
    last_sync_timestamp: datetime | None,
    columns: list[str],
    string_pk_cols: set[str],
) -> dict:
    """Stage composite-PK linked-server data locally, then MERGE on DSO."""
    full_table = f"[{config.schema}].[{config.table}]"
    pk_cols = (
        [config.primary_key] if isinstance(config.primary_key, str) else list(config.primary_key)
    )
    batch_size = config.linked_server_batch_size

    sync_columns = [c for c in columns if c not in config.exclude_columns]
    col_list = ", ".join(f"[{c}]" for c in sync_columns)
    pk_list = ", ".join(f"[{c}]" for c in pk_cols)
    order_by_pk = ", ".join(f"[{c}]" for c in pk_cols)
    order_by_pk_desc = ", ".join(f"[{c}] DESC" for c in pk_cols)

    update_ts_col = _find_update_timestamp_col(sync_columns)
    insert_ts_col = _find_insert_timestamp_col(sync_columns)

    watermark_where = ""
    if config.mode == "watermark" and last_sync_timestamp is not None:
        wm_col = config.watermark_column or update_ts_col or "dw_timestamp_update"
        ins_col = insert_ts_col or "dw_timestamp_insert"
        ts_str = last_sync_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        watermark_where = f" AND COALESCE([{wm_col}], [{ins_col}]) >= ''{ts_str}''"

    def _batch_inner_sql(last_pk_values: tuple | None, select_cols: str) -> str:
        pk_filter = _build_composite_keyset_predicate(pk_cols, last_pk_values)
        return (
            f"SELECT TOP({batch_size}) {select_cols} "
            f"FROM [{STAGING_DATABASE}].[{config.staging_schema}].[{config.staging_table}] "
            f"WHERE 1=1{pk_filter}{watermark_where} "
            f"ORDER BY {order_by_pk}"
        )

    def _batch_select_sql(last_pk_values: tuple | None) -> str:
        inner_sql = _batch_inner_sql(last_pk_values, col_list)
        return (
            f"INSERT INTO #_sync_staging ({col_list}) "
            f"SELECT * FROM OPENQUERY([{LINKED_SERVER_NAME}], '{inner_sql}')"
        )

    def _last_pk_sql(last_pk_values: tuple | None) -> str:
        inner = (
            f"SELECT TOP(1) {pk_list} FROM ("
            f"{_batch_inner_sql(last_pk_values, pk_list)}"
            f") sub ORDER BY {order_by_pk_desc}"
        )
        return f"SELECT * FROM OPENQUERY([{LINKED_SERVER_NAME}], '{inner}')"

    def _count_sql() -> str:
        inner = (
            f"SELECT COUNT(*) FROM [{STAGING_DATABASE}].[{config.staging_schema}].[{config.staging_table}] "
            f"WHERE 1=1{watermark_where}"
        )
        return f"SELECT * FROM OPENQUERY([{LINKED_SERVER_NAME}], '{inner}')"

    def _build_local_merge_sql() -> str:
        on_clause = _build_on_clause(pk_cols, config, string_pk_cols)
        update_columns = [
            c
            for c in sync_columns
            if c not in pk_cols and c not in config.insert_only_columns and c != update_ts_col
        ]
        update_set = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in update_columns)
        if update_ts_col:
            update_set += f", tgt.[{update_ts_col}] = CAST(SYSDATETIME() AS datetime2(0))"

        insert_cols = ", ".join(f"[{c}]" for c in sync_columns)
        insert_vals = ", ".join(f"src.[{c}]" for c in sync_columns)

        return (
            f"MERGE {full_table} AS tgt "
            f"USING #_sync_staging AS src "
            f"ON {on_clause} "
            f"WHEN MATCHED THEN "
            f"UPDATE SET {update_set} "
            f"WHEN NOT MATCHED BY TARGET THEN "
            f"INSERT ({insert_cols}) "
            f"VALUES ({insert_vals});"
        )

    def _open_conn_and_create_temp() -> tuple:
        """Open a fresh DSO connection (via the resource) and create the
        #_sync_staging temp table.

        Uses a raw (non-contextmanager) connection so the batch retry loop can
        close and reopen it on a transient connectivity error while recreating
        the temp table — mirroring the single-PK path. Closes its own connection
        if temp-table setup fails so a failed reconnect cannot leak it. The
        caller owns the lifecycle and closes it in the finally block below.
        """
        conn = mssql_target_dso.get_raw_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT TOP 0 {col_list} INTO #_sync_staging FROM {full_table}")
        except BaseException:
            try:
                conn.close()
            except Exception:
                pass
            raise
        return conn, cursor

    sync_start = time.monotonic()
    conn, cursor = _open_conn_and_create_temp()

    try:
        # Phase 0: Count source rows for progress logging. On a transient
        # connectivity error, reconnect + recreate the temp table so Phase 1
        # starts on a healthy session (see _sync_via_linked_server_batched).
        try:
            cursor.execute(_count_sql())
            source_count = cursor.fetchone()[0]
            estimated_batches = math.ceil(source_count / batch_size) if source_count > 0 else 1
            context.log.info(
                f"Batched composite sync {full_table}: {source_count:,} source rows, "
                f"~{estimated_batches} batches of {batch_size:,}"
            )
        except Exception as e:
            estimated_batches = None
            if _is_retryable_connectivity_error(e):
                context.log.warning(
                    f"Count for {full_table} failed with a transient connectivity "
                    f"error ({str(e)[:200]}); reconnecting before Phase 1."
                )
                _safe_close(cursor, conn)
                conn, cursor = _open_conn_and_create_temp()
            else:
                context.log.warning(f"Could not count source rows: {e}")

        # Phase 1: batch-INSERT via OPENQUERY (composite keyset paginated,
        # reconnecting on a transient connectivity error).
        def _next_pk(cur: object, last_pk_value: object) -> object:
            cur.execute(_last_pk_sql(last_pk_value))
            row = cur.fetchone()
            return None if row is None else tuple(row)

        total_rows, conn, cursor = _stage_batches_via_openquery(
            context,
            full_table,
            conn,
            cursor,
            _batch_select_sql,
            _next_pk,
            _open_conn_and_create_temp,
            estimated_batches,
            sync_start,
        )

        if config.mode == "truncate_insert":
            # Always TRUNCATE — even at 0 source rows — so a full refresh clears
            # stale target rows when the source has become empty. (This is the
            # intended composite behaviour and differs from the single-PK path,
            # which early-returns at 0 rows; see the truncate-when-empty test.)
            context.log.info(f"Phase 2: TRUNCATE + INSERT into {full_table}")
            cursor.execute(f"TRUNCATE TABLE {full_table}")
            if total_rows == 0:
                rows_affected = 0
            else:
                cursor.execute(
                    f"INSERT INTO {full_table} ({col_list}) "
                    f"SELECT {col_list} FROM #_sync_staging"
                )
                rows_affected = cursor.rowcount
        elif total_rows == 0:
            rows_affected = 0
        else:
            # Empty-target short-circuit (see _sync_via_linked_server_batched):
            # bulk INSERT WITH (TABLOCK) when the DSO target is empty (minimally
            # logged), MERGE otherwise. Plain emptiness check, not sentinel-aware.
            cursor.execute(
                f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {full_table}) THEN 1 ELSE 0 END"
            )
            if cursor.fetchone()[0]:
                context.log.info(f"Phase 2: Local composite MERGE into {full_table}")
                _clear_stale_current_before_merge(cursor, config, "#_sync_staging", string_pk_cols)
                cursor.execute(_build_local_merge_sql())
                rows_affected = cursor.rowcount
            else:
                context.log.info(
                    f"Phase 2: target empty - bulk INSERT WITH (TABLOCK) into {full_table} "
                    "(minimally logged)"
                )
                cursor.execute(
                    f"INSERT INTO {full_table} WITH (TABLOCK) ({col_list}) "
                    f"SELECT {col_list} FROM #_sync_staging"
                )
                rows_affected = cursor.rowcount

        conn.commit()
    finally:
        try:
            cursor.execute(
                "IF OBJECT_ID('tempdb..#_sync_staging') IS NOT NULL " "DROP TABLE #_sync_staging"
            )
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    duration_ms = int((time.monotonic() - sync_start) * 1000)
    sync_duration_seconds = duration_ms / 1000.0
    rows_per_second = rows_affected / sync_duration_seconds if sync_duration_seconds > 0 else 0.0

    effective_mode = config.mode
    if config.mode == "watermark" and last_sync_timestamp is not None:
        effective_mode = "watermark_delta"
    elif config.mode == "watermark":
        effective_mode = "watermark_full"

    context.log.info(
        f"Synced {rows_affected:,} rows to {full_table} via batched composite linked server "
        f"(mode={effective_mode}, duration={duration_ms}ms, "
        f"rows/s={rows_per_second:,.0f})"
    )

    return {
        "rows_affected": rows_affected,
        "mode": effective_mode,
        "sync_engine": SYNC_ENGINE_LINKED_SERVER,
        "table": full_table,
        "columns_synced": len(sync_columns),
        "duration_ms": duration_ms,
        "sync_duration_seconds": sync_duration_seconds,
        "rows_per_second": round(rows_per_second, 1),
    }


def _sync_via_linked_server(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    config: SyncTableConfig,
    mssql_source_staging: MSSQLResource | None = None,
    validate_schemas: bool = False,
) -> dict:
    """Execute sync using linked server SQL.

    Behaviour depends on config.mode:
    - 'merge': Full MERGE of all rows via build_merge_statement().
    - 'watermark': Reads last sync timestamp from SyncStateManager, uses
      build_delta_merge_statement() to only sync changed rows. Falls back
      to full merge when no previous timestamp exists (first run). Updates
      SyncStateManager after a successful sync.
    - 'truncate_insert': TRUNCATE + INSERT for full table refresh.

    Schema validation should be done by the caller (sync_table_via_linked_server).
    This function assumes schemas have already been validated.
    """
    from DSO.load.sync_state import SyncStateManager

    full_table = f"[{config.schema}].[{config.table}]"

    # Schema validation is now done by caller - only validate here if explicitly requested
    if validate_schemas and mssql_source_staging is not None:
        validate_table_schemas_match(
            context,
            mssql_source_staging,
            mssql_target_dso,
            config.schema,
            config.table,
            staging_schema=config.staging_schema,
            staging_table=config.staging_table,
        )

    # Get column structure (from DSO as it's the target schema)
    columns = get_table_columns(mssql_target_dso, config.schema, config.table)
    if not columns:
        raise RuntimeError(f"Table {full_table} not found in DSO database or has no columns")

    # Validate watermark_column exists in table columns
    if config.mode == "watermark":
        if config.watermark_column not in columns:
            raise ValueError(
                f"watermark_column '{config.watermark_column}' not found in "
                f"table {full_table}. Available columns: {columns}"
            )

    # Only apply COLLATE DATABASE_DEFAULT when staging and DSO collations differ.
    pk_cols = (
        [config.primary_key] if isinstance(config.primary_key, str) else list(config.primary_key)
    )
    needs_collate = mssql_source_staging is not None and check_collation_mismatch(
        mssql_source_staging, mssql_target_dso, context
    )
    string_pk_cols: set[str] = (
        get_string_pk_columns(mssql_target_dso, config.schema, config.table, pk_cols)
        if needs_collate
        else set()
    )

    # Resolve delta state for watermark mode
    last_sync_timestamp: datetime | None = None
    if config.mode == "watermark":
        state_mgr = SyncStateManager(mssql_target_dso, context)
        last_sync_timestamp = state_mgr.get_last_sync_timestamp(config.schema, config.table)
        if last_sync_timestamp is not None:
            context.log.info(
                f"Watermark mode: column={config.watermark_column}, "
                f"last_sync={last_sync_timestamp.isoformat()}"
            )
        else:
            context.log.info(
                "Watermark mode: no previous sync found — executing full sync (first run)"
            )

    # Delegate to batched path for single-PK tables
    if isinstance(config.primary_key, str):
        context.log.info(
            f"Syncing {full_table} with {len(columns)} columns via batched linked server "
            f"(batch_size={config.linked_server_batch_size})"
        )
        result = _sync_via_linked_server_batched(
            context,
            mssql_target_dso,
            config,
            last_sync_timestamp=last_sync_timestamp,
            columns=columns,
            string_pk_cols=string_pk_cols,
        )
        if config.mode == "watermark":
            if result["rows_affected"] > 0:
                state_mgr.update_sync_state(
                    config.schema,
                    config.table,
                    rows_affected=result["rows_affected"],
                    duration_ms=result.get("duration_ms", 0),
                    status="success",
                )
            else:
                context.log.info(f"Watermark not advanced for {full_table} — 0 rows synced")
        return result

    # Composite PK tables use the same local-staging principle as single-PK
    # tables. Avoid distributed MERGE over linked server for large/wide tables
    # such as fhir.QuestionnaireResponse_item.
    if len(pk_cols) > 1:
        context.log.info(
            f"Syncing {full_table} with {len(columns)} columns via batched composite "
            f"linked server (batch_size={config.linked_server_batch_size})"
        )
        result = _sync_via_linked_server_batched_composite(
            context,
            mssql_target_dso,
            config,
            last_sync_timestamp=last_sync_timestamp,
            columns=columns,
            string_pk_cols=string_pk_cols,
        )
        if config.mode == "watermark":
            if result["rows_affected"] > 0:
                state_mgr.update_sync_state(
                    config.schema,
                    config.table,
                    rows_affected=result["rows_affected"],
                    duration_ms=result.get("duration_ms", 0),
                    status="success",
                )
            else:
                context.log.info(f"Watermark not advanced for {full_table} — 0 rows synced")
        return result

    # ── Fallback for any remaining non-batched cases ──────
    context.log.info(f"Syncing {full_table} with {len(columns)} columns via linked server")

    sync_start = time.monotonic()

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()

        # Optionally disable non-PK nonclustered indexes on target to reduce MERGE cost
        disabled_indexes: list[str] = []
        if config.disable_target_indexes_during_sync:
            disabled_indexes = _get_non_pk_nonclustered_index_names(
                cursor, config.schema, config.table
            )
            for idx_name in disabled_indexes:
                cursor.execute(f"ALTER INDEX [{idx_name}] ON {full_table} DISABLE")
            if disabled_indexes:
                context.log.info(
                    f"Disabled {len(disabled_indexes)} nonclustered indexes on {full_table} for sync"
                )

        # Enable IDENTITY_INSERT if needed (when PK is an IDENTITY column)
        if config.identity_insert:
            context.log.info(f"Enabling IDENTITY_INSERT for {full_table}")
            cursor.execute(f"SET IDENTITY_INSERT {full_table} ON")

        try:
            if config.mode == "truncate_insert":
                # Full refresh mode
                truncate_sql, insert_sql = build_truncate_insert_statement(config, columns)

                context.log.info(f"Executing TRUNCATE on {full_table}")
                cursor.execute(truncate_sql)

                context.log.info("Executing INSERT from staging via linked server")
                cursor.execute(insert_sql)
                rows_affected = cursor.rowcount

            elif config.mode == "watermark" and last_sync_timestamp is not None:
                # Delta MERGE mode — only sync rows changed since last watermark
                merge_sql = build_delta_merge_statement(
                    config,
                    columns,
                    last_sync_timestamp=last_sync_timestamp,
                    string_pk_cols=string_pk_cols,
                )

                context.log.info(
                    f"Executing delta MERGE from staging via linked server "
                    f"(rows where {config.watermark_column} >= "
                    f"'{last_sync_timestamp.strftime('%Y-%m-%d %H:%M:%S')}')"
                )
                source_from = (
                    f"[{LINKED_SERVER_NAME}].[{STAGING_DATABASE}]"
                    f".[{config.staging_schema}].[{config.staging_table}]"
                )
                _clear_stale_current_before_merge(cursor, config, source_from, string_pk_cols)
                cursor.execute(merge_sql)
                rows_affected = cursor.rowcount

            else:
                # Full MERGE (mode='merge', or mode='watermark' first-run fallback)
                merge_sql = build_merge_statement(config, columns, string_pk_cols=string_pk_cols)

                context.log.info("Executing MERGE from staging via linked server")
                source_from = (
                    f"[{LINKED_SERVER_NAME}].[{STAGING_DATABASE}]"
                    f".[{config.staging_schema}].[{config.staging_table}]"
                )
                _clear_stale_current_before_merge(cursor, config, source_from, string_pk_cols)
                cursor.execute(merge_sql)
                rows_affected = cursor.rowcount

        finally:
            if config.identity_insert:
                context.log.info(f"Disabling IDENTITY_INSERT for {full_table}")
                cursor.execute(f"SET IDENTITY_INSERT {full_table} OFF")
            # Rebuild indexes that were disabled before MERGE
            for idx_name in disabled_indexes:
                try:
                    cursor.execute(f"ALTER INDEX [{idx_name}] ON {full_table} REBUILD")
                except Exception as e:
                    context.log.error(f"Failed to rebuild index [{idx_name}] on {full_table}: {e}")
                    raise
            if disabled_indexes:
                context.log.info(f"Rebuilt {len(disabled_indexes)} indexes on {full_table}")
            cursor.close()

        conn.commit()

    duration_ms = int((time.monotonic() - sync_start) * 1000)
    sync_duration_seconds = duration_ms / 1000.0
    rows_per_second = rows_affected / sync_duration_seconds if sync_duration_seconds > 0 else 0.0

    # Only advance watermark when rows were actually synced — advancing on
    # zero rows causes the delta window to skip past unsynced data.
    if config.mode == "watermark":
        if rows_affected > 0:
            state_mgr.update_sync_state(
                config.schema,
                config.table,
                rows_affected=rows_affected,
                duration_ms=duration_ms,
                status="success",
            )
        else:
            context.log.info(f"Watermark not advanced for {full_table} — 0 rows synced")

    effective_mode = config.mode
    if config.mode == "watermark" and last_sync_timestamp is not None:
        effective_mode = "watermark_delta"
    elif config.mode == "watermark":
        effective_mode = "watermark_full"

    context.log.info(
        f"✅ Synced {rows_affected} rows to {full_table} "
        f"(mode={effective_mode}, duration={duration_ms}ms, "
        f"rows/s={rows_per_second:,.0f})"
    )

    return {
        "rows_affected": rows_affected,
        "mode": effective_mode,
        "sync_engine": SYNC_ENGINE_LINKED_SERVER,
        "table": full_table,
        "columns_synced": len(columns),
        "duration_ms": duration_ms,
        "sync_duration_seconds": sync_duration_seconds,
        "rows_per_second": round(rows_per_second, 1),
    }


def _sync_via_python(
    context: dg.AssetExecutionContext,
    mssql_source_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
    config: SyncTableConfig,
    fallback_reason: str | None = None,
) -> dict:
    """
    Execute sync using Python-based data transfer with streaming chunks.

    Reads from staging in chunks of config.chunk_size rows via fetchmany()
    to keep memory usage constant regardless of table size. Both staging and
    DSO connections are held open simultaneously during the transfer.

    This is a fallback for when linked servers are not available
    (e.g., Azure SQL Edge in local development). Caller must set
    DSO_SYNC_ALLOW_PYTHON_FALLBACK=true to allow this path.

    Schema validation should be done by the caller (sync_table_via_linked_server).
    This function assumes schemas have already been validated and match.
    """
    from DSO.load.sync_state import SyncStateManager

    staging_table_ref = f"[{config.staging_schema}].[{config.staging_table}]"
    target_table_ref = f"[{config.schema}].[{config.table}]"
    full_table = target_table_ref

    # Get column structure from staging (source of truth for data)
    with mssql_source_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{config.staging_schema}' AND TABLE_NAME = '{config.staging_table}'
            ORDER BY ORDINAL_POSITION
            """
            )
            staging_columns = [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    if not staging_columns:
        raise RuntimeError(f"Table {full_table} not found in staging database or has no columns")

    # Validate watermark_column exists in table columns
    if config.mode == "watermark" and config.watermark_column:
        if config.watermark_column not in staging_columns:
            raise ValueError(
                f"watermark_column '{config.watermark_column}' not found in "
                f"table {full_table}. Available columns: {staging_columns}"
            )

    # Only apply COLLATE DATABASE_DEFAULT when staging and DSO collations differ.
    pk_cols_for_type_check = (
        [config.primary_key] if isinstance(config.primary_key, str) else list(config.primary_key)
    )
    needs_collate = check_collation_mismatch(mssql_source_staging, mssql_target_dso, context)
    string_pk_cols: set[str] = (
        get_string_pk_columns(mssql_target_dso, config.schema, config.table, pk_cols_for_type_check)
        if needs_collate
        else set()
    )

    # Filter out excluded columns
    sync_columns = [c for c in staging_columns if c not in config.exclude_columns]

    if len(sync_columns) != len(staging_columns):
        excluded = [c for c in staging_columns if c in config.exclude_columns]
        context.log.info(f"Excluding columns from sync: {excluded}")

    col_list = ", ".join(f"[{c}]" for c in sync_columns)

    context.log.info(
        f"Syncing {full_table} with {len(sync_columns)} columns via Python fallback "
        f"(chunk_size={config.chunk_size}, mode={config.mode})"
    )

    # Build SELECT query with optional deduplication for composite PKs.
    # Truncate-insert mode does not rely on PK semantics in Python fallback.
    pk_cols = (
        [config.primary_key] if isinstance(config.primary_key, str) else list(config.primary_key)
    )
    pk_cols_present = [c for c in pk_cols if c in sync_columns]
    if config.mode != "truncate_insert":
        missing_pk_cols = [c for c in pk_cols if c not in sync_columns]
        if missing_pk_cols:
            raise RuntimeError(
                f"Cannot sync {full_table}: missing PK columns in staging: {missing_pk_cols}"
            )

    # Resolve delta state for watermark mode
    state_mgr: SyncStateManager | None = None
    last_sync_timestamp: datetime | None = None
    if config.mode == "watermark":
        state_mgr = SyncStateManager(mssql_target_dso, context)
        last_sync_timestamp = state_mgr.get_last_sync_timestamp(config.schema, config.table)
        if last_sync_timestamp is not None:
            context.log.info(
                f"Python watermark mode: column={config.watermark_column}, "
                f"last_sync={last_sync_timestamp.isoformat()}"
            )
        else:
            context.log.info(
                "Python watermark mode: no previous sync found — executing full sync (first run)"
            )

    watermark_where_clause = ""
    select_params: tuple[datetime] | tuple[()] = ()
    if config.mode == "watermark" and last_sync_timestamp is not None:
        source_placeholder = get_param_placeholder(mssql_source_staging)
        watermark_col = (
            config.watermark_column
            or _find_update_timestamp_col(sync_columns)
            or "dw_timestamp_update"
        )
        insert_fallback = _find_insert_timestamp_col(sync_columns) or "dw_timestamp_insert"
        watermark_where_clause = (
            f"WHERE COALESCE([{watermark_col}], [{insert_fallback}]) >= " f"{source_placeholder}"
        )
        select_params = (last_sync_timestamp,)

    if len(pk_cols_present) > 1:
        pk_list = ", ".join(f"[{c}]" for c in pk_cols_present)
        timestamp_cols = [
            c for c in sync_columns if "timestamp" in c.lower() or "update" in c.lower()
        ]
        order_col = f"[{timestamp_cols[0]}] DESC" if timestamp_cols else "1"

        select_sql = f"""
            SELECT {col_list}
            FROM (
                SELECT {col_list},
                       ROW_NUMBER() OVER (PARTITION BY {pk_list} ORDER BY {order_col}) AS rn
                FROM {staging_table_ref}
                {watermark_where_clause}
            ) dedup
            WHERE rn = 1
        """
        context.log.info(f"Using deduplicated SELECT with PK: ({pk_list})")
    else:
        select_sql = f"""
            SELECT {col_list}
            FROM {staging_table_ref}
            {watermark_where_clause}
        """

    # Prepare DSO statements
    target_placeholder = get_param_placeholder(mssql_target_dso)
    placeholders = ", ".join([target_placeholder] * len(sync_columns))
    insert_sql = f"INSERT INTO {target_table_ref} ({col_list}) VALUES ({placeholders})"

    # For watermark-delta: prefer temp table + set-based MERGE per chunk.
    # Falls back to row-level upsert if temp table creation fails (e.g. tempdb permissions).
    temp_table_name = f"#_sync_{config.schema}_{config.table}"
    temp_insert_sql: str | None = None
    merge_from_temp_sql: str | None = None
    row_upsert_sql: str | None = None

    if config.mode == "watermark" and last_sync_timestamp is not None:
        source_column_list = ", ".join(f"[{c}]" for c in sync_columns)
        on_clause = _build_on_clause(pk_cols, config, string_pk_cols)
        update_ts_col = _find_update_timestamp_col(sync_columns)
        update_columns = [
            c
            for c in sync_columns
            if c not in pk_cols and c not in config.insert_only_columns and c != update_ts_col
        ]
        update_assignments = [f"tgt.[{c}] = src.[{c}]" for c in update_columns]
        if update_ts_col:
            update_assignments.append(
                f"tgt.[{update_ts_col}] = CAST(SYSDATETIME() AS datetime2(0))"
            )
        update_set = ", ".join(update_assignments)
        insert_vals = ", ".join(f"src.[{c}]" for c in sync_columns)

        # Primary strategy: temp table + set-based MERGE
        temp_insert_sql = f"INSERT INTO {temp_table_name} ({col_list}) VALUES ({placeholders})"
        merge_from_temp_sql = f"""
            MERGE {full_table} AS tgt
            USING {temp_table_name} AS src
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({source_column_list})
                VALUES ({insert_vals});
        """

        # Fallback strategy: row-level upsert (slower but works without tempdb access)
        row_upsert_sql = f"""
            MERGE {full_table} AS tgt
            USING (VALUES ({placeholders})) AS src ({source_column_list})
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({source_column_list})
                VALUES ({insert_vals});
        """

    # Stream data from staging → DSO in chunks.
    # Both connections are held open simultaneously so rows are never fully
    # materialised in Python memory.
    sync_start = time.monotonic()
    log_every_n_chunks = 10
    effective_mode = f"python_{config.mode}"
    backoff_seconds = (5, 15, 45)
    for attempt in range(len(backoff_seconds) + 1):
        total_rows = 0
        chunks_processed = 0
        try:
            with mssql_source_staging.get_connection() as staging_conn:
                staging_cursor = staging_conn.cursor()
                try:
                    if select_params:
                        staging_cursor.execute(select_sql, select_params)
                    else:
                        staging_cursor.execute(select_sql)

                    # Peek at first chunk to detect empty source BEFORE mutating DSO
                    first_chunk = staging_cursor.fetchmany(config.chunk_size)
                    if not first_chunk:
                        duration_ms = int((time.monotonic() - sync_start) * 1000)
                        if config.mode == "watermark" and state_mgr is not None:
                            context.log.info(
                                f"Watermark not advanced for {full_table} — no source data"
                            )
                        context.log.info("No data to sync — source table is empty")
                        if config.mode == "watermark":
                            effective_mode = (
                                "python_watermark_delta"
                                if last_sync_timestamp is not None
                                else "python_watermark_full"
                            )
                        return {
                            "rows_affected": 0,
                            "mode": effective_mode,
                            "sync_engine": SYNC_ENGINE_PYTHON_FALLBACK,
                            "fallback_reason": fallback_reason,
                            "table": full_table,
                            "columns_synced": len(sync_columns),
                            "sync_duration_seconds": round(duration_ms / 1000.0, 2),
                            "rows_per_second": 0.0,
                            "note": "Source table was empty",
                        }

                    with mssql_target_dso.get_connection() as dso_conn:
                        dso_cursor = dso_conn.cursor()

                        if config.identity_insert:
                            context.log.info(f"Enabling IDENTITY_INSERT for {full_table}")
                            dso_cursor.execute(f"SET IDENTITY_INSERT {full_table} ON")

                        try:
                            is_watermark_delta = (
                                config.mode == "watermark" and last_sync_timestamp is not None
                            )
                            use_temp_table = False
                            if is_watermark_delta:
                                effective_mode = "python_watermark_delta"
                                try:
                                    if config.identity_insert:
                                        # SELECT INTO copies IDENTITY property; use arithmetic
                                        # expression (col + 0) to strip it for numeric PK columns.
                                        pk_cols = (
                                            config.primary_key
                                            if isinstance(config.primary_key, list)
                                            else [config.primary_key]
                                        )
                                        cols_expr = ", ".join(
                                            f"([{c}] + 0) AS [{c}]" if c in pk_cols else f"[{c}]"
                                            for c in sync_columns
                                        )
                                        dso_cursor.execute(
                                            f"SELECT TOP 0 {cols_expr} "
                                            f"INTO {temp_table_name} FROM {full_table}"
                                        )
                                    else:
                                        dso_cursor.execute(
                                            f"SELECT TOP 0 {col_list} "
                                            f"INTO {temp_table_name} FROM {full_table}"
                                        )
                                    use_temp_table = True
                                    context.log.info(
                                        "Python fallback watermark delta: temp table + set-based MERGE per chunk"
                                    )
                                except Exception as temp_err:
                                    context.log.warning(
                                        f"Could not create temp table ({temp_err}); "
                                        f"falling back to row-level upsert"
                                    )
                            else:
                                if config.mode == "watermark":
                                    effective_mode = "python_watermark_full"
                                    context.log.info(
                                        "Python fallback watermark first run: using full truncate-insert"
                                    )
                                elif config.mode == "merge":
                                    context.log.info(
                                        "Python fallback using truncate-insert pattern for MERGE..."
                                    )
                                else:
                                    context.log.info("Python fallback using truncate-insert mode")

                                context.log.info(f"Executing TRUNCATE on {full_table}")
                                dso_cursor.execute(f"TRUNCATE TABLE {full_table}")
                                context.log.info("Streaming data from staging to DSO...")

                            chunk = first_chunk
                            while chunk:
                                if chunk[0].__class__.__name__ == "Row":
                                    chunk = [tuple(row) for row in chunk]

                                _set_mssql_python_chunk_input_sizes(dso_conn, dso_cursor, chunk)

                                if is_watermark_delta:
                                    if use_temp_table:
                                        dso_cursor.executemany(temp_insert_sql, chunk)
                                        _clear_stale_current_before_merge(
                                            dso_cursor, config, temp_table_name, string_pk_cols
                                        )
                                        dso_cursor.execute(merge_from_temp_sql)
                                        dso_cursor.execute(f"TRUNCATE TABLE {temp_table_name}")
                                    else:
                                        dso_cursor.executemany(row_upsert_sql, chunk)
                                else:
                                    dso_cursor.executemany(insert_sql, chunk)

                                total_rows += len(chunk)
                                chunks_processed += 1

                                if chunks_processed % log_every_n_chunks == 0:
                                    elapsed = time.monotonic() - sync_start
                                    rps = total_rows / elapsed if elapsed > 0 else 0
                                    context.log.info(
                                        f"Progress: {total_rows:,} rows in {chunks_processed} chunks "
                                        f"({rps:,.0f} rows/s)"
                                    )

                                chunk = staging_cursor.fetchmany(config.chunk_size)

                        finally:
                            if use_temp_table:
                                try:
                                    dso_cursor.execute(
                                        f"IF OBJECT_ID('tempdb..{temp_table_name}') IS NOT NULL "
                                        f"DROP TABLE {temp_table_name}"
                                    )
                                except Exception:
                                    pass
                            if config.identity_insert:
                                context.log.info(f"Disabling IDENTITY_INSERT for {full_table}")
                                dso_cursor.execute(f"SET IDENTITY_INSERT {full_table} OFF")
                            dso_cursor.close()

                        dso_conn.commit()
                finally:
                    staging_cursor.close()
            break
        except Exception as e:
            if attempt < len(backoff_seconds) and _is_retryable_connectivity_error(e):
                wait = backoff_seconds[attempt]
                context.log.warning(
                    f"Python fallback sync for {full_table} failed (attempt "
                    f"{attempt + 1}/{len(backoff_seconds) + 1}): {str(e)[:200]}. "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)
                continue
            raise

    duration_ms = int((time.monotonic() - sync_start) * 1000)
    sync_duration_seconds = duration_ms / 1000.0
    rows_per_second = total_rows / sync_duration_seconds if sync_duration_seconds > 0 else 0.0

    if config.mode == "watermark" and state_mgr is not None:
        if total_rows > 0:
            state_mgr.update_sync_state(
                config.schema,
                config.table,
                rows_affected=total_rows,
                duration_ms=duration_ms,
                status="success",
            )
        else:
            context.log.info(f"Watermark not advanced for {full_table} — 0 rows synced")

    context.log.info(
        f"✅ Synced {total_rows:,} rows to {full_table} via Python fallback "
        f"({chunks_processed} chunks of {config.chunk_size}, mode={effective_mode}, "
        f"duration={sync_duration_seconds:.1f}s, rows/s={rows_per_second:,.0f})"
    )

    return {
        "rows_affected": total_rows,
        "mode": effective_mode,
        "sync_engine": SYNC_ENGINE_PYTHON_FALLBACK,
        "fallback_reason": fallback_reason,
        "table": full_table,
        "columns_synced": len(sync_columns),
        "sync_duration_seconds": round(sync_duration_seconds, 2),
        "rows_per_second": round(rows_per_second, 1),
        "note": "Used Python fallback (linked server not available)",
    }


def get_staging_row_count(
    mssql_target_dso: MSSQLResource,
    schema: str,
    table: str,
    linked_server: str = LINKED_SERVER_NAME,
    staging_db: str = STAGING_DATABASE,
    mssql_source_staging: MSSQLResource | None = None,
) -> int:
    """
    Get row count from staging table via linked server or direct connection.

    In same-database mode, queries the table directly without linked server.
    If mssql_source_staging is provided and linked server is unavailable,
    queries staging directly.

    Args:
        mssql_target_dso: DSO database resource
        schema: Schema name
        table: Table name
        linked_server: Name of the linked server
        staging_db: Name of the staging database
        mssql_source_staging: Optional staging resource for direct query fallback

    Returns:
        Row count from staging table
    """
    full_table = f"[{schema}].[{table}]"

    # In same-database mode, query the table directly
    if is_same_database_mode(mssql_target_dso):
        with mssql_target_dso.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
                return cursor.fetchone()[0]
            finally:
                cursor.close()

    # Try linked server first
    source_table = f"[{linked_server}].[{staging_db}].[{schema}].[{table}]"
    try:
        with mssql_target_dso.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
                return cursor.fetchone()[0]
            finally:
                cursor.close()
    except Exception:
        # Linked server failed - try direct staging connection if available
        if mssql_source_staging is not None:
            with mssql_source_staging.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
                    return cursor.fetchone()[0]
                finally:
                    cursor.close()
        else:
            # No fallback available, re-raise
            raise


def get_dso_row_count(
    mssql_target_dso: MSSQLResource,
    schema: str,
    table: str,
) -> int:
    """
    Get row count from DSO table.

    Args:
        mssql_target_dso: DSO database resource
        schema: Schema name
        table: Table name

    Returns:
        Row count from DSO table
    """
    target_table = f"[{schema}].[{table}]"

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
            return cursor.fetchone()[0]
        finally:
            cursor.close()


def compare_row_counts(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    schema: str,
    table: str,
    mssql_source_staging: MSSQLResource | None = None,
    staging_schema: str | None = None,
    staging_table: str | None = None,
) -> dict:
    """
    Compare row counts between staging and DSO for a table.

    Args:
        context: Dagster asset execution context
        mssql_target_dso: DSO database resource
        schema: Schema name
        table: Table name
        mssql_source_staging: Optional staging resource for direct query fallback
        staging_schema: Staging schema when it differs from DSO (defaults to schema)
        staging_table: Staging table/view when it differs from DSO (defaults to table)

    Returns:
        Dictionary with staging_count, dso_count, and match status
    """
    src_schema = staging_schema if staging_schema is not None else schema
    src_table = staging_table if staging_table is not None else table
    staging_count = get_staging_row_count(
        mssql_target_dso,
        src_schema,
        src_table,
        mssql_source_staging=mssql_source_staging,
    )
    dso_count = get_dso_row_count(mssql_target_dso, schema, table)

    match = staging_count == dso_count
    status = "✅ MATCH" if match else "⚠️ MISMATCH"

    context.log.info(
        f"{status}: {src_schema}.{src_table} -> {schema}.{table} - "
        f"Staging: {staging_count}, DSO: {dso_count}"
    )

    return {
        "staging_count": staging_count,
        "dso_count": dso_count,
        "match": match,
    }
