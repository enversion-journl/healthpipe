# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Bulk-copy helpers for mssql-python's native TDS bulk-insert protocol.

Includes chunk-size estimation, string/binary truncation to fit BCP column
limits, the low-level ``bulkcopy_insert`` call, and retry/refresh logic for
long-running sessions.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from dagster import AssetExecutionContext
from DSI.connectors.mssql.models import TableConfig

# Audit columns the MSSQL-source ext tables carry beyond the source columns.
# The Oracle-DFM ext tables are created without them, so that path passes
# ``extra_columns=()``.
AUDIT_COLUMNS: tuple[str, ...] = ("SourceSystem", "dw_load_timestamp", "dw_partition_date")

# ---------------------------------------------------------------------------
# BCP column-width limits
# ---------------------------------------------------------------------------

BCP_NVARCHAR_LIMIT = 4000
BCP_VARCHAR_LIMIT = 8000
BCP_VARBINARY_LIMIT = 8000


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------


def build_bcp_truncation_limits(
    columns_info: list[tuple[str, Any, Any, Any, Any, Any]],
) -> list[int | None]:
    """Build per-column character limits for BCP-safe truncation.

    Returns a list parallel to *columns_info* where each element is either
    the maximum character (or byte) length that ``avoid_max_types=True`` would
    produce for that column, or ``None`` if truncation is not needed (fixed-
    size types or already-sized string columns).
    """
    limits: list[int | None] = []
    for _col_name, col_type, _nullable, col_size, _prec, _scale in columns_info:
        dt = (col_type or "").lower() if isinstance(col_type, str) else ""
        if dt in ("nvarchar", "ntext", "text"):
            if isinstance(col_size, int) and 0 < col_size <= BCP_NVARCHAR_LIMIT:
                limits.append(None)
            else:
                limits.append(BCP_NVARCHAR_LIMIT)
        elif dt == "varchar":
            if isinstance(col_size, int) and 0 < col_size <= BCP_VARCHAR_LIMIT:
                limits.append(None)
            else:
                limits.append(BCP_VARCHAR_LIMIT)
        elif dt in ("varbinary", "image"):
            if isinstance(col_size, int) and 0 < col_size <= BCP_VARBINARY_LIMIT:
                limits.append(None)
            else:
                limits.append(BCP_VARBINARY_LIMIT)
        else:
            limits.append(None)
    return limits


def truncate_for_bcp(
    data: list[tuple[Any, ...]],
    limits: list[int | None],
    context: AssetExecutionContext | None = None,
) -> int:
    """Truncate oversized string/binary values in *data* in-place.

    Only the first ``len(limits)`` columns of each row are checked; the
    trailing extra columns (SourceSystem, dw_load_timestamp, dw_partition_date)
    are left untouched.

    Returns the number of individual cell values that were truncated.
    """
    truncated_count = 0
    for row_idx in range(len(data)):
        row = data[row_idx]
        mutated = False
        row_list: list[Any] | None = None
        for col_idx, limit in enumerate(limits):
            if limit is None or col_idx >= len(row):
                continue
            val = row[col_idx]
            if val is None:
                continue
            if isinstance(val, str) and len(val) > limit:
                if not mutated:
                    row_list = list(row)
                    mutated = True
                row_list[col_idx] = val[:limit]
                truncated_count += 1
            elif isinstance(val, bytes | bytearray) and len(val) > limit:
                if not mutated:
                    row_list = list(row)
                    mutated = True
                row_list[col_idx] = val[:limit]
                truncated_count += 1
        if mutated and row_list is not None:
            data[row_idx] = tuple(row_list)
    if truncated_count > 0 and context is not None:
        context.log.warning(
            f"Truncated {truncated_count} oversized value(s) to fit BCP column limits "
            f"(NVARCHAR\u2192{BCP_NVARCHAR_LIMIT}, VARCHAR\u2192{BCP_VARCHAR_LIMIT})"
        )
    return truncated_count


# ---------------------------------------------------------------------------
# Decimal normalisation
# ---------------------------------------------------------------------------


def normalize_decimals_for_bcp(
    data: list[tuple[Any, ...]],
) -> int:
    """Normalize ``Decimal`` values in *data* in-place to avoid TDS bulk-copy failures.

    The mssql-python bulk-copy driver transmits a ``Decimal`` using its raw
    coefficient + exponent over TDS. SQL Server then rejects the row when the
    transmitted scale (``-exponent``) exceeds the target column's scale, even
    when the value is exactly representable at the target scale. Two flavors
    show up in practice for ``DECIMAL(p, s)`` targets:

    * ``Decimal('0E-12')`` — a zero whose internal exponent is non-zero. The
      driver encodes it with negative scale and SQL Server rejects it.
    * ``Decimal('2876.000000000000000')`` / ``Decimal('13309.150000000000000')``
      — non-zero values arriving with scale 15 (e.g. from a source view that
      performs arithmetic) for a target column of scale 12.

    The fix is lossless: strip trailing zeros from the Decimal coefficient up
    to (but not past) exponent 0, and replace any zero value with a bare
    ``Decimal('0')``. Mathematically identical, but produces a TDS encoding
    SQL Server accepts.

    Returns the number of cells that were normalized.
    """
    DEC_ZERO = Decimal((0, (0,), 0))
    normalized_count = 0
    for row_idx in range(len(data)):
        row = data[row_idx]
        row_list: list[Any] | None = None
        for col_idx, val in enumerate(row):
            if not isinstance(val, Decimal):
                continue
            sign, digits, exponent = val.as_tuple()
            # Non-finite (NaN/Inf) — exponent is a string; skip.
            if not isinstance(exponent, int) or exponent >= 0:
                continue
            if len(digits) == 1 and digits[0] == 0:
                new_val = DEC_ZERO if sign == 0 else Decimal((sign, (0,), 0))
            else:
                trailing = 0
                for d in reversed(digits):
                    if d == 0:
                        trailing += 1
                    else:
                        break
                strip = trailing if trailing < -exponent else -exponent
                if strip == 0:
                    continue
                new_digits = digits[:-strip] if strip < len(digits) else (0,)
                new_val = Decimal((sign, new_digits, exponent + strip))
            if row_list is None:
                row_list = list(row)
            row_list[col_idx] = new_val
            normalized_count += 1
        if row_list is not None:
            data[row_idx] = tuple(row_list)

    return normalized_count


# ---------------------------------------------------------------------------
# Chunk-size estimation
# ---------------------------------------------------------------------------


def estimate_column_max_bytes(data_type: Any, col_size: Any, col_precision: Any) -> int:
    """Return a conservative per-column max-byte estimate for fetch chunk sizing."""
    dt = str(data_type).lower() if data_type else ""

    if dt in {"nvarchar", "nchar"}:
        if isinstance(col_size, int) and col_size > 0:
            return min(col_size, 4000) * 2
        return 8000
    if dt in {"varchar", "char"}:
        if isinstance(col_size, int) and col_size > 0:
            return min(col_size, 8000)
        return 8000
    if dt in {"text", "ntext", "xml"}:
        return 8000
    if dt in {"varbinary", "binary", "image"}:
        if isinstance(col_size, int) and col_size > 0:
            return min(col_size, 8000)
        return 8000
    if dt in {"bigint", "datetime", "datetime2", "datetimeoffset", "float"}:
        return 8
    if dt in {"int", "real", "smalldatetime"}:
        return 4
    if dt in {"smallint"}:
        return 2
    if dt in {"tinyint", "bit"}:
        return 1
    if dt in {"date"}:
        return 3
    if dt in {"time"}:
        return 5
    if dt in {"uniqueidentifier"}:
        return 16
    if dt in {"decimal", "numeric", "money", "smallmoney"}:
        if isinstance(col_precision, int):
            if col_precision <= 9:
                return 5
            if col_precision <= 19:
                return 9
            if col_precision <= 28:
                return 13
        return 17
    if dt in {"sql_variant"}:
        return 8016
    return 128


def compute_streaming_chunk_size(
    columns_info: list[tuple[str, Any, Any, Any, Any, Any]],
    extra_column_count: int = 3,
) -> tuple[int, int]:
    """Compute a safe fetch chunk size by balancing cells and estimated bytes.

    Returns ``(fetch_chunk_size, estimated_row_bytes)``.
    """
    num_cols = len(columns_info) + extra_column_count

    max_cells_per_chunk = 4_500_000
    max_rows_per_chunk = 20_000
    target_chunk_bytes = 64 * 1024 * 1024

    estimated_row_bytes = sum(
        estimate_column_max_bytes(col_type, col_size, col_precision)
        for _, col_type, _, col_size, col_precision, _ in columns_info
    )
    estimated_row_bytes += 100 + 8 + 3

    # Capped at 10k (min 5k) after repeated mssql-python cursor.bulkcopy()
    # wedges at the 15-20k range (finanskladdelinje_enriched monthly, SOF
    # appointment daily). Keeps the global ceiling well below the empirical
    # wedge threshold.
    min_rows_per_chunk = 10_000
    rows_by_cells = max(1, max_cells_per_chunk // max(num_cols, 1))
    rows_by_bytes = max(1, target_chunk_bytes // max(estimated_row_bytes, 1))
    fetch_chunk_size = max(
        min_rows_per_chunk, min(max_rows_per_chunk, rows_by_cells, rows_by_bytes)
    )

    return fetch_chunk_size, estimated_row_bytes


# ---------------------------------------------------------------------------
# Low-level bulk-copy insert
# ---------------------------------------------------------------------------

# Per-call cursor.bulkcopy() deadline — a wedge-detection bound, not a capacity
# limit. A healthy chunk inserts in <1s; this caps how long a wedged call hangs
# before bulkcopy_insert_with_retry recovers.
BULKCOPY_TIMEOUT_SECONDS = 120


def bulkcopy_insert(
    context: AssetExecutionContext,
    bulkcopy_conn,
    config: TableConfig,
    column_names: list[str],
    data: list[tuple[Any, ...]],
    table_lock: bool = True,
    extra_columns: Sequence[str] = AUDIT_COLUMNS,
) -> int:
    """Insert via mssql-python's native bulk-copy protocol.

    Uses ``cursor.bulkcopy()`` which maps to SQL Server's TDS bulk-insert,
    bypassing the INSERT-per-row overhead entirely. ``table_lock`` controls the
    BCP ``TABLOCK`` hint (BU lock on a heap; default ``True`` matches production).
    ``extra_columns`` are appended to the column mapping after *column_names*
    (default: the three audit columns the MSSQL-source ext tables carry).
    """
    all_columns = list(column_names) + list(extra_columns)

    bc_cursor = bulkcopy_conn.cursor()
    try:
        result = bc_cursor.bulkcopy(
            f"ext.{config.target_table}",
            data,
            batch_size=0,
            timeout=BULKCOPY_TIMEOUT_SECONDS,
            table_lock=table_lock,
            column_mappings=all_columns,
        )
        row_count = result.get("rows_copied", len(data))
        return row_count
    except Exception as e:
        context.log.error(f"Bulk-copy failed: {e}")
        raise
    finally:
        bc_cursor.close()


# ---------------------------------------------------------------------------
# Connection refresh & retry
# ---------------------------------------------------------------------------


def is_timeout_error(exc: BaseException) -> bool:
    """Check if an exception is a bulk-copy timeout."""
    msg = str(exc).lower()
    return "timeout" in msg and ("deadline" in msg or "elapsed" in msg or "expired" in msg)


# Chunk size at or above which a bulk-copy timeout is likely caused by
# mssql-python's cursor.bulkcopy() wedging client-side rather than by a
# server-side problem. Used to add a hint to the warning emitted on timeout.
BULKCOPY_CHUNK_SIZE_HINT_THRESHOLD = 10_000


def refresh_bulkcopy_conn(
    context: AssetExecutionContext,
    bulkcopy_conn,
    mssql_target_resource,
    label: str,
    open_contexts: list | None = None,
) -> object:
    """Close *bulkcopy_conn* and return a fresh one from *mssql_target_resource*.

    The ``get_connection()`` resource is a generator-based context
    manager.  We must keep a reference to the generator alive for the lifetime
    of the connection — otherwise the GC finalises the generator, which runs
    ``finally: conn.close()`` and silently closes the connection we just
    opened.  Callers pass *open_contexts* (a list they own) so we can stash
    the context-manager reference there.
    """
    context.log.info(f"{label} Opening fresh bulk-copy connection...")
    try:
        bulkcopy_conn.close()
    except Exception:
        pass
    ctx_mgr = mssql_target_resource.get_connection()
    conn = ctx_mgr.__enter__()
    if open_contexts is not None:
        open_contexts.append(ctx_mgr)
    return conn


def _warn_bulkcopy_timeout(context: AssetExecutionContext, prefix: str, chunk_size: int) -> None:
    """Log a bulk-copy timeout warning, with a chunk-size hint when relevant.

    Above ``BULKCOPY_CHUNK_SIZE_HINT_THRESHOLD``, mssql-python's
    ``cursor.bulkcopy()`` is known to wedge client-side on long extractions
    (silent hang, no server-side activity). The hint points operators at
    ``MssqlExtractor(bcp_max_chunk_size=...)`` rather than chasing data shape.
    """
    msg = f"{prefix} Bulk-copy timed out."
    if chunk_size >= BULKCOPY_CHUNK_SIZE_HINT_THRESHOLD:
        msg += (
            f" Chunk size {chunk_size:,} rows may be too large for "
            f"mssql-python.cursor.bulkcopy() on a long extraction; "
            f"cap it via MssqlExtractor(bcp_max_chunk_size=...) "
            f"(5,000 was sufficient for finanskladdelinje_enriched monthly)."
        )
    context.log.warning(msg)


_MIN_SPLIT_ROWS = 500
_MAX_SPLIT_DEPTH = 5


def bulkcopy_insert_with_retry(
    context: AssetExecutionContext,
    bulkcopy_conn,
    config: TableConfig,
    column_names: list[str],
    data: list[tuple[Any, ...]],
    mssql_target_resource=None,
    max_retries: int = 2,
    open_contexts: list | None = None,
    table_lock: bool = True,
    extra_columns: Sequence[str] = AUDIT_COLUMNS,
    expected_rows_before: int | None = None,
) -> tuple[int, object]:
    """Bulk-copy insert with split-first timeout recovery.

    Tries the current connection; on timeout, refreshes the connection and
    splits the chunk in half (recursive, down to ``_MIN_SPLIT_ROWS``) rather
    than re-attempting the wedge-prone size. A same-size retry on the fresh
    connection happens only at the split floor.

    ``expected_rows_before`` (opt-in) is the caller's running row count for
    the target table BEFORE this call. Only valid when the caller is the
    single writer to a freshly created table (the Oracle-DFM ext path). On a
    timeout it lets the recovery distinguish "server committed the batch but
    the client timed out" (bank the rows, do not re-insert -> no duplicates)
    from "nothing landed" (split/retry as usual). Costs one COUNT(*) on the
    rare timeout path only. ``None`` (default) keeps the previous behavior.

    Returns ``(rows_inserted, active_connection)`` so the caller can keep using
    the (possibly new) connection.
    """
    total_rows, bulkcopy_conn = _bulkcopy_with_split(
        context,
        bulkcopy_conn,
        config,
        column_names,
        data,
        mssql_target_resource=mssql_target_resource,
        max_retries=max_retries,
        open_contexts=open_contexts,
        depth=0,
        table_lock=table_lock,
        extra_columns=extra_columns,
        expected_rows_before=expected_rows_before,
    )
    return total_rows, bulkcopy_conn


def _count_target_rows(conn, config: TableConfig) -> int:
    """COUNT(*) of the bulk-copy target via *conn* (timeout-recovery guard)."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [ext].[{config.target_table}]")
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def _bulkcopy_with_split(
    context: AssetExecutionContext,
    bulkcopy_conn,
    config: TableConfig,
    column_names: list[str],
    data: list[tuple[Any, ...]],
    *,
    mssql_target_resource=None,
    max_retries: int = 2,
    open_contexts: list | None = None,
    depth: int = 0,
    table_lock: bool = True,
    extra_columns: Sequence[str] = AUDIT_COLUMNS,
    expected_rows_before: int | None = None,
) -> tuple[int, object]:
    """Recursive helper: try the chunk; on timeout refresh + split.

    A same-size retry on the fresh connection happens only at the split floor.
    """
    chunk_size = len(data)

    # --- attempt: current connection ---
    try:
        count = bulkcopy_insert(
            context,
            bulkcopy_conn,
            config,
            column_names,
            data,
            table_lock=table_lock,
            extra_columns=extra_columns,
        )
        return count, bulkcopy_conn
    except Exception as first_err:
        if not is_timeout_error(first_err):
            raise
        _warn_bulkcopy_timeout(context, f"[rows={chunk_size:,}] (current connection)", chunk_size)

    can_split = chunk_size > _MIN_SPLIT_ROWS and depth < _MAX_SPLIT_DEPTH

    # The wedge is size-correlated, so refresh the suspect connection and split
    # rather than re-attempting the same size (which just wastes another
    # timeout). At the floor, where halving is impossible, retry the same size
    # once on the fresh connection.
    if mssql_target_resource is not None:
        bulkcopy_conn = refresh_bulkcopy_conn(
            context,
            bulkcopy_conn,
            mssql_target_resource,
            label=(
                f"[rows={chunk_size:,}] "
                + ("(refresh, splitting)" if can_split else "(refresh, at split floor)")
            ),
            open_contexts=open_contexts,
        )
        # Duplicate guard: the chunk is one atomic batch, so count the target
        # to learn whether the timed-out call actually committed.
        if expected_rows_before is not None:
            committed = _count_target_rows(bulkcopy_conn, config)
            if committed == expected_rows_before + chunk_size:
                context.log.warning(
                    f"[rows={chunk_size:,}] Server committed the batch despite the "
                    f"client-side timeout (count {committed:,}); banking it without "
                    f"re-insert to avoid duplicates."
                )
                return chunk_size, bulkcopy_conn
            if committed != expected_rows_before:
                raise RuntimeError(
                    f"Bulk-copy timeout recovery found {committed:,} rows in "
                    f"ext.{config.target_table}, expected {expected_rows_before:,} "
                    f"or {expected_rows_before + chunk_size:,} — refusing to "
                    f"retry into an inconsistent target."
                )
        if not can_split:
            try:
                count = bulkcopy_insert(
                    context,
                    bulkcopy_conn,
                    config,
                    column_names,
                    data,
                    table_lock=table_lock,
                    extra_columns=extra_columns,
                )
                return count, bulkcopy_conn
            except Exception as retry_err:
                if not is_timeout_error(retry_err):
                    raise
                _warn_bulkcopy_timeout(
                    context,
                    f"[rows={chunk_size:,}] (fresh conn retry, at split floor)",
                    chunk_size,
                )

    # --- split: chunk is genuinely too large for the timeout window ---
    if not can_split:
        raise RuntimeError(
            f"Bulk-copy timed out for ext.{config.target_table} "
            f"({chunk_size:,} rows) — cannot split further "
            f"(min={_MIN_SPLIT_ROWS}, depth={depth}/{_MAX_SPLIT_DEPTH})."
        )

    mid = chunk_size // 2
    context.log.info(
        f"[rows={chunk_size:,}] Splitting into {mid:,} + {chunk_size - mid:,} "
        f"(depth={depth + 1})"
    )

    left_count, bulkcopy_conn = _bulkcopy_with_split(
        context,
        bulkcopy_conn,
        config,
        column_names,
        data[:mid],
        mssql_target_resource=mssql_target_resource,
        max_retries=max_retries,
        open_contexts=open_contexts,
        depth=depth + 1,
        table_lock=table_lock,
        extra_columns=extra_columns,
        expected_rows_before=expected_rows_before,
    )
    right_count, bulkcopy_conn = _bulkcopy_with_split(
        context,
        bulkcopy_conn,
        config,
        column_names,
        data[mid:],
        mssql_target_resource=mssql_target_resource,
        max_retries=max_retries,
        open_contexts=open_contexts,
        depth=depth + 1,
        table_lock=table_lock,
        extra_columns=extra_columns,
        expected_rows_before=(
            None if expected_rows_before is None else expected_rows_before + left_count
        ),
    )

    return left_count + right_count, bulkcopy_conn
