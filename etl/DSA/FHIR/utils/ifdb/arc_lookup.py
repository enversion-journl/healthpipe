# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Chunked MSSQL IN-clause lookup for IFDB archive (``arc.*``) tables.

MSSQL pendant to :mod:`DSA.FHIR.utils.ifdb.oracle_lookup` for assets migrated
to the DFM archive layer. Same chunk-and-bind contract, three differences:

1. Values are bound as positional ``?`` parameters, chunked below SQL
   Server's 2100-parameter statement limit (Oracle's limit is 1000).
2. Identifiers are validated *and* bracket-quoted via ``utils.sql_safety``,
   so ``arc.ifdb_diagnose`` renders as ``[arc].[ifdb_diagnose]``.
3. Archive rows carry soft-delete audit columns: lookups filter to
   ``dw_is_deleted = 0`` unless ``include_deleted=True``. Pick per table:
   for archives with ``detect_deletes=True`` (full-load sources) a
   ``dw_is_deleted=1`` row is gone from Oracle, so the default filter is
   correct. For archives with ``source_delete_column`` (SLETTET/DELETED
   business flags) a soft-deleted row is still a real row the transform must
   see — it tombstones the FHIR resource from it. Pass
   ``include_deleted=True`` there (e.g. patient/encounter/episode_of_care);
   filtering the row out would make required references unresolvable.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any

from core.resources import MSSQLResource
from utils.sql_safety import safe_bracket_quote, safe_table_ref

# SQL Server's hard limit is 2100 parameters per statement; 2000 leaves headroom.
MSSQL_IN_CHUNK_LIMIT = 2000


def _dedupe_chunks(ids: Iterable[Any], chunk_size: int) -> list[list[str]]:
    deduped = sorted({str(i) for i in ids if i is not None})
    return [deduped[i : i + chunk_size] for i in range(0, len(deduped), chunk_size)]


def _execute_chunked(
    ids: Iterable[Any],
    *,
    table: str,
    columns: Sequence[str],
    where_column: str,
    mssql: MSSQLResource,
    chunk_size: int,
    include_deleted: bool,
) -> Iterator[dict[str, Any]]:
    if chunk_size <= 0 or chunk_size > MSSQL_IN_CHUNK_LIMIT:
        raise ValueError(f"chunk_size={chunk_size} must be in 1..{MSSQL_IN_CHUNK_LIMIT}")
    table_sql = safe_table_ref(table)
    where_sql = safe_bracket_quote(where_column)
    if not columns:
        raise ValueError("columns must be non-empty")
    cols_sql = ", ".join(safe_bracket_quote(c) for c in columns)
    deleted_filter = "" if include_deleted else " AND [dw_is_deleted] = 0"

    chunks = _dedupe_chunks(ids, chunk_size)
    if not chunks:
        return

    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for chunk in chunks:
                placeholders = ", ".join("?" for _ in chunk)
                query = (
                    f"SELECT {cols_sql} FROM {table_sql} "
                    f"WHERE {where_sql} IN ({placeholders}){deleted_filter}"
                )
                cursor.execute(query, chunk)
                result_cols = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    yield dict(zip(result_cols, row, strict=False))
        finally:
            cursor.close()


def chunked_in_fetch(
    ids: Iterable[Any],
    *,
    table: str,
    columns: Sequence[str],
    where_column: str,
    mssql: MSSQLResource,
    chunk_size: int = MSSQL_IN_CHUNK_LIMIT,
    include_deleted: bool = False,
) -> list[dict[str, Any]]:
    """Fetch archive rows by chunked IN-clause; return a flat list of dicts.

    ``ids`` is deduplicated and stringified before binding. Empty / all-None
    input returns ``[]`` without opening a cursor. ``table``, ``columns``,
    and ``where_column`` are validated and bracket-quoted; only positional
    binds are sent to the database.
    """
    return list(
        _execute_chunked(
            ids,
            table=table,
            columns=columns,
            where_column=where_column,
            mssql=mssql,
            chunk_size=chunk_size,
            include_deleted=include_deleted,
        )
    )


def chunked_in_fetch_grouped(
    ids: Iterable[Any],
    *,
    table: str,
    columns: Sequence[str],
    where_column: str,
    mssql: MSSQLResource,
    key_fn: Callable[[dict[str, Any]], str | None],
    sort_key: Callable[[dict[str, Any]], Any] | None = None,
    chunk_size: int = MSSQL_IN_CHUNK_LIMIT,
    include_deleted: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch archive rows by chunked IN-clause; group by ``key_fn(row)``.

    Rows where ``key_fn`` returns ``None`` are dropped. When ``sort_key`` is
    given, each group's list is sorted by it (in place) for deterministic
    downstream ordering — important for code that builds FHIR resources
    where coding order is observable.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _execute_chunked(
        ids,
        table=table,
        columns=columns,
        where_column=where_column,
        mssql=mssql,
        chunk_size=chunk_size,
        include_deleted=include_deleted,
    ):
        key = key_fn(row)
        if key is None:
            continue
        grouped.setdefault(key, []).append(row)
    if sort_key:
        for rows in grouped.values():
            rows.sort(key=sort_key)
    return grouped


__all__ = [
    "MSSQL_IN_CHUNK_LIMIT",
    "chunked_in_fetch",
    "chunked_in_fetch_grouped",
]
