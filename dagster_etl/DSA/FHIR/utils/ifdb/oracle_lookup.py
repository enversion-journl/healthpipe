# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Chunked Oracle IN-clause lookup for IFDB source tables.

Centralises the chunk-and-bind pattern used by IFDB-side helpers that fan
out a list of identifiers into ``SELECT ... WHERE col IN (...)`` against
IFDB. Two motivations:

1. Oracle has a hard 1000-element limit on IN-clauses. Every caller used
   to re-implement the chunking loop and the placeholder string.
2. Hand-rolled ``f"'{id}'"`` interpolation in 5+ call sites multiplied
   the SQL-injection surface. This module is the single place where binds
   are constructed (positional ``:1, :2, ...``), and table/column
   identifiers are validated against an allowlist before reaching the
   cursor.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any

import dagster as dg
from core.resources import IFDBOracleResource
from DSI.connectors.oracle.models import fetch_rows_as_dicts

# Oracle's hard limit on IN-list elements is 1000; 999 leaves headroom.
ORACLE_IN_CHUNK_LIMIT = 999

# Allowlist for SQL identifiers passed in as table / column names.
# Schema-qualified names (e.g. ``IFDB.PATIENT_OPLYSNINGER_COLUMNA``) allowed.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


def _validate_identifier(value: str, *, label: str) -> str:
    if not isinstance(value, str) or not _IDENT_RE.fullmatch(value):
        raise ValueError(
            f"Invalid SQL identifier for {label}: {value!r}. Must match "
            f"{_IDENT_RE.pattern} (schema-qualified names allowed)."
        )
    return value


def _dedupe_chunks(ids: Iterable[Any], chunk_size: int) -> list[list[str]]:
    deduped = sorted({str(i) for i in ids if i is not None})
    return [deduped[i : i + chunk_size] for i in range(0, len(deduped), chunk_size)]


def _execute_chunked(
    ids: Iterable[Any],
    *,
    table: str,
    columns: Sequence[str],
    where_column: str,
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
    chunk_size: int,
) -> Iterator[dict[str, Any]]:
    if chunk_size <= 0 or chunk_size > ORACLE_IN_CHUNK_LIMIT:
        raise ValueError(f"chunk_size={chunk_size} must be in 1..{ORACLE_IN_CHUNK_LIMIT}")
    table_sql = _validate_identifier(table, label="table")
    where_sql = _validate_identifier(where_column, label="where_column")
    if not columns:
        raise ValueError("columns must be non-empty")
    cols_sql = ", ".join(_validate_identifier(c, label="column") for c in columns)

    chunks = _dedupe_chunks(ids, chunk_size)
    if not chunks:
        return

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for chunk in chunks:
                placeholders = ", ".join(f":{i + 1}" for i in range(len(chunk)))
                query = (
                    f"SELECT {cols_sql} FROM {table_sql} " f"WHERE {where_sql} IN ({placeholders})"
                )
                cursor.execute(query, chunk)
                yield from fetch_rows_as_dicts(cursor, context)
        finally:
            cursor.close()


def chunked_in_fetch(
    ids: Iterable[Any],
    *,
    table: str,
    columns: Sequence[str],
    where_column: str,
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
    chunk_size: int = ORACLE_IN_CHUNK_LIMIT,
) -> list[dict[str, Any]]:
    """Fetch IFDB rows by chunked IN-clause; return a flat list of dicts.

    ``ids`` is deduplicated and stringified before binding. Empty / all-None
    input returns ``[]`` without opening a cursor. ``table``, ``columns``,
    and ``where_column`` are validated against an identifier allowlist;
    only positional binds are sent to the database.
    """
    return list(
        _execute_chunked(
            ids,
            table=table,
            columns=columns,
            where_column=where_column,
            ifdb_oracle=ifdb_oracle,
            context=context,
            chunk_size=chunk_size,
        )
    )


def chunked_in_fetch_grouped(
    ids: Iterable[Any],
    *,
    table: str,
    columns: Sequence[str],
    where_column: str,
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
    key_fn: Callable[[dict[str, Any]], str | None],
    sort_key: Callable[[dict[str, Any]], Any] | None = None,
    chunk_size: int = ORACLE_IN_CHUNK_LIMIT,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch IFDB rows by chunked IN-clause; group by ``key_fn(row)``.

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
        ifdb_oracle=ifdb_oracle,
        context=context,
        chunk_size=chunk_size,
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
    "ORACLE_IN_CHUNK_LIMIT",
    "chunked_in_fetch",
    "chunked_in_fetch_grouped",
]
