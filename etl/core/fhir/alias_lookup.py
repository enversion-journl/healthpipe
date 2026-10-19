# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Identifier lookups that see every alias, served by the value GIN index.

Patient carries aliases (EPJ's other patient ids, previous CPR numbers) that the
UNIQUE expression index does not see, so every value is probed through the GIN
index in ``utils.fhir.index_config`` and the system is rechecked on the candidate
rows. ``@> ANY(array)`` cannot use a GIN index, so the needles are joined through
``unnest``: one index probe per needle. The planner picks a sequential scan for
this join on its own, so ``enable_seqscan = off`` is set for the transaction.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence

from psycopg import sql
from utils.fhir.index_config import ALIAS_GIN_EXPRESSION

# Needles per statement; the SQL Server-side lookups chunk at the same order of magnitude.
CHUNK = 5_000


def _tving_indeks(cursor) -> None:
    """Disable sequential scans for the rest of the transaction (the connection when autocommit)."""
    i_transaktion = not getattr(getattr(cursor, "connection", None), "autocommit", False)
    cursor.execute(f"SET {'LOCAL ' if i_transaktion else ''}enable_seqscan = off")


def needle(system: str, value: str) -> str:
    """The containment operand for one (system, value): a one-element identifier list."""
    return json.dumps([{"system": system, "value": value}])


def _chunks(items: Sequence, size: int | None = None):
    size = size or CHUNK
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def alias_matches(cursor, table: str, pairs: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """(id, value) for every resource carrying one of the ``(system, value)`` pairs."""
    query = sql.SQL(
        "SELECT t.id::text, n.value "
        "FROM unnest(%s::text[], %s::text[]) AS n(system, value) "
        "JOIN {table} t ON {expression} @> to_jsonb(n.value) "
        "AND t.resource->'identifier' @> "
        "jsonb_build_array(jsonb_build_object('system', n.system, 'value', n.value))"
    ).format(
        table=sql.Identifier(table),
        expression=sql.SQL(ALIAS_GIN_EXPRESSION.replace("resource->", "t.resource->")),
    )
    found: list[tuple[str, str]] = []
    portioner = _chunks([(str(s), str(v)) for s, v in pairs])
    for nummer, chunk in enumerate(portioner):
        if nummer == 0:
            _tving_indeks(cursor)
        cursor.execute(query, ([s for s, _ in chunk], [v for _, v in chunk]))
        found.extend((str(fid), str(value)) for fid, value in cursor.fetchall())
    return found


def alias_resources(cursor, table: str, pairs: Sequence[tuple[str, str]]) -> dict[str, list]:
    """id -> stored identifier list for every resource carrying one of the pairs.

    The ids come from ``alias_matches`` and the lists are then fetched by primary
    key; selecting the documents in the probe query would force a DISTINCT over jsonb.
    """
    ids = {fid for fid, _ in alias_matches(cursor, table, pairs)}
    if not ids:
        return {}
    found: dict[str, list] = {}
    query = sql.SQL(
        "SELECT t.id::text, t.resource->'identifier' FROM {table} t WHERE t.id = ANY(%s)"
    ).format(table=sql.Identifier(table))
    for chunk in _chunks(sorted(ids)):
        cursor.execute(query, (chunk,))
        for fid, identifiers in cursor.fetchall():
            found[str(fid)] = list(identifiers or [])
    return found


def lookup_values(
    cursor, *, resource_type: str, system: str, values: Iterable[str]
) -> dict[str, set[str]]:
    """value -> ids of EVERY resource carrying it as any identifier of ``system``.

    Every value is probed by containment, never short-circuited through the
    expression index: a value that is the first identifier on one resource and
    an alias on another must come back with both ids, so the caller can raise
    the conflict instead of silently picking the first resource.
    """
    wanted = sorted({str(v).strip() for v in values if v is not None and str(v).strip()})
    result: dict[str, set[str]] = {}
    if not wanted:
        return result
    table = resource_type.lower()
    for fid, value in alias_matches(cursor, table, [(system, v) for v in wanted]):
        result.setdefault(value, set()).add(fid)
    return result


__all__ = ["CHUNK", "alias_matches", "alias_resources", "lookup_values", "needle"]
