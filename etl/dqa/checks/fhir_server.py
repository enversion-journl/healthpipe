# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared helper: stage the live resource ids from the FHIR server (Aidbox
PostgreSQL) into a MSSQL temp table, so flat ``fhir.*`` tables can be reconciled
against the server's *current* contents.

Aidbox keeps live resources in a table named after the resource type (lowercase,
e.g. ``encounter``) with the resource UUID in ``id``; a hard delete removes the
row (it survives only in ``<type>_history``). So ``SELECT id FROM <type>`` is the
set of ids that still exist on the server -- which equals ``fhir.<Table>``'s
resource-id column (e.g. ``encounter_id``).

Cross-DB (PostgreSQL -> MSSQL), so -- like ``source_archive_reconciliation`` -- ids are
streamed (server-side cursor) and ``bulkcopy``'d into a global ``##temp`` table
rather than pulled into Python; the diff/update then runs in MSSQL.
"""

from __future__ import annotations

import uuid

from dqa.checks.helpers import COLLATE_DB_DEFAULT

_BATCH = 10_000
_BULK_TIMEOUT = 600


def _stage_ddl(stage: str) -> str:
    """``CREATE TABLE`` for ``##dqa_fhir_ids``. Collation is pinned at DDL.

    DATABASE_DEFAULT is correct because this runs on ``mssql_target_staging``
    (same DB as ``fhir.*``). Pinning the column -- not the join predicate --
    keeps ``IX_dqa_fhir_ids`` sargable.
    """
    return f"CREATE TABLE {stage} ([id] NVARCHAR(255){COLLATE_DB_DEFAULT} NOT NULL)"


def stage_fhir_server_ids(conn, cur, fhir_database, resource_table: str) -> tuple[str, int]:
    """Stream live ids from Aidbox ``resource_table`` into a new ``##temp`` table.

    Returns ``(stage_table_name, loaded_count)``. The temp lives on ``conn`` (keep
    it open until the diff/update runs); it auto-drops when ``conn`` closes.
    """
    stage = f"##dqa_fhir_ids_{uuid.uuid4().hex}"
    cur.execute(_stage_ddl(stage))
    # Release the schema lock before bulkcopy()'s own connection reads the
    # staging metadata, else that connection blocks indefinitely.
    conn.commit()

    used_bulkcopy = hasattr(cur, "bulkcopy")
    bulkcopy_disabled = False
    insert_sql = f"INSERT INTO {stage} (id) VALUES (?)"

    loaded = 0
    with fhir_database.get_connection() as pg:
        # Server-side cursor: don't buffer the whole id set in client memory.
        with pg.cursor(name=f"dqa_fhir_ids_{uuid.uuid4().hex}") as pgcur:
            pgcur.execute(f"SELECT id FROM {resource_table}")
            while True:
                rows = pgcur.fetchmany(_BATCH)
                if not rows:
                    break
                batch = [(str(r[0]),) for r in rows if r[0] is not None]
                if not batch:
                    continue
                if used_bulkcopy and not bulkcopy_disabled:
                    try:
                        cur.bulkcopy(
                            stage,
                            batch,
                            batch_size=0,
                            timeout=_BULK_TIMEOUT,
                            table_lock=True,
                            column_mappings=["id"],
                        )
                    except Exception:  # noqa: BLE001 -- fall back, don't fail
                        bulkcopy_disabled = True
                        cur.executemany(insert_sql, batch)
                else:
                    cur.executemany(insert_sql, batch)
                loaded += len(batch)

    cur.execute(f"CREATE CLUSTERED INDEX IX_dqa_fhir_ids ON {stage} (id)")
    return stage, loaded


def not_deleted_pred(col: str, alias: str = "f") -> str:
    """SQL predicate for 'not soft-deleted' (dw_is_deleted INT: NULL or != 1).

    dw_is_deleted is INT NOT NULL DEFAULT 0 on every fhir.* table, so the NULL
    branch is dead once the tightening migration has run everywhere. It is kept
    deliberately: this asset may run against a database that has not been
    migrated yet, and there the NULL branch is what keeps live rows visible.
    """
    return f"({alias}.[{col}] IS NULL OR {alias}.[{col}] <> 1)"
