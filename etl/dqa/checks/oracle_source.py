# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared helper: stage the live merge keys of an IFDB Oracle table into a MSSQL
global ``##temp`` table, so ``arc.*`` archives can be diffed / cleaned against the
source's *current* contents.

The key-staging sibling of ``fhir_server.stage_fhir_server_ids`` (same batched
``bulkcopy``-with-``executemany``-fallback pattern, same global-temp rationale --
``bulkcopy`` opens its own connection, which cannot see a session ``#temp``).
Keys are normalized with the ETL's ``normalize_oracle_id`` so they compare equal
to what the extraction wrote into the NVARCHAR archive column: NUMBER ids lose
the ``.0`` artifact, and a DATE key (e.g. Location's PERIOD_START) stringifies to
the exact ``YYYY-MM-DD HH:MM:SS`` form the archive stores.

Composite merge keys: pass a sequence of columns and the staged table
gets ``k0..k{n-1}``; a single column keeps the historical ``k`` name, so existing
single-key SQL is unchanged. Rows where ANY key part is NULL are skipped -- the
arc primary key is NOT NULL, so they cannot correspond to an archived row.

Used by ``dqa_arc_ifdb_vanished_cleanup`` (soft-delete arc rows that vanished from
IFDB). ``archive_reconciliation`` keeps its own composite-key staging inline.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from dqa.checks.helpers import COLLATE_DB_DEFAULT
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id

_BATCH = 10_000
_BULK_TIMEOUT = 600


def _stage_ddl(stage: str, stage_cols: Sequence[str]) -> str:
    """``CREATE TABLE`` for ``##dqa_ifdb_keys``. Collation is pinned at DDL.

    DATABASE_DEFAULT is correct because this runs on ``mssql_target_staging``
    (same DB as ``arc.*``). Pinning the columns -- not the join predicate --
    keeps ``IX_dqa_ifdb_keys`` sargable.
    """
    cols = ", ".join(f"[{c}] NVARCHAR(255){COLLATE_DB_DEFAULT} NOT NULL" for c in stage_cols)
    return f"CREATE TABLE {stage} ({cols})"


def stage_ifdb_keys(
    conn,
    cur,
    ifdb_oracle,
    source_table: str,
    key_columns: str | Sequence[str],
    *,
    source_filter: str | None = None,
) -> tuple[str, int]:
    """Stream the DISTINCT live ``key_columns`` tuples of ``source_table`` (Oracle)
    into a new ``##temp`` table on the MSSQL connection.

    Returns ``(stage_table_name, loaded_count)``. The temp lives on ``conn`` (keep
    it open until the diff/update runs); it auto-drops when ``conn`` closes.
    """
    cols = [key_columns] if isinstance(key_columns, str) else list(key_columns)
    stage_cols = ["k"] if len(cols) == 1 else [f"k{i}" for i in range(len(cols))]

    stage = f"##dqa_ifdb_keys_{uuid.uuid4().hex}"
    cur.execute(_stage_ddl(stage, stage_cols))
    # Release the schema lock before bulkcopy()'s own connection reads the
    # staging metadata, else that connection blocks indefinitely.
    conn.commit()

    osql = f"SELECT DISTINCT {', '.join(cols)} FROM {source_table}"
    if source_filter:
        osql += f" WHERE {source_filter}"

    used_bulkcopy = hasattr(cur, "bulkcopy")
    bulkcopy_disabled = False
    insert_sql = (
        f"INSERT INTO {stage} ({', '.join(stage_cols)}) "
        f"VALUES ({', '.join(['?'] * len(stage_cols))})"
    )

    loaded = 0
    with ifdb_oracle.get_connection() as oconn:
        ocur = oconn.cursor()
        ocur.execute(osql)
        while True:
            rows = ocur.fetchmany(_BATCH)
            if not rows:
                break
            batch = [
                tuple(normalize_oracle_id(v) for v in r)
                for r in rows
                if all(v is not None for v in r)
            ]
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
                        column_mappings=stage_cols,
                    )
                except Exception:  # noqa: BLE001 -- fall back, don't fail
                    bulkcopy_disabled = True
                    cur.executemany(insert_sql, batch)
            else:
                cur.executemany(insert_sql, batch)
            loaded += len(batch)

    cur.execute(f"CREATE CLUSTERED INDEX IX_dqa_ifdb_keys ON {stage} ({', '.join(stage_cols)})")
    return stage, loaded
