# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""archive_reconciliation check: an arc.* archive matches its DFM source.

Completes the lineage <source> -> arc (MSSQL) -> downstream, for any source the
platform extracts + archives via DFM (IFDB Oracle today; SydAX/MySupply/ILS/KMD
MSSQL sources use the same shared factory + ``core.archive_merge``). Which SQL
dialect the SOURCE speaks is a parameter (``dialect``); the diff itself always
runs in MSSQL against the staged key set.

It's cross-database and the archives are large (IFDB.LOCATION is ~14.4M rows at
the archive grain), so rather than pulling every key into Python it **stages**
the source keys and diffs in SQL:

1. ``SELECT <key columns> ... GROUP BY`` from the source (server-side distinct),
   streamed in 10k batches into a *global* ``##dqa_recon_<uuid>`` temp table on
   the MSSQL connection via ``bulkcopy`` (executemany fallback). For an Oracle
   source, id columns are normalized with the ETL's ``normalize_oracle_id``
   (strips the ``.0`` NUMBER artifact); other columns are stringified with
   ``str()`` -- the same form the extraction stored in the NVARCHAR archive
   column. MSSQL sources have no NUMBER artifact -> nothing normalized by
   default (``normalize_columns`` overrides either way).
2. The diff runs entirely in MSSQL (``NOT EXISTS`` / ``LEFT JOIN`` over the
   indexed composite key) -- O(batch) Python memory, no full key set in process.

The key is the archive's **merge key** and may be composite (``key_columns`` is a
list). Location's grain is ``(OBJECT_ID, PERIOD_START)``, so a single-column key
would collapse ~14.4M archived rows to ~hundreds of distinct OBJECT_IDs and miss
real gaps -- the keys must mirror ``ArchiveMergeConfig.merge_keys``.

Reports (per composite key tuple):
- ``missing_in_arc``  : key in the source but not in arc  -> the archive missed it (BAD).
- ``extra_in_arc``    : key in arc but not in the source  -> the source hard-deleted it.
    * marked deleted (dw_is_deleted=1) -> expected (archive retired it).
    * NOT marked        -> the archive didn't soft-delete a vanished row (BAD).

A **global** ``##temp`` table (uuid-suffixed, sof_utils pattern) is used so that
``bulkcopy()`` -- which opens its own internal connection for the TDS bulk insert
-- can see it; a session ``#temp`` is invisible to that second connection. The
CREATE is committed before the first ``bulkcopy`` so its connection isn't blocked
by the schema lock. The table auto-drops when the staging connection closes.

params:
    source_table:       str         -- source table, e.g. "IFDB.LOCATION"
    key_columns:        list[str]   -- merge key, same names on both sides,
                                       e.g. ["OBJECT_ID", "PERIOD_START"]
    dialect:            str         -- SQL dialect of the SOURCE: "oracle" (default)
                                       or "mssql". Drives the flow-start bound
                                       expression and the default key normalization.
    source_resource:    str         -- Dagster resource key of the SOURCE connection
                                       (default "ifdb_oracle"). Must also be listed
                                       in the CheckSpecification's ``requires``.
    normalize_columns:  list[str]   -- subset of key_columns to run through
                                       normalize_oracle_id. Default: for "oracle",
                                       the columns whose name ends in "_ID"; for
                                       "mssql", none.
    source_filter:      str | None  -- optional source-side WHERE
    deleted_column:     str | None  -- arc "removed from source" marker, e.g. "dw_is_deleted"
    flow_start_bound_column: str | None -- source upper-bound column (e.g.
                                       "IFDB_MODIFICATION_DATETIME"). When set, the
                                       MISSING direction only counts source keys
                                       whose upper-bound column is <= the last DFM
                                       flow-start, mirroring the delta extract --
                                       so rows that arrived after the last archive
                                       run aren't false gaps. Opt-in (delta tables
                                       only); leave unset for full-load sources.
    flow_start_state:   list[str]   -- the ``meta.DFMState`` row the flow-start
                                       checkpoint lives in, as [schema_name,
                                       table_name] -- i.e. the extract config's
                                       ``flow_start_from``. Default
                                       ["dfm", "ifdb_flow_start"] (IFDB).
    capture_replay:     bool        -- when True and the subject has a ReplaySpec,
                                       enqueue the missing_in_arc keys to
                                       dqa.RemediationQueue for dqa_arc_ifdb_replay
                                       (ids-mode re-extract + archive merge).
                                       Single-column merge keys only.
    missing_only:       bool        -- skip the EXTRA/vanished direction. For a
                                       caller whose source is bounded (e.g. one
                                       backfill window) while arc is not: every
                                       arc key outside the bound would count as
                                       extra, and the full-arc anti-join would be
                                       paid for a number nobody can use.

The flow-start bound applies to MISSING only. The EXTRA/vanished direction always
compares arc against the FULL live source key set -- otherwise a key still present
in the source but modified after flow-start would be a false "vanished".

requires: the ``source_resource`` key, e.g. ("ifdb_oracle",)
"""

from __future__ import annotations

import time
import uuid

from dqa.checks.helpers import COLLATE_DB_DEFAULT, get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id

# Keep batches <= 10k: mssql-python's bulkcopy() is known to wedge on ~15-20k rows
# (see DSA/FHIR/utils/sql_on_fhir/sof_utils.py).
_BATCH = 10_000
_BULK_TIMEOUT = 600  # seconds per bulkcopy call

_DIALECTS = ("oracle", "mssql")


def _stage_ddl(stage: str, kcols: list[str]) -> str:
    """``CREATE TABLE`` for ``##dqa_recon``. Collation is pinned at DDL.

    DATABASE_DEFAULT is correct because this runs on ``mssql_target_staging``
    (same DB as ``arc.*`` and ``dqa.RemediationQueue``). Pinning the key
    columns -- not the join / MERGE predicate -- keeps ``IX_dqa_recon``
    sargable. ``within_bound`` is INT; COLLATE is illegal on non-character types.
    """
    keys = ", ".join(f"[{kc}] NVARCHAR(255){COLLATE_DB_DEFAULT} NULL" for kc in kcols)
    return f"CREATE TABLE {stage} ({keys}, [within_bound] INT NULL)"


def _read_flow_start(cur, schema_name: str, table_name: str) -> str | None:
    """Latest flow-start checkpoint from meta.DFMState (staging scope).

    ``(schema_name, table_name)`` is the extract config's ``flow_start_from`` --
    the same row the DFM delta extract reads for its upper bound. Degrades to
    None (unbounded) if the table/row is absent, never raising.
    """
    try:
        cur.execute(
            "SELECT checkpoint_value FROM [meta].[DFMState] "
            "WHERE scope = 'staging' AND target_type = 'dfm' "
            "AND schema_name = ? AND table_name = ?",
            (schema_name, table_name),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:  # noqa: BLE001 -- degrade to unbounded
        return None


def _bound_literal(dialect: str, flow_start: str) -> str:
    """The flow-start timestamp as a datetime literal in the SOURCE dialect.

    Fractional seconds are stripped ('YYYY-MM-DD HH:MM:SS'), matching how the DFM
    extracts normalize checkpoints before comparing.
    """
    fs_norm = flow_start.split(".")[0].strip()
    if dialect == "oracle":
        return f"TO_DATE('{fs_norm}', 'YYYY-MM-DD HH24:MI:SS')"
    return f"CONVERT(DATETIME2, '{fs_norm}', 120)"  # mssql, ODBC canonical format


def _stringify(value, normalize: bool):
    """Canonicalize one source key value to the string the archive stored.

    Normalized columns get ``normalize_oracle_id`` (strips the stray ``.0`` Oracle
    NUMBER artifact, mirroring the ETL). Everything else is ``str()`` -- e.g. an
    Oracle DATE comes back as a datetime whose ``str()`` is
    ``YYYY-MM-DD HH:MM:SS``, exactly what the extraction wrote into the archive's
    NVARCHAR column.
    """
    if value is None:
        return None
    if normalize:
        return normalize_oracle_id(value)
    return str(value)


@register_check("archive_reconciliation")
def archive_reconciliation(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    dialect = p.get("dialect", "oracle")
    if dialect not in _DIALECTS:
        raise ValueError(
            f"archive_reconciliation on {subject.table}: unknown dialect {dialect!r}; "
            f"expected one of {_DIALECTS}."
        )
    source_resource = p.get("source_resource", "ifdb_oracle")
    fs_schema, fs_table = p.get("flow_start_state", ["dfm", "ifdb_flow_start"])

    schema, table = split_table(subject.table)
    arc = f"[{schema}].[{table}]"
    key_cols = list(p["key_columns"])
    n = len(key_cols)
    if "normalize_columns" in p:
        norm = set(p["normalize_columns"] or [])
    elif dialect == "oracle":
        # Oracle NUMBER ids come back as floats -> strip the ".0" artifact.
        norm = {c for c in key_cols if c.upper().endswith("_ID")}
    else:
        norm = set()  # mssql sources have no NUMBER artifact
    norm_flags = [c in norm for c in key_cols]
    deleted_column = p.get("deleted_column")
    deleted_expr = (
        f"MAX(CASE WHEN CAST([{deleted_column}] AS NVARCHAR(10)) = '1' THEN 1 ELSE 0 END)"
        if deleted_column
        else "0"
    )

    kcols = [f"k{i}" for i in range(n)]  # staging temp column names
    fs_col = p.get("flow_start_bound_column")  # opt-in source upper-bound column
    stage = f"##dqa_recon_{uuid.uuid4().hex}"

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()

        # Optional flow-start upper bound (mirrors the DFM delta extract). The
        # archive only holds rows whose upper-bound column is <= the last flow-start;
        # without this, live source rows newer than the last archive run look like
        # gaps. Staged per key as ``within_bound`` and applied to MISSING only --
        # the EXTRA/vanished direction ignores it (else a key still in the source
        # but modified after flow-start would be a false "vanished").
        within_expr = "1"
        flow_start = None
        if fs_col:
            flow_start = _read_flow_start(cur, fs_schema, fs_table)
            if flow_start:
                within_expr = (
                    f"MAX(CASE WHEN ({fs_col} <= {_bound_literal(dialect, flow_start)} "
                    f"OR {fs_col} IS NULL) THEN 1 ELSE 0 END)"
                )
            else:
                context.log.warning(
                    f"archive_reconciliation: flow_start_bound_column set but no "
                    f"meta.DFMState row for ({fs_schema}, {fs_table}); "
                    f"reconciling unbounded."
                )

        osql = (
            f"SELECT {', '.join(key_cols)}, {within_expr} AS within_bound FROM {p['source_table']}"
        )
        if p.get("source_filter"):
            osql += f" WHERE {p['source_filter']}"
        osql += f" GROUP BY {', '.join(key_cols)}"

        cols = kcols + ["within_bound"]
        cur.execute(_stage_ddl(stage, kcols))
        # Release the schema lock before bulkcopy()'s own connection reads the
        # staging metadata, else that connection blocks indefinitely.
        conn.commit()

        used_bulkcopy = hasattr(cur, "bulkcopy")
        bulkcopy_disabled = False
        insert_sql = (
            f"INSERT INTO {stage} ({', '.join(f'[{c}]' for c in cols)}) "
            f"VALUES ({', '.join(['?'] * (n + 1))})"
        )

        loaded = skipped_null = 0
        with getattr(resources, source_resource).get_connection() as sconn:
            scur = sconn.cursor()
            scur.execute(osql)
            while True:
                rows = scur.fetchmany(_BATCH)
                if not rows:
                    break
                batch = []
                for r in rows:
                    vals = tuple(_stringify(r[i], norm_flags[i]) for i in range(n))
                    if any(v is None for v in vals):
                        skipped_null += 1
                        continue
                    batch.append(vals + (int(r[n] or 0),))
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
                            column_mappings=cols,
                        )
                    except Exception as exc:  # noqa: BLE001
                        bulkcopy_disabled = True
                        context.log.warning(
                            f"bulkcopy into {stage} failed ({exc}); falling back to executemany"
                        )
                        cur.executemany(insert_sql, batch)
                else:
                    cur.executemany(insert_sql, batch)
                loaded += len(batch)

        strategy = "bulkcopy" if used_bulkcopy and not bulkcopy_disabled else "executemany"
        bound = f"bounded {fs_col} <= {flow_start}" if (fs_col and flow_start) else "unbounded"
        context.log.info(
            f"archive_reconciliation: staged {loaded} source keys via {strategy} "
            f"({skipped_null} skipped for NULL key part) from {p['source_table']} "
            f"[{dialect}, {bound}]"
        )
        cur.execute(
            f"CREATE CLUSTERED INDEX IX_dqa_recon ON {stage} "
            f"({', '.join(f'[{kc}]' for kc in kcols)})"
        )
        # COMMIT the staging work (executemany-fallback INSERTs + the index DDL)
        # BEFORE anything reads the temp from ANOTHER session. Autocommit is off, so
        # the CREATE INDEX otherwise holds its Sch-M lock until this transaction
        # ends -- and the capture below opens its OWN connection to read the temp,
        # which would block on that lock FOREVER (no lock timeout). Same reasoning
        # as the commit after CREATE TABLE above.
        conn.commit()
        context.log.info(
            f"archive_reconciliation: staging indexed; diffing against {arc} "
            f"(full key-set anti-joins -- can take a while on large archives)..."
        )
        t0 = time.perf_counter()

        # ---- set-logic in MSSQL over the composite key ----
        # src dedups keys; within_bound=1 if ANY row of the key is within the bound.
        src = (
            f"(SELECT {', '.join(kcols)}, MAX([within_bound]) AS within_bound FROM {stage} "
            f"GROUP BY {', '.join(kcols)}) s"
        )
        join_a_s = " AND ".join(f"a.[{key_cols[i]}] = s.{kcols[i]}" for i in range(n))
        join_s_ak = " AND ".join(f"s.{kcols[i]} = ak.{kcols[i]}" for i in range(n))
        arc_keys = (
            f"(SELECT {', '.join(f'[{key_cols[i]}] AS {kcols[i]}' for i in range(n))}, "
            f"{deleted_expr} AS d FROM {arc} "
            f"WHERE {' AND '.join(f'[{c}] IS NOT NULL' for c in key_cols)} "
            f"GROUP BY {', '.join(f'[{c}]' for c in key_cols)}) ak"
        )

        cur.execute(
            f"SELECT COUNT(*), SUM(CASE WHEN within_bound = 1 THEN 1 ELSE 0 END) FROM {src}"
        )
        source_count, within_count = (int(v or 0) for v in cur.fetchone())

        # MISSING: within-bound source keys absent from arc (the archive gap).
        missing_pred = f"s.within_bound = 1 AND NOT EXISTS (SELECT 1 FROM {arc} a WHERE {join_a_s})"
        cur.execute(f"SELECT COUNT(*) FROM {src} WHERE {missing_pred}")
        missing = int(cur.fetchone()[0] or 0)

        # EXTRA/vanished: arc keys absent from the FULL live source (bound ignored).
        if p.get("missing_only"):
            arc_total, extra, stale = -1, 0, 0
        else:
            cur.execute(
                f"SELECT COUNT(*), "
                f"SUM(CASE WHEN s.{kcols[0]} IS NULL THEN 1 ELSE 0 END), "
                f"SUM(CASE WHEN s.{kcols[0]} IS NULL AND ak.d = 0 THEN 1 ELSE 0 END) "
                f"FROM {arc_keys} LEFT JOIN {src} ON {join_s_ak}"
            )
            arc_total, extra, stale = (int(v or 0) for v in cur.fetchone())
        extra_part = (
            "extra direction skipped (missing_only)"
            if p.get("missing_only")
            else f"arc={arc_total}, extra_in_arc={extra} (vanished_not_marked_deleted={stale})"
        )
        context.log.info(
            f"archive_reconciliation: diff done in {time.perf_counter() - t0:.1f}s -- "
            f"source={source_count} (within bound: {within_count}), "
            f"missing_in_arc={missing}, {extra_part}"
        )

        examples = []
        if missing:
            cur.execute(
                f"SELECT TOP 15 {', '.join('s.' + kc for kc in kcols)} FROM {src} "
                f"WHERE {missing_pred}"
            )
            examples += [
                {"kind": "missing_in_arc", "key": " | ".join(str(c) for c in row)}
                for row in cur.fetchall()
            ]
        if stale:
            cur.execute(
                f"SELECT TOP 15 {', '.join('ak.' + kc for kc in kcols)} "
                f"FROM {arc_keys} LEFT JOIN {src} ON {join_s_ak} "
                f"WHERE s.{kcols[0]} IS NULL AND ak.d = 0"
            )
            examples += [
                {"kind": "vanished_not_marked_deleted", "key": " | ".join(str(c) for c in row)}
                for row in cur.fetchall()
            ]

        # Enqueue the missing keys for the arc ids-mode replay (dqa_arc_ifdb_replay).
        # Must run while the ##staging temp is still alive (global temps are visible
        # to the capture's own connection; the commit above released this session's
        # locks on it, so that read cannot block). Single-key subjects only -- the
        # replay filters ONE source column; composite grains (Location) are excluded.
        if missing and p.get("capture_replay") and subject.replay is not None and n == 1:
            context.log.info(
                f"archive_reconciliation: capturing {missing} missing keys to "
                f"dqa.RemediationQueue for {subject.replay.entity}..."
            )
            try:
                from dqa.remediation.capture import capture_archive_reconciliation_gaps

                capture_archive_reconciliation_gaps(
                    context,
                    subject,
                    resources,
                    missing_source=f"FROM {src} WHERE {missing_pred}",
                    key_expr=f"s.{kcols[0]}",
                    source_table=p["source_table"],
                )
            except Exception as exc:  # never let capture fail the check
                context.log.warning(f"DQA archive_reconciliation capture failed (non-fatal): {exc}")

        # Best-effort drop; the global temp also auto-drops when *conn* closes.
        try:
            cur.execute(f"IF OBJECT_ID('tempdb..{stage}') IS NOT NULL DROP TABLE {stage}")
        except Exception:  # noqa: BLE001
            pass

    bounded = bool(fs_col and flow_start)
    violations = missing + stale
    return CheckOutcome(
        passed=violations == 0,
        checked=within_count if bounded else source_count,
        violations=violations,
        examples=examples,
        extra={
            "source_table": p["source_table"],
            "dialect": dialect,
            "key_columns": ",".join(key_cols),
            "source_count": source_count,
            "within_flow_start": within_count if bounded else source_count,
            "flow_start_bound": f"{fs_col} <= {flow_start}" if bounded else "unbounded",
            "arc_total": arc_total,
            "missing_in_arc": missing,
            "extra_in_arc": extra,
            "vanished_not_marked_deleted": stale,
            "vanished_marked_deleted": extra - stale,
            "source_null_keys": skipped_null,
        },
    )
