# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dso_reconciliation: a DSO table matches its DSA (staging) source after sync.

The DSO-sync copies each staging (DSA) table to the DSO/reporting database via the
``STAGING_LINK`` linked server. Right after a sync the two should be identical, so
this check -- run **on the DSO connection** -- diffs the DSO-local table against the
DSA table reached over the linked server:

- ``missing_in_dso``         : PK present in DSA but absent from DSO -> not synced.
- ``extra_in_dso``           : PK present in DSO but absent from DSA -> stale / a
                               delete that didn't propagate.
- ``delete_status_mismatch`` : PK on both sides but the soft-delete flag differs
                               (only when the table has a delete column).

The comparison is on the **full primary key** (composite-aware), which is unique on
each side, so the delete-flag comparison is a straight per-row equality (no MIN/MAX
aggregation needed, unlike the business-key ``archive_fhir_reconciliation``). All three
counts come from ONE ``FULL OUTER JOIN`` pass over the linked server -- separate
``NOT EXISTS`` / ``JOIN`` queries would re-read the whole remote table per count,
which hurts on Observation-scale tables.

Detection only -- no capture/replay yet (deferred until DSO-sync is on DFM).

params:
    pk:              list[str]   -- primary-key column(s), from the sync config.
    staging_db:      str         -- the DSA database name (linked-server target).
    linked_server:   str | None  -- linked-server name; "" / None -> same-instance
                                    3-part name (local dev).
    delete_columns:  list[str]   -- candidate soft-delete columns to auto-detect
                                    (default ["dw_is_deleted"]).
"""

from __future__ import annotations

from dqa.checks.helpers import COLLATE_DB_DEFAULT, get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check
from DSO.load.sync_utils import STRING_DATA_TYPES

_DELETE_CANDIDATES = ("dw_is_deleted",)

# The DSO (reporting) DB and the Nomad-hosted DSA/staging DB can have different
# default collations (they do on QA/Prod; local/Dev share one), so an equality or
# UNION on two character columns across the STAGING_LINK linked server raises
# "Cannot resolve the collation conflict". We coerce both sides to the DSO
# connection's own collation with COLLATE DATABASE_DEFAULT -- a functional no-op
# when the collations already match, so the identical SQL runs in every
# environment. COLLATE is only legal on character expressions, hence the per-column
# type check against STRING_DATA_TYPES (shared with the DSO-sync's MERGE handling,
# which solves the same mismatch the same way).
_CHAR_TYPES = STRING_DATA_TYPES
_COLLATE = COLLATE_DB_DEFAULT

# Fetching example keys re-runs the full missing/extra/mismatch diff a SECOND time
# over the STAGING_LINK linked server (correlated NOT EXISTS). On very large tables
# (e.g. fhir.Observation, fct.Faktura) that doubling is the expensive part, so above
# this many DSO rows we report the counts only and skip the example-key query.
# Overridable per check via the ``examples_max_checked`` param.
_EXAMPLES_MAX_CHECKED = 5_000_000


def _column_types(cur, schema: str, table: str) -> dict[str, str]:
    """Map ``COLUMN_NAME -> DATA_TYPE`` (lower-cased) for a DSO table.

    One INFORMATION_SCHEMA read serves both the soft-delete-column probe and the
    per-PK-column char detection that decides where collation coercion is needed.
    """
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (schema, table),
    )
    return {name: (dtype or "").lower() for name, dtype in cur.fetchall()}


def _collate_if_char(expr: str, is_char: bool) -> str:
    """Append COLLATE DATABASE_DEFAULT to *character* expressions only."""
    return expr + _COLLATE if is_char else expr


def _deleted_expr(alias: str, col: str) -> str:
    # The CAST result is NVARCHAR, so COLLATE is always legal here; it aligns a
    # possibly-remote (DSA) char delete column with the local '1' literal's collation.
    return f"CASE WHEN CAST({alias}.[{col}] AS NVARCHAR(10)){_COLLATE} = '1' THEN 1 ELSE 0 END"


def _pk_key_expr(alias: str, pk: list[str]) -> str:
    """A single NVARCHAR expression identifying a row by its (composite) PK.

    Each cast carries COLLATE DATABASE_DEFAULT so the examples ``UNION ALL`` -- whose
    branches read from the local DSO and the linked DSA server -- can reconcile the
    ``k`` column without a collation conflict.
    """
    parts = [f"CAST({alias}.[{c}] AS NVARCHAR(100)){_COLLATE}" for c in pk]
    return " + '|' + ".join(parts) if len(parts) > 1 else parts[0]


@register_check("dso_reconciliation")
def dso_reconciliation(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    schema, table = split_table(subject.table)
    pk: list[str] = list(p["pk"])
    staging_db = p["staging_db"]
    link = p.get("linked_server") or ""
    candidates = tuple(p.get("delete_columns", _DELETE_CANDIDATES))

    dso = f"[{schema}].[{table}]"  # local on the DSO server
    dsa = (
        f"[{link}].[{staging_db}].[{schema}].[{table}]"
        if link
        else f"[{staging_db}].[{schema}].[{table}]"
    )
    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        col_types = _column_types(cur, schema, table)
        delete_col = next((c for c in candidates if c in col_types), None)
        # Char PK columns need COLLATE DATABASE_DEFAULT on both sides of the
        # cross-server join to survive a DSO/DSA collation mismatch (QA/Prod).
        char_pk = {c for c in pk if col_types.get(c, "") in _CHAR_TYPES}
        on = " AND ".join(
            f"{_collate_if_char(f'd.[{c}]', c in char_pk)}"
            f" = {_collate_if_char(f's.[{c}]', c in char_pk)}"
            for c in pk
        )
        context.log.info(
            f"dso_reconciliation: {dso} vs {dsa} on PK {pk}"
            + (f", char PK cols {sorted(char_pk)} collated" if char_pk else "")
            + (f", delete col '{delete_col}'" if delete_col else " (key-set only)")
        )

        cur.execute(f"SELECT COUNT(*) FROM {dso}")
        checked = int(cur.fetchone()[0] or 0)

        # All three counts in ONE pass over the linked server (FULL OUTER JOIN on
        # the PK) -- separate NOT EXISTS / JOIN queries would each re-read the whole
        # remote table. PK columns are NOT NULL on both sides, so a NULL first PK
        # column after the outer join reliably means "no row on that side".
        pk0 = pk[0]
        mismatch_expr = (
            f"CASE WHEN d.[{pk0}] IS NOT NULL AND s.[{pk0}] IS NOT NULL "
            f"AND {_deleted_expr('d', delete_col)} <> {_deleted_expr('s', delete_col)} "
            f"THEN 1 ELSE 0 END"
            if delete_col
            else "0"
        )
        cur.execute(
            f"SELECT "
            f"SUM(CASE WHEN d.[{pk0}] IS NULL THEN 1 ELSE 0 END) AS missing_in_dso, "
            f"SUM(CASE WHEN s.[{pk0}] IS NULL THEN 1 ELSE 0 END) AS extra_in_dso, "
            f"SUM({mismatch_expr}) AS delete_mismatch "
            f"FROM {dso} d FULL OUTER JOIN {dsa} s ON {on}"
        )
        missing, extra, delete_mismatch = (int(v or 0) for v in cur.fetchone())

        examples: list[dict] = []
        examples_max = int(p.get("examples_max_checked", _EXAMPLES_MAX_CHECKED))
        examples_skipped = False
        if (missing or extra or delete_mismatch) and checked <= examples_max:
            missing_src = f"FROM {dsa} s WHERE NOT EXISTS (SELECT 1 FROM {dso} d WHERE {on})"
            extra_src = f"FROM {dso} d WHERE NOT EXISTS (SELECT 1 FROM {dsa} s WHERE {on})"
            branches = [
                f"SELECT 'missing_in_dso' AS kind, {_pk_key_expr('s', pk)} AS k, "
                f"CAST(NULL AS NVARCHAR(100)) AS detail {missing_src}",
                f"SELECT 'extra_in_dso', {_pk_key_expr('d', pk)}, "
                f"CAST(NULL AS NVARCHAR(100)) {extra_src}",
            ]
            if delete_col:
                mismatch_src = (
                    f"FROM {dso} d JOIN {dsa} s ON {on} "
                    f"WHERE {_deleted_expr('d', delete_col)} <> {_deleted_expr('s', delete_col)}"
                )
                branches.append(
                    f"SELECT 'delete_status_mismatch', {_pk_key_expr('d', pk)}, "
                    f"CONCAT('dso_deleted=', {_deleted_expr('d', delete_col)}, "
                    f"',dsa_deleted=', {_deleted_expr('s', delete_col)}) {mismatch_src}"
                )
            cur.execute(
                "SELECT TOP 30 kind, k, detail FROM (" + " UNION ALL ".join(branches) + ") q"
            )
            for kind, key, detail in cur.fetchall():
                ex = {"kind": kind, "key": key}
                if detail:
                    ex["detail"] = detail
                examples.append(ex)
        elif missing or extra or delete_mismatch:
            # Large table: don't pay a second full linked-server diff just for examples.
            examples_skipped = True
            context.log.info(
                f"dso_reconciliation: {dso} has {checked} rows (> {examples_max}); reporting "
                f"counts only and skipping the example-key query (second linked-server diff)."
            )

    violations = missing + extra + delete_mismatch
    extra_meta = {
        "staging_db": staging_db,
        "linked_server": link or "(direct)",
        "primary_key": pk,
        "missing_in_dso": missing,
        "extra_in_dso": extra,
    }
    if delete_col:
        extra_meta["delete_status_mismatch"] = delete_mismatch
        extra_meta["delete_column"] = delete_col
    if examples_skipped:
        extra_meta["examples_skipped_large_table"] = True
    return CheckOutcome(
        passed=violations == 0,
        checked=checked,
        violations=violations,
        examples=examples,
        extra=extra_meta,
    )
