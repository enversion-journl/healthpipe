# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""source_reconciliation check: the flat table's business keys match the source.

Reconciles the distinct business identifiers in the subject's flat table against
the source-of-truth archive (``arc.*``) -- over the FULL key set (live + deleted),
and also checks that the soft-delete flag agrees for keys present in both:

- ``missing_in_target``      : in the source but absent from the flat table -> NOT loaded.
- ``extra_in_target``        : in the flat table but absent from the source -> orphan /
  duplicate / stale (e.g. the leftover from deleting duplicate FHIR resources).
- ``delete_status_mismatch`` : present in both, but soft-deleted on one side only
  -> a delete that didn't propagate (or a resurrection).

Both tables must live in the same database (``subject.resource_key``) -- the
common case, since ``arc.*`` and ``[fhir].*`` are both in DSA. The diff runs in
SQL (NOT EXISTS / JOIN over DISTINCT CTEs), so it scales to large archives.

params:
    source_table:    str          -- e.g. "arc.ifdb_encounter"
    source_key:      str          -- business key column, e.g. "OBJECT_ID"
    target_key:      str          -- business key column on the flat table, e.g. "identifier_value"
    source_deleted:  str | None   -- arc soft-delete column ('1' = deleted), e.g. "DELETED"
    target_deleted:  str | None   -- flat-table soft-delete column (1 = deleted), e.g. "dw_is_deleted"
    source_filter:   str | None   -- optional extra scoping on the source
    target_filter:   str | None   -- optional extra scoping on the target
    capture_replay:  bool         -- when True and the subject has a ReplaySpec,
                                     enqueue the gap keys (missing + arc-deleted
                                     mismatch) to dqa.RemediationQueue for
                                     dqa_remediation_replay. Only safe when
                                     source_key == ReplaySpec.filter_column.
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


def _where(key: str, extra: str | None) -> str:
    clause = f"WHERE [{key}] IS NOT NULL"
    if extra:
        clause += f" AND ({extra})"
    return clause


def _deleted_expr(col: str | None) -> str:
    """1 when the (string-or-int) flag equals 1/'1', else 0 (NULL -> 0)."""
    if not col:
        return "0"
    return f"CASE WHEN CAST([{col}] AS NVARCHAR(10)) = '1' THEN 1 ELSE 0 END"


@register_check("source_reconciliation")
def source_reconciliation(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    s_schema, s_table = split_table(p["source_table"])
    t_schema, t_table = split_table(subject.table)
    sk, tk = p["source_key"], p["target_key"]
    compare_deleted = bool(p.get("source_deleted") and p.get("target_deleted"))

    # A business key can map to several rows (resource versions): the live one plus
    # soft-deleted stragglers that dqa_fhir_server_cleanup left behind (old versions
    # hard-deleted in Aidbox). The key is "live" iff AT LEAST ONE row is non-deleted,
    # so aggregate the delete flag with MIN (deleted iff ALL rows deleted), not MAX
    # -- MAX would let a single deleted straggler mask a live row and emit a phantom
    # delete_status_mismatch (source live, "target deleted").
    cte = (
        f"WITH src AS (SELECT [{sk}] AS k, MIN({_deleted_expr(p.get('source_deleted'))}) AS d "
        f"FROM [{s_schema}].[{s_table}] {_where(sk, p.get('source_filter'))} GROUP BY [{sk}]), "
        f"tgt AS (SELECT [{tk}] AS k, MIN({_deleted_expr(p.get('target_deleted'))}) AS d "
        f"FROM [{t_schema}].[{t_table}] {_where(tk, p.get('target_filter'))} GROUP BY [{tk}])"
    )

    # The delete-status comparison only runs when BOTH delete columns are given.
    # Otherwise src.d is a constant 0 (no source delete col), so a src.d <> tgt.d
    # branch would report every deleted target row as a bogus mismatch -- skip it
    # in both the count and the examples.
    mismatch_count = (
        "(SELECT COUNT(*) FROM src JOIN tgt ON tgt.k = src.k WHERE src.d <> tgt.d)"
        if compare_deleted
        else "0"
    )

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(
            cte
            + f"""
            SELECT
                (SELECT COUNT(*) FROM src) AS source_count,
                (SELECT COUNT(*) FROM tgt) AS target_count,
                (SELECT COUNT(*) FROM src WHERE NOT EXISTS (SELECT 1 FROM tgt WHERE tgt.k = src.k)) AS missing,
                (SELECT COUNT(*) FROM tgt WHERE NOT EXISTS (SELECT 1 FROM src WHERE src.k = tgt.k)) AS extra,
                {mismatch_count} AS status_mismatch
            """
        )
        source_count, target_count, missing, extra, status_mismatch = (
            int(v or 0) for v in cur.fetchone()
        )

        examples = []
        if missing or extra or status_mismatch:
            branches = [
                "SELECT 'missing_in_target' AS kind, src.k AS k, CAST(NULL AS NVARCHAR(100)) AS detail "
                "FROM src WHERE NOT EXISTS (SELECT 1 FROM tgt WHERE tgt.k = src.k)",
                "SELECT 'extra_in_target', tgt.k, CAST(NULL AS NVARCHAR(100)) "
                "FROM tgt WHERE NOT EXISTS (SELECT 1 FROM src WHERE src.k = tgt.k)",
            ]
            if compare_deleted:
                branches.append(
                    "SELECT 'delete_status_mismatch', src.k, "
                    "CONCAT('source_deleted=', src.d, ',target_deleted=', tgt.d) "
                    "FROM src JOIN tgt ON tgt.k = src.k WHERE src.d <> tgt.d"
                )
            cur.execute(
                cte + " SELECT TOP 30 kind, k, detail FROM (" + " UNION ALL ".join(branches) + ") d"
            )
            for kind, key, detail in cur.fetchall():
                ex = {"kind": kind, "key": key}
                if detail:
                    ex["detail"] = detail
                examples.append(ex)

    violations = missing + extra + status_mismatch
    extra_meta = {
        "source_table": p["source_table"],
        "source_count": source_count,
        "target_count": target_count,
        "missing_in_target": missing,
        "extra_in_target": extra,
    }
    if compare_deleted:
        extra_meta["delete_status_mismatch"] = status_mismatch

    # Optionally enqueue the gap keys (arc rows missing from the flat table, plus
    # arc-deleted rows whose delete never propagated) for DFM ids-mode replay.
    if p.get("capture_replay") and subject.replay is not None and (missing or status_mismatch):
        try:
            from dqa.remediation.capture import capture_source_reconciliation_gaps

            capture_source_reconciliation_gaps(
                context,
                subject,
                resources,
                source_table=p["source_table"],
                source_key=sk,
                target_key=tk,
                source_deleted=p.get("source_deleted"),
                target_deleted=p.get("target_deleted"),
                source_filter=p.get("source_filter"),
                target_filter=p.get("target_filter"),
            )
        except Exception as exc:  # never fail the check on a capture problem
            context.log.warning(f"DQA source_reconciliation capture failed (non-fatal): {exc}")

    return CheckOutcome(
        passed=violations == 0,
        checked=source_count,
        violations=violations,
        examples=examples,
        extra=extra_meta,
    )
