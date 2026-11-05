# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""patient_arc_integrity check: fhir.Patient's chosen IFDB patient_id is correct.

Patient is deduped by CPR in the ETL: per CPR exactly one arc.ifdb_patient row is
POSTed -- non-deleted preferred, then latest OPDATERET_TID (see
``patient_ifdb_contribution._dedup_rows_by_cpr``). So a plain ``archive_fhir_reconciliation`` on
PATIENT_ID false-flags the *dropped* duplicates: a deleted PATIENT_ID that was
intentionally not POSTed looks like ``missing_in_target``. Person-level presence
("every CPR reached fhir") is the separate CPR ``archive_fhir_reconciliation``; this
check is dedup-aware and only about the per-``patient_id`` *choice*, fhir -> arc:

- ``orphan_ifdb_id``   : fhir.ifdb_patient_id not in arc.PATIENT_ID -> stale/wrong id (BAD).
- ``kept_deleted_dup`` : an ACTIVE fhir row points at a DELETED arc row while that
                         CPR also has a non-deleted arc row -> fhir shows a deleted
                         person as live, keeping the wrong duplicate (BAD). A
                         *soft-deleted* fhir row pointing at a deleted arc row is
                         correct propagation and is NOT flagged.
- ``delete_mismatch``  : for the kept id, fhir.dw_is_deleted disagrees with arc.SLETTET
                         -> a delete that didn't propagate (BAD).

params:
    source_table:    str  -- arc archive, e.g. "arc.ifdb_patient"
    source_key:      str  -- arc patient id column (PATIENT_ID)
    source_cpr:      str  -- arc CPR column (CPR)
    source_deleted:  str  -- arc soft-delete column (SLETTET, '1' = deleted)
    target_key:      str  -- flat-table IFDB id column (ifdb_patient_id)
    target_deleted:  str  -- flat-table soft-delete column (dw_is_deleted)
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


def _del(col: str, alias: str) -> str:
    """1 when the (string-or-int) soft-delete flag equals 1/'1', else 0 (NULL -> 0)."""
    return f"CASE WHEN CAST({alias}.[{col}] AS NVARCHAR(10)) = '1' THEN 1 ELSE 0 END"


@register_check("patient_arc_integrity")
def patient_arc_integrity(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    a_schema, a_table = split_table(p["source_table"])
    f_schema, f_table = split_table(subject.table)
    arc = f"[{a_schema}].[{a_table}]"
    fhir = f"[{f_schema}].[{f_table}]"
    pid, cpr, sdel_col = p["source_key"], p["source_cpr"], p["source_deleted"]
    tkey, tdel_col = p["target_key"], p["target_deleted"]

    sdel, tdel = _del(sdel_col, "a"), _del(tdel_col, "p")
    active_sibling = (
        f"EXISTS (SELECT 1 FROM {arc} a2 WHERE a2.[{cpr}] = a.[{cpr}] "
        f"AND (CAST(a2.[{sdel_col}] AS NVARCHAR(10)) <> '1' OR a2.[{sdel_col}] IS NULL))"
    )
    # fhir rows whose ifdb id resolves to an arc row (the JOIN population)
    joined = f"{fhir} p JOIN {arc} a ON a.[{pid}] = p.[{tkey}]"
    orphan_pred = (
        f"p.[{tkey}] IS NOT NULL AND NOT EXISTS "
        f"(SELECT 1 FROM {arc} a WHERE a.[{pid}] = p.[{tkey}])"
    )
    # Mutually exclusive: kept_deleted_dup is the specific "kept a deleted arc row
    # while an active sibling exists" case; delete_mismatch is the *remaining* flag
    # disagreements (so a row is counted in exactly one category, not both).
    #
    # The fhir row must still be ACTIVE (tdel <> 1): fhir.Patient can retain BOTH a
    # live row (for the live PATIENT_ID) and a soft-deleted row (for the deleted
    # PATIENT_ID) of the same CPR. A soft-deleted fhir row pointing at a deleted arc
    # row mirrors arc correctly -- it is NOT a kept-deleted-dup (same idea as the
    # MIN-aggregation fix in archive_fhir_reconciliation). Only a still-active fhir row
    # showing a deleted person as live, while the CPR has a live arc sibling, is BAD.
    kept_pred = f"{sdel} = 1 AND {tdel} <> 1 AND {active_sibling}"
    mismatch_pred = f"({sdel} <> {tdel}) AND NOT ({kept_pred})"

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {fhir} p WHERE p.[{tkey}] IS NOT NULL")
        checked = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"SELECT "
            f"(SELECT COUNT(*) FROM {fhir} p WHERE {orphan_pred}), "
            f"(SELECT COUNT(*) FROM {joined} WHERE {kept_pred}), "
            f"(SELECT COUNT(*) FROM {joined} WHERE {mismatch_pred})"
        )
        orphan, kept_deleted_dup, delete_mismatch = (int(v or 0) for v in cur.fetchone())

        examples = []
        if orphan or kept_deleted_dup or delete_mismatch:
            cur.execute(
                f"SELECT TOP 30 kind, k FROM ("
                f"SELECT 'orphan_ifdb_id' AS kind, p.[{tkey}] AS k FROM {fhir} p WHERE {orphan_pred} "
                f"UNION ALL "
                f"SELECT 'kept_deleted_dup', p.[{tkey}] FROM {joined} WHERE {kept_pred} "
                f"UNION ALL "
                f"SELECT 'delete_mismatch', p.[{tkey}] FROM {joined} WHERE {mismatch_pred}"
                f") d"
            )
            examples = [{"kind": kind, "key": k} for kind, k in cur.fetchall()]

    # Route kept_deleted_dup to the patient replay: re-running the transform
    # re-dedups the CPR group (picks the active sibling) and re-points fhir. Best-
    # effort; no-op unless the subject has a ReplaySpec (fhir_patient does).
    if kept_deleted_dup and subject.replay is not None:
        try:
            from dqa.remediation.capture import capture_kept_deleted_dups

            capture_kept_deleted_dups(
                context,
                subject,
                resources,
                joined=joined,
                kept_pred=kept_pred,
                target=p["source_table"],
            )
        except Exception as exc:  # never let capture fail the check
            context.log.warning(f"DQA kept_deleted_dup capture failed (non-fatal): {exc}")

    violations = orphan + kept_deleted_dup + delete_mismatch
    return CheckOutcome(
        passed=violations == 0,
        checked=checked,
        violations=violations,
        examples=examples,
        extra={
            "source_table": p["source_table"],
            "compares": f"{fhir}.{tkey} -> {arc}.{pid} (CPR-deduped via {cpr}/{sdel_col})",
            "orphan_ifdb_id": orphan,
            "kept_deleted_dup": kept_deleted_dup,
            "delete_mismatch": delete_mismatch,
        },
    )
