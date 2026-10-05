# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""referential_integrity check: every FK value resolves to a parent row.

Catches orphaned references -- e.g. fhir.Encounter.service_provider_id pointing
at an Organization that was deleted. FHIR *_id columns hold Aidbox UUIDs, so this
joins child-UUID -> parent-UUID PK.

params:
    column:        str            -- the FK column on the subject table
    parent_table:  str            -- schema-qualified parent, e.g. "fhir.Organization"
    parent_key:    str            -- parent key column, e.g. "organization_id"
    required:      bool           -- if True, a NULL/sentinel FK is also a violation
    ignore_values: list[str]      -- values meaning "no reference" (default: [''])
"""

from __future__ import annotations

import time
from typing import Any

from dqa.checks.helpers import get_conn, split_table, sql_literal
from dqa.model import CheckOutcome
from dqa.registry import register_check


def violation_predicate(params: dict[str, Any], alias: str) -> str:
    """The FULL violation predicate for one referential spec, qualified to ``alias``.

    Dangling non-null reference, plus -- for ``required`` edges -- the NULL/sentinel
    branch. Shared with the replay recheck (``remediation/replay.py``) so a queued
    key only flips to ``resolved`` when the SAME condition the check flagged has
    cleared: a required FK that is still NULL after replay must NOT resolve.
    """
    fk = params["column"]
    p_schema, p_table = split_table(params["parent_table"])
    parent_key = params["parent_key"]
    ignore = params.get("ignore_values", [""])

    ignore_preds = "".join(f" AND {alias}.[{fk}] <> {sql_literal(v)}" for v in ignore)
    dangling = (
        f"{alias}.[{fk}] IS NOT NULL{ignore_preds} "
        f"AND NOT EXISTS (SELECT 1 FROM [{p_schema}].[{p_table}] tgt "
        f"WHERE tgt.[{parent_key}] = {alias}.[{fk}])"
    )
    if not params.get("required", False):
        return dangling
    missing = f"{alias}.[{fk}] IS NULL" + "".join(
        f" OR {alias}.[{fk}] = {sql_literal(v)}" for v in ignore
    )
    return f"({missing}) OR ({dangling})"


@register_check("referential_integrity")
def referential_integrity(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    fk = p["column"]
    parent_table = p["parent_table"]
    parent_key = p["parent_key"]
    required = bool(p.get("required", False))

    s_schema, s_table = split_table(subject.table)
    idc = subject.identity_column

    where = violation_predicate(p, "src")

    # Only check active (non-soft-deleted) CHILD rows: a soft-deleted row's
    # dangling FK is irrelevant (the row is logically gone, e.g. retired by
    # dqa_fhir_server_cleanup). The parent side stays existence-only -- whether a
    # resolved parent is itself soft-deleted is delete-propagation, not ref. integrity.
    checked_where = ""
    if subject.deleted_column:
        dc = subject.deleted_column
        where = f"({where}) AND (src.[{dc}] IS NULL OR src.[{dc}] <> 1)"
        checked_where = f" WHERE ([{dc}] IS NULL OR [{dc}] <> 1)"

    src = f"[{s_schema}].[{s_table}] src"
    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        # The anti-join is the heavy part (full table scan when the FK column is
        # unindexed); log around it so a long check reads as "working", not "hung".
        context.log.info(
            f"referential_integrity {subject.table}.{fk} -> {parent_table}.{parent_key}: "
            f"running anti-join (can take minutes on large unindexed columns)..."
        )
        t0 = time.perf_counter()
        cur.execute(f"SELECT COUNT(*) FROM {src} WHERE {where}")
        violations = int(cur.fetchone()[0] or 0)
        context.log.info(
            f"referential_integrity {subject.table}.{fk}: {violations} dangling "
            f"in {time.perf_counter() - t0:.1f}s"
        )

        cur.execute(f"SELECT COUNT(*) FROM [{s_schema}].[{s_table}]{checked_where}")
        checked = int(cur.fetchone()[0] or 0)

        examples = []
        if violations:
            cur.execute(
                f"SELECT TOP 20 src.[{idc}] AS id, src.[{fk}] AS fk FROM {src} WHERE {where}"
            )
            examples = [{"id": r[0], "fk": r[1]} for r in cur.fetchall()]

    # Capture the violating business keys for DFM ids-mode replay (no-op unless the
    # subject declares a ReplaySpec). Best-effort -- never fails the check.
    if violations:
        try:
            from dqa.remediation.capture import capture_referential_violations

            capture_referential_violations(
                context,
                subject,
                spec,
                resources,
                where=where,
                reference_column=fk,
                target=f"{parent_table}.{parent_key}",
            )
        except Exception as exc:  # never let capture fail the check
            context.log.warning(f"DQA remediation capture failed (non-fatal): {exc}")

    return CheckOutcome(
        passed=violations == 0,
        checked=checked,
        violations=violations,
        examples=examples,
        extra={
            "column": fk,
            "target": f"{parent_table}.{parent_key}",
            "required": required,
        },
    )
