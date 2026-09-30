# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""uniqueness check: a grain key has no duplicates.

This is the first line of defence against the duplicate-resource problem
(duplicate FHIR Organization/Encounter on the server).

With a soft-delete column in play, uniqueness is enforced among **non-deleted**
rows only -- the right semantics for a business key, where a soft-deleted duplicate
is *expected* (e.g. the leftover an old resource version becomes after
``dqa_fhir_server_cleanup``). A strict check would keep failing forever once a
duplicate has been correctly soft-deleted.

The column defaults to ``subject.deleted_column``; pass an explicit
``deleted_column=None`` in ``params`` to opt OUT and enforce strict, all-rows
uniqueness -- the right semantics for PK / resource-id grains, where even a
soft-deleted duplicate id would indicate a projection bug.

params:
    grain:          str            -- column, or comma-separated columns
    ignore_values:  list[str]      -- grain values to skip (default: [''])
    deleted_column: str | None     -- soft-delete column scoping the check to active
                                      rows (flag NULL or != 1). Absent -> falls back
                                      to subject.deleted_column; explicit None ->
                                      strict uniqueness over ALL rows.
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn, split_table, sql_literal
from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("uniqueness")
def uniqueness(context, subject, spec, resources) -> CheckOutcome:
    grain = spec.params["grain"]
    ignore = spec.params.get("ignore_values", [""])
    # An explicit params value wins -- INCLUDING an explicit None (strict, all-rows
    # uniqueness for PK / resource-id grains). Only an absent key falls back to the
    # subject's soft-delete column.
    deleted_column = (
        spec.params["deleted_column"] if "deleted_column" in spec.params else subject.deleted_column
    )
    schema, table = split_table(subject.table)

    cols = [c.strip() for c in grain.split(",")]

    # ignore_values are applied to the FIRST grain column only (the common single-
    # column-with-sentinel case). Combining an explicit ignore list with a COMPOSITE
    # grain would silently skip only rows whose first column matches -- almost never
    # what's intended -- so refuse it loudly rather than mis-scope the check.
    if len(cols) > 1 and "ignore_values" in spec.params:
        raise ValueError(
            f"uniqueness on {subject.table}: ignore_values is only supported for a "
            f"single-column grain; got composite grain {cols!r} with ignore_values "
            f"{ignore!r}. Drop ignore_values, or split into single-column checks."
        )

    grain_sel = ", ".join(f"[{c}]" for c in cols)

    preds = [f"[{c}] IS NOT NULL" for c in cols]
    # Apply ignore-value filtering to the first grain column (the common case is
    # a single-column grain with a '' / '-1' sentinel).
    preds += [f"[{cols[0]}] <> {sql_literal(v)}" for v in ignore]
    if deleted_column:
        preds.append(f"([{deleted_column}] IS NULL OR [{deleted_column}] <> 1)")
    where = " WHERE " + " AND ".join(preds)

    dup_groups = (
        f"SELECT {grain_sel} FROM [{schema}].[{table}]{where} "
        f"GROUP BY {grain_sel} HAVING COUNT(*) > 1"
    )

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM ({dup_groups}) d")
        violations = int(cur.fetchone()[0] or 0)

        # Rows considered = rows eligible for the duplicate scan (same predicate:
        # non-NULL grain, sentinels excluded, active-only when scoped).
        cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]{where}")
        checked = int(cur.fetchone()[0] or 0)

        examples = []
        if violations:
            cur.execute(
                f"SELECT TOP 20 {grain_sel}, COUNT(*) AS n "
                f"FROM [{schema}].[{table}]{where} "
                f"GROUP BY {grain_sel} HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC"
            )
            examples = [
                {"key": list(r[:-1]) if len(cols) > 1 else r[0], "count": int(r[-1])}
                for r in cur.fetchall()
            ]

    return CheckOutcome(
        passed=violations == 0,
        checked=checked,
        violations=violations,
        examples=examples,
        extra={"grain": grain, "active_only": bool(deleted_column)},
    )
