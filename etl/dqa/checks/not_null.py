# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""not_null check: required columns must not be NULL.

Scoped to active rows when the subject declares a ``deleted_column`` -- a NULL in
a soft-deleted row shouldn't fail (the row is logically gone).

``violations`` counts offending ROWS (a row with >= 1 required NULL), consistent
with the other checks; the per-column null-cell counts are reported in ``extra``
(``null_counts`` / ``null_cells``).

params:
    columns: list[str]            -- columns that must be non-NULL (required)
    where:   str | None           -- optional row filter
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("not_null")
def not_null(context, subject, spec, resources) -> CheckOutcome:
    columns = spec.params["columns"]
    where = spec.params.get("where")
    schema, table = split_table(subject.table)

    preds = []
    if where:
        preds.append(f"({where})")
    if subject.deleted_column:
        dc = subject.deleted_column
        preds.append(f"([{dc}] IS NULL OR [{dc}] <> 1)")
    where_clause = (" WHERE " + " AND ".join(preds)) if preds else ""

    # violations = offending ROWS (>= 1 required column NULL), so the count means the
    # same thing as every other check (rows/keys), not summed null cells. The
    # per-column null counts are kept in extra for diagnosis.
    row_null_pred = " OR ".join(f"[{c}] IS NULL" for c in columns)
    per_col = ", ".join(f"SUM(CASE WHEN [{c}] IS NULL THEN 1 ELSE 0 END)" for c in columns)
    sql = (
        f"SELECT COUNT(*), SUM(CASE WHEN {row_null_pred} THEN 1 ELSE 0 END), "
        f"{per_col} FROM [{schema}].[{table}]{where_clause}"
    )

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()

    checked = int(row[0] or 0)
    violations = int(row[1] or 0)  # rows with at least one required NULL
    null_counts = {c: int(row[i + 2] or 0) for i, c in enumerate(columns)}
    failing = {c: n for c, n in null_counts.items() if n > 0}

    return CheckOutcome(
        passed=violations == 0,
        checked=checked,
        violations=violations,
        examples=sorted(failing),
        extra={
            "null_counts": failing,
            "null_cells": sum(failing.values()),  # total NULL cells (old metric, kept)
            "columns": columns,
        },
    )
