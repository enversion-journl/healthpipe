# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""freshness check: a watermark must be recent (and every expected group present).

Reads MAX(watermark_column) from a watermark table -- optionally per group --
and fails when the newest value is older than ``max_age_days``, or when an
expected group has no watermark at all. This is the "did the source stop
delivering" signal that no row-level check can give: a feed that silently
stops leaves every other check green while the data quietly ages.

The age is computed server-side (DATEDIFF against SYSDATETIME()) so the check
follows the database clock, not the pod's.

params:
    watermark_column: str             -- date/datetime column to take MAX of (required)
    max_age_days:     int             -- fail when the newest watermark is older (required)
    group_column:     str | None      -- watermark per group; the OLDEST group counts
    expected_groups:  list[str] | None -- groups that must be present (each missing
                                          group is a violation); requires group_column
    where:            str | None      -- optional row filter
    table:            str | None      -- watermark table override (schema.table); the
                                          watermark can live somewhere other than the
                                          subject's own table (e.g. a delivery register)
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("freshness")
def freshness(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    column = p["watermark_column"]
    max_age_days = int(p["max_age_days"])
    group_column = p.get("group_column")
    expected_groups = p.get("expected_groups")
    where = p.get("where")

    # The watermark may live somewhere other than the subject's own table — a
    # per-type freshness reads the delivery register, not the fact table.
    schema, table = split_table(p.get("table") or subject.table)
    where_clause = f" WHERE ({where})" if where else ""

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        try:
            if group_column:
                cur.execute(
                    f"SELECT [{group_column}], "
                    f"DATEDIFF(day, MAX([{column}]), SYSDATETIME()) "
                    f"FROM [{schema}].[{table}]{where_clause} "
                    f"GROUP BY [{group_column}]"
                )
                # A group whose MAX watermark is NULL has no datable row: skip
                # it rather than crash on int(None). Can't happen while the
                # column is NOT NULL, but the check is advertised as reusable.
                ages = {str(g): int(age) for g, age in cur.fetchall() if age is not None}
            else:
                cur.execute(
                    f"SELECT DATEDIFF(day, MAX([{column}]), SYSDATETIME()) "
                    f"FROM [{schema}].[{table}]{where_clause}"
                )
                row = cur.fetchone()
                age = row[0] if row is not None else None
                # MAX over zero rows is NULL: an empty watermark table means the
                # feed never delivered, which is as stale as it gets.
                ages = {"(all)": int(age)} if age is not None else {}
        finally:
            cur.close()

    stale = {g: a for g, a in ages.items() if a > max_age_days}
    missing = [g for g in (expected_groups or []) if g not in ages]
    examples = [{"group": g, "age_days": a} for g, a in sorted(stale.items())] + [
        {"group": g, "age_days": None} for g in missing
    ]

    return CheckOutcome(
        passed=not stale and not missing and bool(ages),
        checked=len(ages),
        violations=len(stale) + len(missing) + (0 if ages else 1),
        examples=examples[:20],
        extra={
            "max_age_days": max_age_days,
            "age_days": ages,
            "missing_groups": missing,
        },
    )
