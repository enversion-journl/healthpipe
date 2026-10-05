# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""sentinel_rate check: how often FK columns fall back to the unknown-member sentinel.

A ``DW_EK_`` key is ``NOT NULL DEFAULT -1``, so a failed dimension lookup lands as
the ``-1`` unknown-member surrogate. The **rate of -1 values** per FK column is the
lookup-coverage signal (referential_integrity ignores ``-1`` as a sentinel, not a
dangling key).

Without ``max_rate`` the check is pure observability (always passes; per-column
rates land in ``dqa.results``/Dagster metadata -- use those to learn the
baselines). With ``max_rate`` set, a column whose rate exceeds its cap is a
violation -- tighten per fact/column once baselines are known.

params:
    columns:  list[str]    -- FK columns to measure (required)
    sentinel: int          -- the unknown-member value (default -1)
    max_rate: float | dict[str, float] | None
                           -- fail when a column's rate exceeds its cap (0..1).
                              float  = one cap for every column;
                              dict   = per-column caps ({column: rate}; columns
                                       absent from the dict stay observability-only);
                              None (default) = observability only.
    where:    str | None   -- optional row filter
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("sentinel_rate")
def sentinel_rate(context, subject, spec, resources) -> CheckOutcome:
    columns = spec.params["columns"]
    sentinel = int(spec.params.get("sentinel", -1))
    max_rate = spec.params.get("max_rate")
    where = spec.params.get("where")
    schema, table = split_table(subject.table)

    where_clause = f" WHERE ({where})" if where else ""
    per_col = ", ".join(f"SUM(CASE WHEN [{c}] = {sentinel} THEN 1 ELSE 0 END)" for c in columns)
    sql = f"SELECT COUNT(*), {per_col} FROM [{schema}].[{table}]{where_clause}"

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()

    checked = int(row[0] or 0)
    counts = {c: int(row[i + 1] or 0) for i, c in enumerate(columns)}
    rates = {c: (n / checked if checked else 0.0) for c, n in counts.items()}

    def _cap(col: str) -> float | None:
        """This column's cap: scalar applies to all, dict is per-column, None = uncapped."""
        if max_rate is None:
            return None
        if isinstance(max_rate, dict):
            cap = max_rate.get(col)
            return float(cap) if cap is not None else None
        return float(max_rate)

    exceeded = {c: r for c, r in rates.items() if _cap(c) is not None and r > _cap(c)}
    worst = sorted(rates.items(), key=lambda kv: kv[1], reverse=True)
    examples = [
        {"column": c, "rate": round(r, 4), "sentinel_rows": counts[c]} for c, r in worst if r > 0
    ][:10]

    return CheckOutcome(
        passed=len(exceeded) == 0,
        checked=checked,
        violations=len(exceeded),
        examples=examples,
        extra={
            "sentinel": sentinel,
            "max_rate": max_rate,
            "rates": {c: round(r, 4) for c, r in rates.items()},
            "columns_exceeding": sorted(exceeded),
        },
    )
