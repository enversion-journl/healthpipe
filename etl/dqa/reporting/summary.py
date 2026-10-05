# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
dqa_summary -- a rollup of the current DQA state, read from ``dqa.Results``.

Materialize it (after the DQA jobs) to get an at-a-glance overview in the Dagster
asset metadata: how many checks are failing, broken down by severity and track,
plus the worst offenders. It reads the newest row per check (per environment)
directly from ``dqa.Results`` -- no dependency on the ``dqa.v_latest`` view -- and
degrades gracefully when the table is absent or empty (no DQA runs yet).
"""

from __future__ import annotations

from collections import Counter, defaultdict

import dagster as dg
from core.resources import MSSQLResource

_LATEST_SQL = """
WITH ranked AS (
    SELECT environment, track, subject, check_type, name_suffix, severity,
           cadence, passed, violation_count,
           ROW_NUMBER() OVER (
               PARTITION BY environment, subject, check_type, ISNULL(name_suffix, '')
               ORDER BY checked_at DESC, result_id DESC
           ) AS rn
    FROM dqa.Results
)
SELECT environment, track, subject, check_type, name_suffix, severity,
       cadence, passed, violation_count
FROM ranked
WHERE rn = 1
"""


@dg.asset(
    name="dqa_summary",
    group_name="data_quality",
    compute_kind="mssql",
    description=(
        "Rollup of the latest DQA Results per environment (reads dqa.Results, "
        "which every check writes on each run)."
    ),
)
def dqa_summary(
    context,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    try:
        with mssql_target_staging.get_connection() as conn:
            cur = conn.cursor()
            if cur.execute("SELECT OBJECT_ID(N'[dqa].[Results]', N'U')").fetchone()[0] is None:
                context.log.warning(
                    "dqa.Results does not exist yet -- materialize staging_infra_dqa "
                    "and run the DQA jobs first."
                )
                return dg.MaterializeResult(
                    metadata={"status": dg.MetadataValue.text("no dqa.Results table yet")}
                )
            cur.execute(_LATEST_SQL)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
    except Exception as exc:  # noqa: BLE001 -- overview must never hard-fail
        context.log.warning(f"dqa_summary could not read dqa.Results: {exc}")
        return dg.MaterializeResult(
            metadata={"status": dg.MetadataValue.text(f"unavailable: {exc}")}
        )

    total = len(rows)
    failing = [r for r in rows if not r["passed"]]
    by_sev = Counter(r["severity"] for r in failing)
    by_track = Counter(r["track"] for r in failing)
    subj_fail: dict[str, int] = defaultdict(int)
    for r in failing:
        subj_fail[r["subject"]] += 1
    top_subjects = sorted(subj_fail.items(), key=lambda kv: kv[1], reverse=True)[:10]

    report = _render_markdown(rows, failing, by_sev, by_track, top_subjects)

    return dg.MaterializeResult(
        metadata={
            "environments": dg.MetadataValue.json(sorted({r["environment"] for r in rows})),
            "checks_total": dg.MetadataValue.int(total),
            "checks_passed": dg.MetadataValue.int(total - len(failing)),
            "failing_total": dg.MetadataValue.int(len(failing)),
            "failing_blocking": dg.MetadataValue.int(by_sev.get("blocking", 0)),
            "failing_error": dg.MetadataValue.int(by_sev.get("error", 0)),
            "failing_warn": dg.MetadataValue.int(by_sev.get("warn", 0)),
            "failing_by_track": dg.MetadataValue.json(dict(by_track)),
            "top_failing_subjects": dg.MetadataValue.json(dict(top_subjects)),
            "report": dg.MetadataValue.md(report),
        }
    )


def _render_markdown(rows, failing, by_sev, by_track, top_subjects) -> str:
    lines = ["## DQA overview", ""]
    lines.append(
        f"- **{len(rows)}** checks, **{len(rows) - len(failing)}** passing, "
        f"**{len(failing)}** failing"
    )
    lines.append(
        f"- failing by severity: blocking={by_sev.get('blocking', 0)}, "
        f"error={by_sev.get('error', 0)}, warn={by_sev.get('warn', 0)}"
    )
    lines.append(f"- failing by track: {dict(by_track) or '{}'}")
    if top_subjects:
        lines += ["", "### Top failing subjects", ""]
        lines += [f"- {subj}: {n}" for subj, n in top_subjects]
    if failing:
        lines += [
            "",
            "### Failing checks",
            "",
            "| subject | check | severity | violations |",
            "|---|---|---|---|",
        ]
        for r in sorted(failing, key=lambda x: (x["severity"], x["subject"]))[:50]:
            name = "/".join(filter(None, [r["check_type"], r["name_suffix"]]))
            lines.append(f"| {r['subject']} | {name} | {r['severity']} | {r['violation_count']} |")
    return "\n".join(lines)
