# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""fhir_server_reconciliation: the flat ``fhir.*`` table matches the FHIR server.

The daily SoF extraction is delta-by-modification-timestamp, so it can ADD/UPDATE
resources but never sees a *deletion* -- a resource hard-deleted in Aidbox lingers
in the flat table. That's how ``fhir.Encounter`` ends up with duplicate
``identifier_value``: an old resource version was deleted on the server, a new one
posted, but the flat table still carries the (now non-existent) old ``encounter_id``.

This check stages the server's live resource ids (see ``fhir_server.py``) and
reports:

- ``stale_in_flat``    : active flat rows whose resource id is GONE from the server
                         -> deleted in Aidbox, not propagated (VIOLATION; fix by
                         soft-deleting -- see dqa_fhir_server_cleanup).
- ``missing_in_flat``  : server ids with no flat row at all -> projection lag
                         (INFO; the next delta SoF run should pick them up).

params:
    resource_table:  str        -- Aidbox table (lowercase resource type), e.g. "encounter"
    deleted_column:  str | None  -- flat soft-delete column (default "dw_is_deleted");
                                   None/absent for a table without one (every row is
                                   treated as live -> detection only, no cleanup).
The flat resource-id column is ``subject.identity_column`` (e.g. encounter_id).

requires: ("fhir_database",)
"""

from __future__ import annotations

from dqa.checks.fhir_server import not_deleted_pred, stage_fhir_server_ids
from dqa.checks.helpers import get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("fhir_server_reconciliation")
def fhir_server_reconciliation(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    schema, table = split_table(subject.table)
    flat = f"[{schema}].[{table}]"
    idc = subject.identity_column
    deleted_col = p.get("deleted_column", "dw_is_deleted")
    resource_table = p["resource_table"]
    # Sparse projections (a row only for *some* resources, e.g. Encounter_Location
    # exists only for encounters that have a location) should not report
    # "missing_in_flat" -- a server id with no flat row is expected there.
    report_missing = bool(p.get("report_missing_in_flat", True))
    # No soft-delete column (e.g. fhir.Organization/Location) -> every row is live;
    # stale rows are still detected, but there is no cleanup to soft-delete them.
    nd = not_deleted_pred(deleted_col, "f") if deleted_col else "1 = 1"

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        stage, live = stage_fhir_server_ids(conn, cur, resources.fhir_database, resource_table)
        context.log.info(
            f"fhir_server_reconciliation: staged {live} live {resource_table} ids; "
            f"diffing against {flat}.{idc}"
        )

        cur.execute(f"SELECT COUNT(*) FROM {flat} f WHERE {nd}")
        active_in_flat = int(cur.fetchone()[0] or 0)

        # An empty live-id set (wrong/empty Aidbox, wrong resource_table, or a staging
        # failure) would flag EVERY active row as stale -- a false 100% that could
        # trigger a mass cleanup. Treat 0 live ids as an anomaly to investigate, not as
        # "every row was deleted on the server": skip the stale/missing diff, but FAIL
        # the check (at its configured severity) so the anomaly is visible in
        # dqa.Results / dqa_summary instead of reading as green for weeks.
        if live == 0:
            context.log.warning(
                f"fhir_server_reconciliation: server returned 0 live {resource_table} ids; "
                f"skipping stale/missing diff for {flat} and FAILING the check as an "
                f"anomaly (investigate server/connection/resource_table). "
                f"{active_in_flat} active rows left untouched."
            )
            return CheckOutcome(
                passed=False,
                checked=active_in_flat,
                violations=0,
                examples=[],
                extra={
                    "resource_table": resource_table,
                    "live_in_server": 0,
                    "active_in_flat": active_in_flat,
                    "stale_in_flat": -1,
                    "note": (
                        "server returned 0 live ids; diff skipped, check failed as "
                        "anomaly -- investigate server/connection/resource_table"
                    ),
                },
            )

        stale_sql = (
            f"FROM {flat} f WHERE {nd} "
            f"AND NOT EXISTS (SELECT 1 FROM {stage} t WHERE t.id = f.[{idc}])"
        )
        cur.execute(f"SELECT COUNT(*) {stale_sql}")
        stale = int(cur.fetchone()[0] or 0)

        missing_in_flat = -1
        if report_missing:
            cur.execute(
                f"SELECT COUNT(*) FROM {stage} t "
                f"WHERE NOT EXISTS (SELECT 1 FROM {flat} f WHERE f.[{idc}] = t.id)"
            )
            missing_in_flat = int(cur.fetchone()[0] or 0)

        examples = []
        if stale:
            cur.execute(f"SELECT TOP 15 f.[{idc}] {stale_sql}")
            examples = [{"kind": "stale_in_flat", "key": r[0]} for r in cur.fetchall()]

        # Best-effort drop; the global temp also auto-drops when conn closes.
        try:
            cur.execute(f"IF OBJECT_ID('tempdb..{stage}') IS NOT NULL DROP TABLE {stage}")
        except Exception:  # noqa: BLE001
            pass

    meta = {
        "resource_table": resource_table,
        "live_in_server": live,
        "active_in_flat": active_in_flat,
        "stale_in_flat": stale,
    }
    if report_missing:
        meta["missing_in_flat"] = missing_in_flat
    return CheckOutcome(
        passed=stale == 0,
        checked=active_in_flat,
        violations=stale,
        examples=examples,
        extra=meta,
    )
