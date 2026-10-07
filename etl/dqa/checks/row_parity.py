# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""row_parity check: the subject table and another table hold exactly as many rows.

For full-rebuild layers the row count IS the completeness contract: dm.HAIBA_*
is TRUNCATE + INSERT from arc with dw_is_deleted carried (never filtered), and
fact and egenskaber share the dw_ek 1:1 — so any count drift means the mint
missed a clean_code, the map join dropped rows, or one side of the pair was
rebuilt without the other. The ETL guards this in-run; this check guards it
at rest, where a partial manual TRUNCATE or a failed half of a rebuild would
otherwise sit invisible until the next run.

Deliberately ignores ``subject.deleted_column``: both sides carry their
soft-deleted rows, and filtering either side would hide exactly the drift the
check exists to catch.

params:
    other_table:   str        -- schema-qualified table to compare against (required)
    where_subject: str | None -- optional row filter on the subject table
    where_other:   str | None -- optional row filter on the other table
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn, split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("row_parity")
def row_parity(context, subject, spec, resources) -> CheckOutcome:
    o_schema, o_table = split_table(spec.params["other_table"])
    s_schema, s_table = split_table(subject.table)
    where_subject = spec.params.get("where_subject")
    where_other = spec.params.get("where_other")

    s_where = f" WHERE ({where_subject})" if where_subject else ""
    o_where = f" WHERE ({where_other})" if where_other else ""

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM [{s_schema}].[{s_table}]{s_where}")
            subject_rows = int(cur.fetchone()[0] or 0)
            cur.execute(f"SELECT COUNT(*) FROM [{o_schema}].[{o_table}]{o_where}")
            other_rows = int(cur.fetchone()[0] or 0)
        finally:
            cur.close()

    diff = abs(subject_rows - other_rows)
    return CheckOutcome(
        passed=diff == 0,
        checked=subject_rows,
        violations=diff,
        extra={
            "subject_rows": subject_rows,
            "other_table": spec.params["other_table"],
            "other_rows": other_rows,
        },
    )
