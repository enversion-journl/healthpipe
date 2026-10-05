# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""completeness check: monthly row-count parity across Source -> DSA -> DSO.

The DWH analogue of "did everything load through the layers?" -- for a fact, it
groups row counts by month in the source system, in the DSA fact, and in the DSO
fact, and flags months where the three disagree (data loss in staging, or a
sync gap to DSO). Mirrors the logic of DSA/SQL/checks/partition_completeness_checks.

Needs three resources: the subject's resource (DSA), the source system, and DSO.
The source resource + ``mssql_target_dso`` must be declared on the CheckSpecification via
``requires``.

params:
    source_resource: str   -- resource key, e.g. "mysupply_mssql"
    source_query:    str   -- returns (month_key as YYYYMM int, count) from the source
    date_column:     str   -- the fact's date surrogate, e.g. "dw_ek_Dato_faktura"
"""

from __future__ import annotations

from typing import Any

from dqa.checks.helpers import split_table
from dqa.model import CheckOutcome
from dqa.registry import register_check


def _monthly_counts(conn, query: str) -> dict[int, int]:
    cur = conn.cursor()
    cur.execute(query)
    return {int(r[0]): int(r[1]) for r in cur.fetchall() if r[0] is not None}


def _unknown_date_rows(conn, schema: str, table: str, date_column: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE [{date_column}] = -1")
    return int(cur.fetchone()[0] or 0)


@register_check("completeness")
def completeness(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    schema, table = split_table(subject.table)
    date_column = p["date_column"]
    source_res = getattr(resources, p["source_resource"])
    dsa_res = getattr(resources, subject.resource_key)
    dso_res = resources.mssql_target_dso

    # YYYYMMDD surrogate -> YYYYMM via integer division by 100.
    dw_query = (
        f"SELECT [{date_column}] / 100 AS month_key, COUNT(*) AS cnt "
        f"FROM [{schema}].[{table}] WHERE [{date_column}] > 0 "
        f"GROUP BY [{date_column}] / 100"
    )

    with source_res.get_connection() as conn:
        source_counts = _monthly_counts(conn, p["source_query"])
    with dsa_res.get_connection() as conn:
        dsa_counts = _monthly_counts(conn, dw_query)
        dsa_unknown = _unknown_date_rows(conn, schema, table, date_column)
    with dso_res.get_connection() as conn:
        dso_counts = _monthly_counts(conn, dw_query)
        dso_unknown = _unknown_date_rows(conn, schema, table, date_column)

    months = sorted(set(source_counts) | set(dsa_counts) | set(dso_counts))
    failed: list[dict[str, Any]] = []
    for month in months:
        src = source_counts.get(month, 0)
        dsa = dsa_counts.get(month, 0)
        dso = dso_counts.get(month, 0)
        if src == dsa == dso:
            continue
        entry: dict[str, Any] = {"month": month}
        if src != dsa:
            entry.update(source=src, dsa=dsa, source_dsa_diff=src - dsa)
        if dsa != dso:
            entry.update(dsa=dsa, dso=dso, dsa_dso_diff=dsa - dso)
        if month not in source_counts:
            entry["note"] = "missing_from_source"
        elif month not in dsa_counts:
            entry["note"] = "missing_from_dsa"
        elif month not in dso_counts:
            entry["note"] = "missing_from_dso"
        failed.append(entry)

    return CheckOutcome(
        passed=len(failed) == 0,
        checked=len(months),
        violations=len(failed),
        examples=failed[:20],
        extra={
            "months_checked": len(months),
            "source_total": sum(source_counts.values()),
            "dsa_total": sum(dsa_counts.values()),
            "dso_total": sum(dso_counts.values()),
            "dsa_unknown_date_rows": dsa_unknown,
            "dso_unknown_date_rows": dso_unknown,
        },
    )
