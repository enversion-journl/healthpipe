# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared helpers for DQA check implementations.

SQL identifiers (schema/table/column names) come from trusted config
(``DqaSubject`` / lineage) and are bracket-quoted and inlined -- identifiers
cannot be bound as parameters. Scalar values assembled into those statements are
inlined via ``sql_literal`` / ``sql_nliteral`` (single-quote escaped), mirroring
the existing checks in ``DSA/SQL/checks/``. Where a value is data-driven and a
plain bind fits (the DSO delete-column probe, the ``executemany`` staging
fallbacks), qmark (``?``) parameters are used instead -- every DQA resource
connects through mssql-python, which supports them.
"""

from __future__ import annotations

import os
from typing import Any

from dqa.model import CheckOutcome

# Width of the [environment] column on dqa.Results / dqa.RemediationQueue.
_ENVIRONMENT_MAX_LEN = 128

# Pin char columns to the *current database's* collation. Two DQA uses, same token:
#
# * ##temp DDL on mssql_target_staging (fhir_server / oracle_source /
#   source_archive_reconciliation): tempdb inherits the instance collation (SQL_Latin1
#   on QA/Prod) while healthpipe_staging is Danish_Norwegian_CI_AS. Without this,
#   joining the staged keys to fhir.* / arc.* -- or MERGEing them into
#   dqa.RemediationQueue -- raises "Cannot resolve the collation conflict".
#   DATABASE_DEFAULT is only correct here because the creating connection's
#   current DB *is* staging; a mssql_source_dso connection would silently
#   resolve to DSO/Latin1 and reintroduce the conflict against Danish fhir.*.
# * dso_reconciliation aliases this as _COLLATE for the STAGING_LINK case
#   (see that module -- different connection, same portable token).
#
# COLLATE is only legal on character types (never INT/datetime). Pin at DDL, not
# on the join predicate, so IX_dqa_fhir_ids / IX_dqa_recon stay sargable.
COLLATE_DB_DEFAULT = " COLLATE DATABASE_DEFAULT"


def dqa_environment() -> str:
    """The environment label persisted with DQA rows.

    ``DQA_ENVIRONMENT`` > ``DAGSTER_BRANCH_POSTFIX`` > ``local``, truncated to the
    NVARCHAR(128) column width so a long feature-branch postfix can't make every
    persist/enqueue INSERT fail (best-effort writes would silently drop all
    history for that branch).
    """
    env = os.environ.get("DQA_ENVIRONMENT") or os.environ.get("DAGSTER_BRANCH_POSTFIX", "local")
    return env[:_ENVIRONMENT_MAX_LEN]


def split_table(qualified: str) -> tuple[str, str]:
    """``"fhir.Encounter"`` -> ``("fhir", "Encounter")``."""
    schema, _, table = qualified.partition(".")
    if not table:
        raise ValueError(f"Expected a schema-qualified table, got {qualified!r}")
    return schema, table


def sql_literal(value: Any) -> str:
    """Quote a value as a SQL string literal (single-quote escaped)."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_nliteral(value: Any) -> str:
    """Quote a value as a Unicode (N-prefixed) SQL string literal.

    Same single-quote escaping as ``sql_literal`` but emits ``N'...'`` so any
    character outside the target database's (var)char code page survives instead of
    being folded to ``?``. Use it for values that may carry non-ASCII text and land
    in an NVARCHAR column (e.g. JSON blobs / free-text notes written to dqa.Results).
    """
    return "N'" + str(value).replace("'", "''") + "'"


def get_conn(resources: Any, resource_key: str):
    """Return the ``get_connection()`` context manager for a resource key."""
    resource = getattr(resources, resource_key)
    return resource.get_connection()


def run_violation_sql(
    resources: Any,
    resource_key: str,
    count_sql: str,
    examples_sql: str | None = None,
    checked_sql: str | None = None,
) -> CheckOutcome:
    """Run a "count violations" check.

    - ``count_sql``    : returns a single row whose first column is the violation count.
    - ``checked_sql``  : optional; first column = rows/keys examined (for context).
    - ``examples_sql`` : optional; first column of each row = an offending key (capped at 20).
    """
    with get_conn(resources, resource_key) as conn:
        cur = conn.cursor()
        cur.execute(count_sql)
        violations = int(cur.fetchone()[0] or 0)

        checked = -1
        if checked_sql:
            cur.execute(checked_sql)
            checked = int(cur.fetchone()[0] or 0)

        examples: list[Any] = []
        if examples_sql and violations:
            cur.execute(examples_sql)
            examples = [row[0] for row in cur.fetchall()[:20]]

    return CheckOutcome(
        passed=violations == 0,
        checked=checked,
        violations=violations,
        examples=examples,
    )
