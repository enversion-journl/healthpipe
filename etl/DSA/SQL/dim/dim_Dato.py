# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Daily refresh of relative fields in dim.Dato on staging and DSO."""

import re
from pathlib import Path

import dagster as dg
from core.resources import MSSQLResource

RELATIVE_SQL_FILE = (
    Path(__file__).resolve().parents[3]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "shared_dimensions"
    / "05_dato_relativ.sql"
)


def _split_go_batches(sql: str) -> list[str]:
    """Split a SQL script into executable batches on standalone GO lines."""
    return [batch.strip() for batch in re.split(r"(?im)^\s*GO\s*$", sql) if batch.strip()]


def _run_relative_refresh(
    resource: MSSQLResource,
    row_count_query: str,
) -> int:
    """Execute relative-dato SQL and return calendar row count."""
    sql_text = RELATIVE_SQL_FILE.read_text(encoding="utf-8")
    batches = _split_go_batches(sql_text)
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for batch in batches:
                cursor.execute(batch)
            cursor.execute(row_count_query)
            row_count = int(cursor.fetchone()[0])
            conn.commit()
            return row_count
        finally:
            cursor.close()


@dg.asset(
    name="dim_Dato",
    deps=["staging_infra_shared_dimensions", "dso_infra_shared_dimensions"],
    group_name="shared_dimensions",
    compute_kind="mssql",
    tags={"domain": "shared_dimensions", "layer": "dimension", "type": "calendar_refresh"},
    description="Refresh relative and afsluttet fields in dim.Dato on staging and DSO",
)
def dim_Dato(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Run shared relative-date refresh SQL on both warehouses."""
    row_count_query = (
        "SELECT COUNT(*) FROM [dim].[Dato] WHERE [dw_ek_Dato] BETWEEN 19700101 AND 20501231"
    )
    staging_rows = _run_relative_refresh(mssql_target_staging, row_count_query)
    dso_rows = _run_relative_refresh(mssql_target_dso, row_count_query)
    context.log.info(
        f"dim.Dato relative refresh completed: staging rows={staging_rows}, dso rows={dso_rows}"
    )
    return dg.MaterializeResult(
        metadata={
            "sql_file": str(RELATIVE_SQL_FILE),
            "staging_rows": staging_rows,
            "dso_rows": dso_rows,
        }
    )
