# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DEV ONLY — Reset all faktura-related tables for a fresh reload cycle.

Clears ref.FakturaToPostering, fct.Faktura/Postering/Fakturalinje,
dim.Faktura (preserving the -1 sentinel), map.Faktura, and staging tables.
No enforced FK constraints exist (sentinel -1 model), so order-free.

Do NOT use in production — use erp_daily_dfm_job instead.

NOTE: truncating map.Faktura re-seeds the dw_ek_Faktura IDENTITY on the next
reload, so any surrogates already synced to the DSO reporting DB become stale.
After running this reset you must also reset/reload the DSO side, otherwise DSO
retains orphaned dw_ek_Faktura values.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, MaterializeResult, asset


@asset(
    group_name="erp_dev",
    compute_kind="mssql",
    tags={"domain": "erp", "type": "dev_only"},
    description=(
        "DEV ONLY — Truncate/delete faktura-related tables for fresh reload. "
        "Preserves dim.Faktura -1 sentinel. Not for production use."
    ),
)
def erp_faktura_reset_tables(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Reset faktura pipeline tables for a full reload."""

    tables_to_truncate = [
        "[fct].[Faktura]",
        "[fct].[Postering]",
        "[fct].[Fakturalinje]",
        "[map].[Faktura]",
    ]
    tables_to_delete = [
        ("[ref].[FakturaToPostering]", None),
        ("[dim].[Faktura]", "dw_ek_Faktura <> -1"),
    ]
    staging_tables = [
        "[stg].[dim_Faktura_mysupply]",
        "[stg].[fct_Faktura_mysupply]",
        "[stg].[fct_Postering_sydax]",
        "[stg].[fct_Fakturalinje_mysupply]",
    ]

    metadata: dict[str, int] = {}

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for table in tables_to_truncate:
                cursor.execute(
                    f"IF OBJECT_ID(N'{table}', 'U') IS NOT NULL "
                    f"BEGIN SELECT COUNT(*) FROM {table} END ELSE SELECT 0"
                )
                before = cursor.fetchone()[0]
                if before > 0:
                    cursor.execute(f"TRUNCATE TABLE {table}")
                context.log.info(f"Truncated {table} ({before} rows)")
                metadata[f"{table}_cleared"] = before

            for table, where in tables_to_delete:
                cursor.execute(
                    f"IF OBJECT_ID(N'{table}', 'U') IS NOT NULL "
                    f"BEGIN SELECT COUNT(*) FROM {table}"
                    + (f" WHERE {where}" if where else "")
                    + " END ELSE SELECT 0"
                )
                before = cursor.fetchone()[0]
                if before > 0:
                    sql = f"DELETE FROM {table}"
                    if where:
                        sql += f" WHERE {where}"
                    cursor.execute(sql)
                context.log.info(f"Deleted from {table} ({before} rows)")
                metadata[f"{table}_cleared"] = before

            for table in staging_tables:
                cursor.execute(
                    f"IF OBJECT_ID(N'{table}', 'U') IS NOT NULL "
                    f"BEGIN SELECT COUNT(*) FROM {table} END ELSE SELECT 0"
                )
                before = cursor.fetchone()[0]
                if before > 0:
                    cursor.execute(f"TRUNCATE TABLE {table}")
                context.log.info(f"Truncated staging {table} ({before} rows)")
                metadata[f"{table}_cleared"] = before

            conn.commit()
        finally:
            cursor.close()

    return MaterializeResult(metadata=metadata)
