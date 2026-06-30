# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Budget (fact-level)

Assign DW_EK to Budget via IDENTITY at fact grain (ref_LedgerbudgetId).
Natural key: CleanCode_Budget (= BUDGET_{ref_LedgerbudgetId})
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_budget_sydax"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign DW_EK to Budget via IDENTITY "
        "using stg_fct_budget_sydax as source (fact-level grain)"
    ),
)
def map_budget(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: insert new Budget CleanCodes into map.Budget."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Budget (fact-level) map update...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_budget_sydax]")
            source_count = cursor.fetchone()[0]
            context.log.info(f"Staging source: {source_count} unique CleanCode_Budget candidates")

            cursor.execute(
                """
            INSERT INTO [map].[Budget] (
                CleanCode_Budget,
                bk_ref_LedgerbudgetId,
                DW_Timestamp_Insert
            )
            SELECT
                f.CleanCode_Budget,
                f.bk_ref_LedgerbudgetId,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[fct_budget_sydax] f
            WHERE f.CleanCode_Budget IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM [map].[Budget] m
                  WHERE m.CleanCode_Budget = f.CleanCode_Budget
              );
        """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM map.Budget")
            total_count = cursor.fetchone()[0]

            context.log.info(
                f"Map update complete: {new_count} new, {total_count} total Budget entries"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Budget",
            "natural_key": "CleanCode_Budget",
            "source_table": "stg.fct_budget_sydax",
        }
    )
