# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Budget (fact-level)

Assign DW_EK to Budget via IDENTITY at fact grain (ref_ledgerbudget_id).
Natural key: clean_code_Budget (= BUDGET_{ref_ledgerbudget_id})
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_Budget_sydax"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign DW_EK to Budget via IDENTITY "
        "using stg_fct_Budget_sydax as source (fact-level grain)"
    ),
)
def map_Budget(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: insert new Budget CleanCodes into map.Budget."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Budget (fact-level) map update...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_Budget_sydax]")
            source_count = cursor.fetchone()[0]
            context.log.info(f"Staging source: {source_count} unique clean_code_Budget candidates")

            cursor.execute(
                """
            INSERT INTO [map].[Budget] (
                clean_code_Budget,
                bk_ref_ledgerbudget_id,
                dw_timestamp_insert
            )
            SELECT
                f.clean_code_Budget,
                f.bk_ref_ledgerbudget_id,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[fct_Budget_sydax] f
            WHERE f.clean_code_Budget IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM [map].[Budget] m
                  WHERE m.clean_code_Budget = f.clean_code_Budget
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
            "natural_key": "clean_code_Budget",
            "source_table": "stg.fct_Budget_sydax",
        }
    )
