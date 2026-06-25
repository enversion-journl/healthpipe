# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: BudgetEgenskaber

Assign DW_EK to BudgetEgenskaber via IDENTITY.
Natural key: CleanCode_BudgetEgenskaber (= BUDGETEGENSKABER_{ref_BudgetmodelId})
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_budget_egenskaber_sydax"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign DW_EK to BudgetEgenskaber via IDENTITY "
        "using stg_dim_budget_egenskaber_sydax as source"
    ),
)
def map_budget_egenskaber(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: insert new BudgetEgenskaber CleanCodes into map.BudgetEgenskaber."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting BudgetEgenskaber map update...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_BudgetEgenskaber_sydax]")
            source_count = cursor.fetchone()[0]
            context.log.info(
                f"Staging source: {source_count} unique CleanCode_BudgetEgenskaber candidates"
            )

            cursor.execute(
                """
            INSERT INTO [map].[BudgetEgenskaber] (
                CleanCode_BudgetEgenskaber,
                bk_ref_BudgetmodelId,
                DW_Timestamp_Insert
            )
            SELECT
                d.CleanCode_BudgetEgenskaber,
                d.bk_ref_BudgetmodelId,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_BudgetEgenskaber_sydax] d
            WHERE NOT EXISTS (
                  SELECT 1
                  FROM [map].[BudgetEgenskaber] m
                  WHERE m.CleanCode_BudgetEgenskaber = d.CleanCode_BudgetEgenskaber
              );
        """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM map.BudgetEgenskaber")
            total_count = cursor.fetchone()[0]

            context.log.info(
                f"Map update complete: {new_count} new, "
                f"{total_count} total BudgetEgenskaber entries"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.BudgetEgenskaber",
            "natural_key": "CleanCode_BudgetEgenskaber",
            "source_table": "stg.dim_BudgetEgenskaber_sydax",
        }
    )
