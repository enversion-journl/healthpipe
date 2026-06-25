# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Aftale

Assign DW_EK_Aftale via IDENTITY using CleanCode_Aftale as natural key.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "stg_dim_aftale_ils"])],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_Aftale via IDENTITY using CleanCode_Aftale as natural key",
)
def map_aftale(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new CleanCode_Aftale values into map.Aftale."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Aftale map update from stg.dim_aftale_ils...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_aftale_ils]")
            stg_count = cursor.fetchone()[0]
            context.log.info(f"stg.dim_aftale_ils has {stg_count} rows")

            cursor.execute(
                """
            INSERT INTO [map].[Aftale] (
                CleanCode_Aftale,
                DW_Timestamp_Insert
            )
            SELECT DISTINCT
                s.CleanCode_Aftale,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_aftale_ils] s
            WHERE s.CleanCode_Aftale IS NOT NULL
              AND s.CleanCode_Aftale <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM [map].[Aftale] m
                  WHERE m.CleanCode_Aftale = s.CleanCode_Aftale
              );
        """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Aftale]")
            total_count = cursor.fetchone()[0]

            context.log.info(
                f"Map update complete: {new_count} new, {total_count} total Aftale entries"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Aftale",
            "natural_key": "CleanCode_Aftale",
            "source_table": "stg.dim_aftale_ils",
        }
    )
