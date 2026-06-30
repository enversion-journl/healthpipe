# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Finanskladde

Assign DW_EK_Finanskladde via IDENTITY using CleanCode_Finanskladde as natural key.
Logic: Insert new finanskladde keys from SydAX dimension staging table.

Uses staging table:
- stg.dim_finanskladde_sydax: Deduplicated CleanCode_Finanskladde values
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="map_finanskladde",
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_finanskladde_sydax"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign DW_EK_Finanskladde via IDENTITY using CleanCode_Finanskladde as natural key"
    ),
)
def map_finanskladde(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    """Map table maintenance: Insert new Finanskladde into map.Finanskladde."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
            INSERT INTO [map].[Finanskladde] (
                CleanCode_Finanskladde,
                DW_Timestamp_Insert,
                DW_Timestamp_Update
            )
            SELECT
                src.CleanCode_Finanskladde,
                CAST(SYSDATETIME() AS datetime2(0)),
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_finanskladde_sydax] src
            WHERE NOT EXISTS (
                  SELECT 1
                  FROM [map].[Finanskladde] m
                  WHERE m.CleanCode_Finanskladde = src.CleanCode_Finanskladde
              );
            """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Finanskladde]")
            total_count = cursor.fetchone()[0]

            context.log.info(
                f"Finanskladde map update complete: inserted {new_count} new rows "
                f"({total_count} total in map.Finanskladde)"
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Finanskladde",
            "natural_key": "CleanCode_Finanskladde",
            "source_table": "stg.dim_finanskladde_sydax",
        }
    )
