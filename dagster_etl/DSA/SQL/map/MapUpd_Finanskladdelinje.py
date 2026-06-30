# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Finanskladdelinje

Assign DW_EK_Finanskladdelinje via IDENTITY using CleanCode_Finanskladdelinje as natural key.
Logic: Insert new finanskladdelinje keys from pre-staging table (distinct CleanCodes).

Uses staging table:
- stg.pre_finanskladdelinje_sydax: Source of distinct CleanCode_Finanskladdelinje values
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="map_finanskladdelinje",
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_finanskladdelinje_sydax"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign DW_EK_Finanskladdelinje via IDENTITY using CleanCode_Finanskladdelinje "
        "as natural key"
    ),
)
def map_finanskladdelinje(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    """Map table maintenance: Insert new Finanskladdelinje into map.Finanskladdelinje."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
            INSERT INTO [map].[Finanskladdelinje] (
                CleanCode_Finanskladdelinje,
                DW_Timestamp_Insert,
                DW_Timestamp_Update
            )
            SELECT DISTINCT
                src.CleanCode_Finanskladdelinje,
                CAST(SYSDATETIME() AS datetime2(0)),
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[pre_finanskladdelinje_sydax] src
            WHERE src.CleanCode_Finanskladdelinje IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM [map].[Finanskladdelinje] m
                  WHERE m.CleanCode_Finanskladdelinje = src.CleanCode_Finanskladdelinje
              );
            """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Finanskladdelinje]")
            total_count = cursor.fetchone()[0]

            context.log.info(
                f"Finanskladdelinje map update complete: inserted {new_count} new rows "
                f"({total_count} total in map.Finanskladdelinje)"
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Finanskladdelinje",
            "natural_key": "CleanCode_Finanskladdelinje",
            "source_table": "stg.pre_finanskladdelinje_sydax",
        }
    )
