# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Postering

Assign dw_ek_Postering via IDENTITY using clean_code_Postering as natural key.
Logic: Insert new postering from SydAX dimension staging table.

clean_code format: "{Kildesystem}_{ref_ledgertrans_id}"
Pattern follows map_Kontoplan: INSERT-only, IDENTITY assigns DW_EK, incremental.

Uses staging table:
- stg.dim_Postering_sydax: Deduplicated clean_code_Postering values
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_Postering_sydax"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_Postering via IDENTITY using clean_code_Postering as natural key",
)
def map_Postering(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Postering into map.Postering."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Postering map update using staging table...")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_Postering_sydax]
            """
            )
            unique_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {unique_count} unique clean_code_Postering")

            cursor.execute(
                """
            INSERT INTO [map].[Postering] (
                clean_code_Postering,
                dw_timestamp_insert,
                dw_timestamp_update
            )
            SELECT
                src.clean_code_Postering,
                CAST(SYSDATETIME() AS datetime2(0)),
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_Postering_sydax] src
            WHERE NOT EXISTS (
                  SELECT 1
                  FROM [map].[Postering] m
                  WHERE m.clean_code_Postering = src.clean_code_Postering
              );
            """
            )

            new_count = cursor.rowcount
            context.log.info(f"Inserted {new_count} new Postering entries")

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Postering]")
            total_count = cursor.fetchone()[0]

            if new_count > 0:
                cursor.execute(
                    """
                SELECT TOP 5 clean_code_Postering, dw_ek_Postering
                FROM [map].[Postering]
                ORDER BY dw_timestamp_insert DESC
                """
                )
                recent_entries = cursor.fetchall()
                context.log.info(f"Sample recent entries: {[(r[0], r[1]) for r in recent_entries]}")

            context.log.info(
                f"Map update complete: {new_count} new, {total_count} total Postering entries"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Postering",
            "natural_key": "clean_code_Postering (Kildesystem + ref_ledgertrans_id)",
            "source_table": "stg.dim_Postering_sydax",
            "cleancode_format": "{Kildesystem}_{ref_ledgertrans_id}",
        }
    )
