# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Kreditor

Assign DW_EK to Kreditor via IDENTITY using CleanCode_Kreditor as natural key.
Logic: Combine kreditor records from SydAX and dimension staging tables (ILS and MySupply).
CleanCode format: "CVR_12345678" (computed in staging per YAML spec).
Pattern follows kraeftpakker: INSERT-only, IDENTITY assigns DW_EK, incremental.

Uses dimension staging tables for ILS and MySupply:
- stg.dim_kreditor_ils: Deduplicated ILS kreditor data
- stg.dim_kreditor_mysupply: Merged MySupply metadata + invoice headers
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_kreditor_sydax"]),
        AssetKey(["erp", "stg_dim_kreditor_ils"]),
        AssetKey(["erp", "stg_dim_kreditor_mysupply"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK to Kreditor via IDENTITY using CleanCode_Kreditor as natural key",
)
def map_kreditor(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Kreditor CleanCodes into map.Kreditor"""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Kreditor map update using pre-staging tables...")

            # DIAGNOSTIC: Count source records with CleanCode
            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_kreditor_sydax]")
            sydax_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_kreditor_ils]")
            ils_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_kreditor_mysupply]")
            mysupply_count = cursor.fetchone()[0]

            context.log.info(
                f"Source counts: SydAX={sydax_count}, ILS={ils_count}, MySupply={mysupply_count}"
            )

            cursor.execute(
                """
                INSERT INTO [map].[Kreditor] (
                    CleanCode_Kreditor,
                    DW_Timestamp_Insert
                )
                SELECT DISTINCT
                    LTRIM(RTRIM(k.CleanCode_Kreditor)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM (
                    SELECT s.CleanCode_Kreditor
                    FROM [stg].[dim_kreditor_sydax] s

                    UNION

                    SELECT i.CleanCode_Kreditor
                    FROM [stg].[dim_kreditor_ils] i

                    UNION

                    SELECT m.CleanCode_Kreditor
                    FROM [stg].[dim_kreditor_mysupply] m
                ) k
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [map].[Kreditor] m
                    WHERE m.CleanCode_Kreditor = LTRIM(RTRIM(k.CleanCode_Kreditor))
                );
            """
            )

            new_count = cursor.rowcount
            conn.commit()

            context.log.info(f"➕ Inserted {new_count} new Kreditor entries")

            # DIAGNOSTIC: Sample recent inserts
            if new_count > 0:
                cursor.execute(
                    """
                    SELECT TOP 5 CleanCode_Kreditor
                    FROM map.Kreditor
                    ORDER BY DW_Timestamp_Insert DESC
                """
                )
                recent_inserts = cursor.fetchall()
                context.log.info(f"📝 Sample new CleanCodes: {[r[0] for r in recent_inserts]}")

            # Get total count
            cursor.execute("SELECT COUNT(*) FROM map.Kreditor")
            total_count = cursor.fetchone()[0]

            context.log.info(
                f"✅ Map update complete: {new_count} new, {total_count} total Kreditor entries"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Kreditor",
            "natural_key": "CleanCode_Kreditor",
            "source_tables": "stg.dim_kreditor_sydax, stg.dim_kreditor_ils, stg.dim_kreditor_mysupply",
            "cleancode_logic": "CleanCode_Kreditor (CVR_* pre-computed in extraction, aliased in dim staging)",
            "clean_architecture_note": "DW_EK_Kreditor assigned via IDENTITY - source of truth",
        }
    )
