# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Fakturalinje

Assign DW_EK_Fakturalinje via IDENTITY using canonical CleanCode_Fakturalinje as natural key.
Logic: Insert new fakturalinje from MySupply dimension staging table.

Traceability components are stored as bk_FakturaHeader, bk_FakturaLines, bk_FakturaMetadata.
Pattern: INSERT-only, IDENTITY assigns DW_EK, incremental.

Uses staging table:
- stg.dim_fakturalinje_mysupply: Deduplicated CleanCode combinations with dimension attributes
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_fakturalinje_mysupply"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_Fakturalinje via IDENTITY using composite CleanCode as natural key",
)
def map_fakturalinje(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Fakturalinje combinations into map.Fakturalinje."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Fakturalinje map update using staging table...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_fakturalinje_mysupply]")
            source_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {source_count} records with valid CleanCodes")

            cursor.execute(
                """
            INSERT INTO [map].[Fakturalinje] (
                bk_FakturaLines,
                bk_FakturaHeader,
                bk_FakturaMetadata,
                CleanCode_Fakturalinje,
                DW_Timestamp_Insert
            )
            SELECT
                source.bk_FakturaLines,
                source.bk_FakturaHeader,
                source.bk_FakturaMetadata,
                source.CleanCode_Fakturalinje,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_fakturalinje_mysupply] source
            WHERE NOT EXISTS (
                SELECT 1
                FROM [map].[Fakturalinje] m
                WHERE m.CleanCode_Fakturalinje = source.CleanCode_Fakturalinje
            );
            """
            )

            new_count = cursor.rowcount
            context.log.info(f"Inserted {new_count} new Fakturalinje entries")

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Fakturalinje]")
            total_count = cursor.fetchone()[0]

            context.log.info(f"Map update complete: {total_count} total Fakturalinje entries")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Fakturalinje",
            "natural_key": "CleanCode_Fakturalinje (composite)",
            "source_table": "stg.dim_fakturalinje_mysupply",
            "pattern": "INSERT (insert-only)",
        }
    )
