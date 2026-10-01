# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Fakturalinje

Assign dw_ek_Fakturalinje via IDENTITY using canonical clean_code_Fakturalinje as natural key.
Logic: Insert new fakturalinje from MySupply dimension staging table.

Traceability components are stored as bk_faktura_header, bk_faktura_lines, bk_faktura_metadata.
Pattern: INSERT-only, IDENTITY assigns DW_EK, incremental.

Uses staging table:
- stg.dim_Fakturalinje_mysupply: Deduplicated CleanCode combinations with dimension attributes
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_Fakturalinje_mysupply"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_Fakturalinje via IDENTITY using composite CleanCode as natural key",
)
def map_Fakturalinje(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Fakturalinje combinations into map.Fakturalinje."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Fakturalinje map update using staging table...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Fakturalinje_mysupply]")
            source_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {source_count} records with valid CleanCodes")

            cursor.execute(
                """
            INSERT INTO [map].[Fakturalinje] (
                bk_faktura_lines,
                bk_faktura_header,
                bk_faktura_metadata,
                clean_code_Fakturalinje,
                dw_timestamp_insert
            )
            SELECT
                source.bk_faktura_lines,
                source.bk_faktura_header,
                source.bk_faktura_metadata,
                source.clean_code_Fakturalinje,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_Fakturalinje_mysupply] source
            WHERE NOT EXISTS (
                SELECT 1
                FROM [map].[Fakturalinje] m
                WHERE m.clean_code_Fakturalinje = source.clean_code_Fakturalinje
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
            "natural_key": "clean_code_Fakturalinje (composite)",
            "source_table": "stg.dim_Fakturalinje_mysupply",
            "pattern": "INSERT (insert-only)",
        }
    )
