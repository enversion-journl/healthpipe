# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Faktura

map_faktura:
  Grain: metadata.id + statistic_invoice_header.id (MySupply source IDs).
  CleanCode format: 'CleanCode_{metadata_id}_{header_id}'.
  Used by fct.Faktura.
  Source: stg.fct_faktura_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_faktura_mysupply"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_Faktura via IDENTITY using metadata_id + header_id grain for fct.Faktura.",
)
def map_faktura(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Faktura keys from MySupply metadata+header grain."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Faktura map update (metadata+header grain)...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_faktura_mysupply]")
            staging_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {staging_count}")

            cursor.execute(
                """
            INSERT INTO [map].[Faktura] (
                bk_FakturaMetadata,
                bk_FakturaHeader,
                CleanCode_Faktura,
                DW_Timestamp_Insert
            )
            SELECT
                src.bk_FakturaMetadata,
                src.bk_FakturaHeader,
                src.CleanCode_Faktura,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[fct_faktura_mysupply] src
            WHERE NOT EXISTS (
                SELECT 1
                FROM [map].[Faktura] m
                WHERE m.CleanCode_Faktura = src.CleanCode_Faktura
            );
            """
            )
            new_count = cursor.rowcount
            context.log.info(f"Inserted {new_count} new Faktura entries (metadata+header grain)")

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Faktura]")
            total_count = cursor.fetchone()[0]

            context.log.info(f"Map update complete: {total_count} total Faktura entries")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Faktura",
            "natural_key": "CleanCode_Faktura (CleanCode_{metadata_id}_{header_id})",
            "source_tables": "stg.fct_faktura_mysupply",
            "pattern": "INSERT WHERE NOT EXISTS (insert-only)",
        }
    )
