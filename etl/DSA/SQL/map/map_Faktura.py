# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Faktura

map_Faktura:
  Grain: metadata.id + statistic_invoice_header.id (MySupply source IDs).
  clean_code format: 'CleanCode_{metadata_id}_{header_id}'.
  Used by dim.Faktura and fct.Faktura.
  Source: stg.dim_Faktura_mysupply (maps seed from dimension staging).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_Faktura_mysupply"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_Faktura via IDENTITY using metadata_id + header_id grain for fct.Faktura.",
)
def map_Faktura(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Faktura keys from MySupply metadata+header grain."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Faktura map update (metadata+header grain)...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Faktura_mysupply]")
            staging_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {staging_count}")

            cursor.execute(
                """
            INSERT INTO [map].[Faktura] (
                bk_faktura_metadata,
                bk_faktura_header,
                clean_code_faktura,
                dw_timestamp_insert
            )
            SELECT
                src.bk_faktura_metadata,
                src.bk_faktura_header,
                src.clean_code_faktura,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_Faktura_mysupply] src
            WHERE NOT EXISTS (
                SELECT 1
                FROM [map].[Faktura] m
                WHERE m.clean_code_faktura = src.clean_code_faktura
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
            "natural_key": "clean_code_faktura (CleanCode_{metadata_id}_{header_id})",
            "source_tables": "stg.dim_Faktura_mysupply",
            "pattern": "INSERT WHERE NOT EXISTS (insert-only)",
        }
    )
