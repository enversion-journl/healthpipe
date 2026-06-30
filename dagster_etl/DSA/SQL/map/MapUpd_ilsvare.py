# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: IlsVare

Assign DW_EK_IlsVare to ILS item numbers via IDENTITY.
Reads from stg.map_ilsvare_ils, INSERT into map.IlsVare on CleanCode when not present.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_map_ilsvare_ils"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_IlsVare via IDENTITY, INSERT from stg.map_ilsvare_ils",
)
def map_ilsvare(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table: Insert new ILS item numbers into map.IlsVare."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [stg].[map_ilsvare_ils]")
            stg_count = cursor.fetchone()[0]
            context.log.info(f"Staging has {stg_count} IlsVare entries")

            cursor.execute(
                """
                INSERT INTO [map].[IlsVare] (
                    CleanCode_IlsVare,
                    DW_Timestamp_Insert,
                    DW_Timestamp_Update
                )
                SELECT
                    source.CleanCode_IlsVare,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[map_ilsvare_ils] source
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [map].[IlsVare] m
                    WHERE m.CleanCode_IlsVare = source.CleanCode_IlsVare
                );
            """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[IlsVare]")
            total_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(f"map.IlsVare: {new_count} inserted, {total_count} total")

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total": total_count,
            "table": "map.IlsVare",
            "pattern": "INSERT (insert-only)",
        }
    )
