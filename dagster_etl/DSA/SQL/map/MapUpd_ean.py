# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: EAN

Assign DW_EK_EAN to EAN codes via IDENTITY.
Reads from stg.dim_ean, INSERT into map.EAN on CleanCode_EAN when not present.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["stg_dim_ean"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_EAN via IDENTITY, INSERT from stg.dim_ean into map.EAN",
)
def map_ean(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new EAN codes into map.EAN."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_ean]")
            stg_count = cursor.fetchone()[0]
            context.log.info(f"Staging has {stg_count} EAN entries")

            cursor.execute(
                """
            INSERT INTO [map].[EAN] (
                CleanCode_EAN,
                DW_Timestamp_Insert,
                DW_Timestamp_Update
            )
            SELECT
                source.CleanCode_EAN,
                CAST(SYSDATETIME() AS datetime2(0)),
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_ean] source
            WHERE source.CleanCode_EAN IS NOT NULL
              AND source.CleanCode_EAN <> ''
              AND NOT EXISTS (
                SELECT 1
                FROM [map].[EAN] m
                WHERE m.CleanCode_EAN = source.CleanCode_EAN
            );
        """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[EAN]")
            total_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(f"map.EAN: {new_count} inserted, {total_count} total")

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.EAN",
            "pattern": "INSERT (insert-only)",
        }
    )
