# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Lager

Assign DW_EK_Lager to Lager codes via IDENTITY.
Reads from stg.dim_lager, INSERT into map.Lager on CleanCode_Lager when not present.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["stg_dim_lager"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_Lager via IDENTITY, INSERT from stg.dim_lager into map.Lager",
)
def map_lager(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Lager codes into map.Lager."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_lager]")
            stg_count = cursor.fetchone()[0]
            context.log.info(f"Staging has {stg_count} Lager entries")

            cursor.execute(
                """
            INSERT INTO [map].[Lager] (
                CleanCode_Lager,
                bk_Code,
                DW_Timestamp_Insert,
                DW_Timestamp_Update
            )
            SELECT
                source.CleanCode_Lager,
                source.Code,
                CAST(SYSDATETIME() AS datetime2(0)),
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_lager] source
            WHERE NOT EXISTS (
                SELECT 1
                FROM [map].[Lager] m
                WHERE m.CleanCode_Lager = source.CleanCode_Lager
            );
        """
            )

            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Lager]")
            total_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(f"map.Lager: {new_count} inserted, {total_count} total")

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total": total_count,
            "table": "map.Lager",
            "pattern": "INSERT (insert-only)",
        }
    )
