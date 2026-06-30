# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Rekvirent

Assign DW_EK_Rekvirent to Rekvirent codes via IDENTITY.
Reads from stg.dim_rekvirent, INSERT-only into map.Rekvirent on CleanCode_Rekvirent.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["stg_dim_rekvirent"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "rekvirent"},
    description="Assign DW_EK_Rekvirent via IDENTITY, INSERT from stg.dim_rekvirent into map.Rekvirent",
)
def map_rekvirent(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: Insert new Rekvirent clean codes into map.Rekvirent."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_rekvirent]")
            stg_count = cursor.fetchone()[0]
            context.log.info(f"Staging has {stg_count} Rekvirent entries")

            cursor.execute(
                """
            INSERT INTO [map].[Rekvirent] (
                CleanCode_Rekvirent,
                bk_Code,
                DW_Timestamp_Insert,
                DW_Timestamp_Update
            )
            SELECT DISTINCT
                source.CleanCode_Rekvirent,
                source.Code,
                CAST(SYSDATETIME() AS datetime2(0)),
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_rekvirent] source
            WHERE source.CleanCode_Rekvirent IS NOT NULL
              AND source.CleanCode_Rekvirent <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM [map].[Rekvirent] target
                  WHERE target.CleanCode_Rekvirent = source.CleanCode_Rekvirent
              );
        """
            )

            new_count = cursor.rowcount

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[Rekvirent]")
            total_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(f"map.Rekvirent: {new_count} inserted, {total_count} total")

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Rekvirent",
            "pattern": "INSERT (insert-only)",
        }
    )
