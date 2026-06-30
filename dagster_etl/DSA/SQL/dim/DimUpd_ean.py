# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: EAN

Populate dim.EAN from stg.dim_ean with resolved surrogate keys.
Logic: Type 1 SCD - overwrite all attributes on each run.

Resolves:
- DW_EK_EAN from map.EAN

Flat dimension (no parent hierarchy, no Organization FK).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_ean"]),
        AssetKey(["stg_dim_ean"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description="Populate dim.EAN (Type 1 SCD) with EAN and Navn_Nemhandelsregister",
)
def dim_ean(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: resolve EKs and MERGE into dim.EAN."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Building temp table with resolved EKs...")

            # Use LEFT JOIN to capture both resolved and unmapped rows in one pass
            cursor.execute(
                """
            DROP TABLE IF EXISTS #EAN_Resolved;

            SELECT
                m.DW_EK_EAN,
                stg.EAN,
                stg.Navn_Nemhandelsregister
            INTO #EAN_Resolved
            FROM [stg].[dim_ean] stg
            LEFT JOIN [map].[EAN] m
                ON m.CleanCode_EAN = stg.CleanCode_EAN
        """
            )

            # Check for unmapped rows (DW_EK_EAN IS NULL means no map match)
            cursor.execute("SELECT COUNT(*) FROM #EAN_Resolved WHERE DW_EK_EAN IS NULL")
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                raise ValueError(
                    f"dim_ean: {unmapped_rows} staging rows have no match in map.EAN. "
                    "Ensure map_ean runs before dim_ean and staging CleanCodes are valid."
                )

            cursor.execute("SELECT COUNT(*) FROM #EAN_Resolved")
            resolved_count = cursor.fetchone()[0]
            context.log.info(f"Resolved {resolved_count} rows for dim.EAN")

            cursor.execute(
                """
            MERGE INTO [dim].[EAN] AS target
            USING #EAN_Resolved AS source
            ON target.DW_EK_EAN = source.DW_EK_EAN
            WHEN MATCHED THEN
                UPDATE SET
                    EAN = source.EAN,
                    Navn_Nemhandelsregister = source.Navn_Nemhandelsregister,
                    DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_EAN, EAN, Navn_Nemhandelsregister,
                    DW_Timestamp_Insert, DW_Timestamp_Update
                )
                VALUES (
                    source.DW_EK_EAN, source.EAN, source.Navn_Nemhandelsregister,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                )
            OUTPUT $action;
        """
            )

            merge_results = cursor.fetchall()
            inserted = sum(1 for r in merge_results if r[0] == "INSERT")
            updated = sum(1 for r in merge_results if r[0] == "UPDATE")

            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #EAN_Resolved")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN Navn_Nemhandelsregister IS NOT NULL
                         AND Navn_Nemhandelsregister <> '' THEN 1 ELSE 0 END) AS has_name
            FROM [dim].[EAN]
            WHERE DW_EK_EAN <> -1
        """
            )
            stats = cursor.fetchone()
            total = stats[0]
            has_name = stats[1]
        finally:
            cursor.close()

    context.log.info(
        f"dim.EAN: {inserted} inserted, {updated} updated, {total} total "
        f"({has_name} with Navn_Nemhandelsregister)"
    )

    return MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": total,
            "has_navn_nemhandelsregister": has_name,
            "table": "dim.EAN",
            "scd_type": "Type 1",
        }
    )
