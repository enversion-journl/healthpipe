# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Kontoplan

Assign DW_EK to Kontoplan via IDENTITY.
Logic from: erp_dims_and_facts.yml (CleanCode_Kontoplan natural key)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_maps",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_kontoplan_sydax"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK to Kontoplan via IDENTITY using stg_dim_kontoplan_sydax as source",
)
def map_kontoplan(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table maintenance: insert new Kontoplan clean codes into map.Kontoplan."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Kontoplan map update...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_kontoplan_sydax]")
            source_count = cursor.fetchone()[0]
            context.log.info(
                f"Staging source: {source_count} unique CleanCode_Kontoplan candidates"
            )

            cursor.execute(
                """
            INSERT INTO [map].[Kontoplan] (
                CleanCode_Kontoplan,
                bk_FinanskontoId, bk_Kontonummer,
                DW_Timestamp_Insert
            )
            SELECT
                d.CleanCode_Kontoplan,
                d.FinanskontoId, d.Kontonummer,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM [stg].[dim_kontoplan_sydax] d
            WHERE NOT EXISTS (
                  SELECT 1
                  FROM [map].[Kontoplan] m
                  WHERE m.CleanCode_Kontoplan = d.CleanCode_Kontoplan
              );
        """
            )

            new_count = cursor.rowcount
            context.log.info(f"Inserted {new_count} new Kontoplan entries")

            if new_count > 0:
                cursor.execute(
                    """
                SELECT TOP 5 CleanCode_Kontoplan
                FROM map.Kontoplan
                ORDER BY DW_Timestamp_Insert DESC
            """
                )
                recent_inserts = cursor.fetchall()
                context.log.info(
                    f"Sample new CleanCode_Kontoplan: {[r[0] for r in recent_inserts]}"
                )

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM map.Kontoplan")
            total_count = cursor.fetchone()[0]

            cursor.execute(
                """
            SELECT COUNT(*) FROM [stg].[dim_kontoplan_sydax] s
            WHERE NOT EXISTS (
                SELECT 1 FROM [map].[Kontoplan] m
                WHERE m.CleanCode_Kontoplan = s.CleanCode_Kontoplan
            )
        """
            )
            unmatched_count = cursor.fetchone()[0]

            context.log.info(
                f"Map update complete: {new_count} new, {total_count} total Kontoplan entries"
            )
            if unmatched_count > 0:
                context.log.warning(
                    f"{unmatched_count} staging kontoplan rows not matched to map (should be 0!)"
                )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Kontoplan",
            "natural_key": "CleanCode_Kontoplan",
            "source_table": "stg.dim_kontoplan_sydax",
        }
    )
