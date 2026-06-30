# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: IndkoebsanalyseLinje (stable CleanCode -> DW_EK for KMD SpendAnalysisLines fact)

Insert-only: assigns DW_EK_IndkoebsanalyseLinje via IDENTITY for new CleanCodes.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [map].[IndkoebsanalyseLinje] (
    CleanCode_IndkoebsanalyseLinje,
    DW_Timestamp_Insert,
    DW_Timestamp_Update
)
SELECT DISTINCT
    source.CleanCode_IndkoebsanalyseLinje,
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[fct_indkoebsanalyse_linje_kmd] source
WHERE source.CleanCode_IndkoebsanalyseLinje IS NOT NULL
  AND source.CleanCode_IndkoebsanalyseLinje <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM [map].[IndkoebsanalyseLinje] m
      WHERE m.CleanCode_IndkoebsanalyseLinje = source.CleanCode_IndkoebsanalyseLinje
  );
"""


@asset(
    name="map_IndkoebsanalyseLinje",
    group_name="facts_indkoebsanalyse",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_fct_indkoebsanalyse_linje_kmd"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK_IndkoebsanalyseLinje via IDENTITY from staging CleanCodes",
)
def map_IndkoebsanalyseLinje(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map table: insert new CleanCodes into map.IndkoebsanalyseLinje."""

    with mssql_target_staging.get_connection(timeout=0) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_indkoebsanalyse_linje_kmd]")
            stg_count = cursor.fetchone()[0]
            context.log.info(f"Staging has {stg_count} IndkoebsanalyseLinje entries")

            cursor.execute(INSERT_SQL)
            new_count = cursor.rowcount
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [map].[IndkoebsanalyseLinje]")
            total_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(f"map.IndkoebsanalyseLinje: {new_count} inserted, {total_count} total")

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total": total_count,
            "table": "map.IndkoebsanalyseLinje",
            "pattern": "INSERT (insert-only)",
        }
    )
