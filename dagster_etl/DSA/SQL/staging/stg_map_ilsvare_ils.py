# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: IlsVare Map from enriched vare (Layer 1)

Distinct CleanCode_IlsVare values for map.IlsVare population.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_map_ilsvare_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["ext_vare_enriched_ils_daily"]),
        AssetKey(["ext_vare_enriched_ils_monthly"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_dim",
        "source": "ils",
        "entity": "ilsvare",
    },
    description="Distinct CleanCode_IlsVare from ext.vare_enriched_ils for map population.",
)
def stg_map_ilsvare_ils(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Stage distinct IlsVare CleanCodes for map table."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS [stg].[map_ilsvare_ils];")

            cursor.execute(
                """
                SELECT DISTINCT
                    CleanCode_IlsVare
                INTO [stg].[map_ilsvare_ils]
                FROM [ext].[vare_enriched_ils]
                WHERE CleanCode_IlsVare IS NOT NULL
                  AND CleanCode_IlsVare <> '';
            """
            )
            row_count = cursor.rowcount

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_stg_map_ilsvare_ils_PK
                ON [stg].[map_ilsvare_ils](CleanCode_IlsVare);
            """
            )
            conn.commit()
        finally:
            cursor.close()

    context.log.info(f"stg.map_ilsvare_ils: {row_count} distinct CleanCodes")

    return MaterializeResult(
        metadata={
            "table": "stg.map_ilsvare_ils",
            "row_count": row_count,
            "source_table": "ext.vare_enriched_ils",
            "grain": "CleanCode_IlsVare (one per vnr)",
            "layer": "stg_dim (Layer 1)",
        }
    )
