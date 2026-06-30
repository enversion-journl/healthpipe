# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Kreditor from MySupply (Dimension Layer)

Aggregate MySupply kreditor data at CVR_CleanCode level.
Derived from pre_stg_fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_kreditor_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "pre_stg_fakturalinje_mysupply"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "mysupply",
        "entity": "kreditor",
    },
    description="Aggregate MySupply kreditor data at CleanCode_Kreditor level - derived from pre_stg_fakturalinje_mysupply",
)
def stg_dim_kreditor_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply kreditor data.

    Aggregates pre_stg_fakturalinje_mysupply at CleanCode_Kreditor level:
    - Selects most complete record (non-NULL values, most recent)
    - Deduplicates to one row per CleanCode_Kreditor
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_kreditor_mysupply from base layer...")

            # Drop existing table
            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_kreditor_mysupply];")

            # Create staging table aggregating from base layer
            cursor.execute(
                """
                SELECT
                    CleanCode_Kreditor,
                    LeverandorCVR,
                    FirmaNavn,
                    ByNavn,
                    Adresse,
                    Postnummer,
                    FakturaValutaKode,
                    FakturaDato
                INTO [stg].[dim_kreditor_mysupply]
                FROM (
                    SELECT
                        CleanCode_Kreditor,
                        LeverandorCVR,
                        FirmaNavn,
                        ByNavn,
                        Adresse,
                        Postnummer,
                        FakturaValutaKode,
                        FakturaDato,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Kreditor
                            ORDER BY
                                CASE WHEN FirmaNavn IS NOT NULL AND FirmaNavn <> '' THEN 0 ELSE 1 END,
                                CASE WHEN LeverandorCVR IS NOT NULL AND LeverandorCVR <> '' THEN 0 ELSE 1 END,
                                FakturaDato DESC
                        ) AS rn
                    FROM [stg].[pre_fakturalinje_mysupply]
                    WHERE CleanCode_Kreditor IS NOT NULL
                      AND CleanCode_Kreditor <> ''
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_kreditor_mysupply with {row_count} rows")

            # Create unique clustered index
            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_kreditor_mysupply_CleanCode
                ON [stg].[dim_kreditor_mysupply](CleanCode_Kreditor);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            # Diagnostic: Check source coverage
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(FirmaNavn) AS has_firma_navn,
                    COUNT(LeverandorCVR) AS has_cvr,
                    COUNT(FakturaValutaKode) AS has_valuta
                FROM [stg].[dim_kreditor_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with FirmaNavn, "
                f"{stats[2]}/{stats[0]} with CVR, {stats[3]}/{stats[0]} with Valutakode"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_kreditor_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_fakturalinje_mysupply",
            "deduplication": "ROW_NUMBER() on CleanCode_Kreditor",
            "has_firma_navn": stats[1],
            "has_cvr": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
