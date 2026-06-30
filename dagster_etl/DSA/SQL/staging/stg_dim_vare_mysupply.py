# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vare from MySupply (Dimension Layer)

Aggregate MySupply vare data at (CVR_CleanCode, Leverandoer_Varenummer_CleanCode) level.
Derived from pre_stg_fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_vare_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "pre_stg_fakturalinje_mysupply"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "mysupply",
        "entity": "vare",
    },
    description="Aggregate MySupply vare data at (CleanCode_Kreditor, CleanCode_LeverandoerVarenummer) level - derived from pre_stg_fakturalinje_mysupply",
)
def stg_dim_vare_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply vare (product/item) data.

    Aggregates pre_stg_fakturalinje_mysupply at composite key level:
    - CleanCode_Kreditor from base layer
    - CleanCode_LeverandoerVarenummer from base layer
    - Selects most complete/recent item name
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_vare_mysupply from base layer...")

            # Drop existing table
            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_vare_mysupply];")

            # Create staging table aggregating from base layer
            cursor.execute(
                """
                SELECT
                    CleanCode_Kreditor,
                    CleanCode_LeverandoerVarenummer,
                    Item_FakturaUdstederVareNr,
                    Item_FakturaUdstederVareNavn,
                    FakturaUdstederCVRNummer,
                    FakturaDato AS latest_invoice_date,
                    Line_DW_LoadTimestamp AS DW_LoadTimestamp
                INTO [stg].[dim_vare_mysupply]
                FROM (
                    SELECT
                        CleanCode_Kreditor,
                        CleanCode_LeverandoerVarenummer,
                        Item_FakturaUdstederVareNr,
                        Item_FakturaUdstederVareNavn,
                        FakturaUdstederCVRNummer,
                        FakturaDato,
                        Line_DW_LoadTimestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Kreditor, CleanCode_LeverandoerVarenummer
                            ORDER BY
                                -- Prefer non-empty item names
                                CASE WHEN Item_FakturaUdstederVareNavn IS NOT NULL
                                    AND Item_FakturaUdstederVareNavn <> '' THEN 0 ELSE 1 END,
                                FakturaDato DESC
                        ) AS rn
                    FROM [stg].[pre_fakturalinje_mysupply]
                    WHERE CleanCode_LeverandoerVarenummer IS NOT NULL
                      AND CleanCode_LeverandoerVarenummer <> ''
                      AND CleanCode_Kreditor IS NOT NULL
                      AND CleanCode_Kreditor <> ''
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_vare_mysupply with {row_count} rows")

            # Create unique clustered index on composite key
            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_vare_mysupply_CompositeKey
                ON [stg].[dim_vare_mysupply](CleanCode_Kreditor, CleanCode_LeverandoerVarenummer);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            # Diagnostic: Check attribute coverage
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Item_FakturaUdstederVareNavn) AS has_navn,
                    COUNT(NULLIF(Item_FakturaUdstederVareNavn, '')) AS has_nonempty_navn
                FROM [stg].[dim_vare_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(f"Coverage: {stats[2]}/{stats[0]} with non-empty item name")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_vare_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_fakturalinje_mysupply",
            "composite_key": "CleanCode_Kreditor + CleanCode_LeverandoerVarenummer",
            "has_item_name": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
