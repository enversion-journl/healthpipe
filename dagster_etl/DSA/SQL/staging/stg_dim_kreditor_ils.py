# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Kreditor from ILS (Dimension Layer)

Aggregate ILS kreditor data at CVR_CleanCode level.
Derived from pre_stg_vareaftaler_ils + stg_lev_ils.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_kreditor_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_vareaftaler_ils"]),
        AssetKey(["ext_lev_ils"]),  # Keep dependency for kreditors not in contracts
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "kreditor",
    },
    description="Aggregate ILS kreditor data at CleanCode_Kreditor level - derived from pre_stg_vareaftaler_ils + stg_lev_ils",
)
def stg_dim_kreditor_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for ILS kreditor data.

    Combines kreditors from:
    - pre_stg_vareaftaler_ils: Kreditors with active contracts
    - stg_lev_ils: All kreditors (including those without contracts)

    Deduplicates at CleanCode_Kreditor level.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_kreditor_ils from base layer...")

            # Drop existing table
            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_kreditor_ils];")

            # Create staging table combining sources
            cursor.execute(
                """
                WITH KreditorSources AS (
                    -- Source 1: Kreditors from contracts (pre_stg_vareaftaler_ils)
                    SELECT
                        CleanCode_Kreditor,
                        se_nr,
                        COALESCE(laftd_lnr, lafth_lnr) AS lnr,
                        kreditor_navn AS navn,
                        adr,
                        adr2,
                        postdi,
                        kontakt,
                        cvr_nr,
                        oprdto,
                        DW_LoadTimestamp,
                        1 AS has_contract
                    FROM [stg].[pre_vareaftaler_ils]
                    WHERE CleanCode_Kreditor IS NOT NULL
                      AND CleanCode_Kreditor <> ''

                    UNION ALL

                    -- Source 2: All kreditors from lev_ils (including those without contracts)
                    SELECT
                        CVR_CleanCode AS CleanCode_Kreditor,
                        se_nr,
                        lnr,
                        navn,
                        adr,
                        adr2,
                        postdi,
                        kontakt,
                        cvr_nr,
                        oprdto,
                        DW_LoadTimestamp,
                        0 AS has_contract
                    FROM (
                        SELECT
                            lev.*,
                            CASE
                                WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
                                THEN 'CVR_' + CASE WHEN REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') LIKE 'DK[0-9]%' THEN SUBSTRING(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''), 3, LEN(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''))) ELSE REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') END
                                ELSE NULL
                            END AS CVR_CleanCode
                        FROM [ext].[lev_ils] lev
                    ) lev_with_cc
                    WHERE CVR_CleanCode IS NOT NULL
                      AND CVR_CleanCode <> ''
                )
                SELECT
                    CleanCode_Kreditor,
                    se_nr,
                    lnr,
                    navn,
                    adr,
                    adr2,
                    postdi,
                    kontakt,
                    cvr_nr,
                    oprdto,
                    DW_LoadTimestamp
                INTO [stg].[dim_kreditor_ils]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Kreditor
                            ORDER BY
                                has_contract DESC,  -- Prefer kreditors with contracts
                                CASE WHEN navn IS NOT NULL AND navn <> '' THEN 0 ELSE 1 END,
                                oprdto DESC
                        ) AS rn
                    FROM KreditorSources
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_kreditor_ils with {row_count} rows")

            # Create unique clustered index
            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_kreditor_ils_CleanCode
                ON [stg].[dim_kreditor_ils](CleanCode_Kreditor);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_kreditor_ils",
            "row_count": row_count,
            "source_tables": "stg.pre_vareaftaler_ils + ext.lev_ils",
            "deduplication": "ROW_NUMBER() on CleanCode_Kreditor (prefer contract kreditors)",
            "layer": "dim (Layer 1)",
        }
    )
