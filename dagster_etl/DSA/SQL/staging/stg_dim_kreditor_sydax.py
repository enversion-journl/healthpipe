# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Kreditor from SydAX (Dimension Layer)

Aggregate SydAX kreditor data at CleanCode_Kreditor level.
Derived from pre_stg_postering_sydax + stg_kreditor_sydax.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_kreditor_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_postering_sydax"]),
        AssetKey(["ext_kreditor_sydax_daily"]),  # Daily partitioned kreditors
        AssetKey(["ext_kreditor_sydax_monthly"]),  # Monthly partitioned kreditors
        AssetKey(["ext_kreditor_sydax_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "kreditor",
    },
    description="Aggregate SydAX kreditor data at CleanCode_Kreditor level - derived from pre_stg_postering_sydax + stg_kreditor_sydax",
)
def stg_dim_kreditor_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX kreditor data.

    Combines kreditors from:
    - pre_stg_postering_sydax: Kreditors with postings/transactions
    - stg_kreditor_sydax: All kreditors (including those without transactions)

    Deduplicates at CleanCode_Kreditor level.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_kreditor_sydax from base layer...")

            # Drop existing table
            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_kreditor_sydax];")

            # Create staging table combining sources
            cursor.execute(
                """
                WITH KreditorSources AS (
                    -- Source 1: Kreditors from postings (pre_stg_postering_sydax)
                    SELECT DISTINCT
                        p.CleanCode_Kreditor,
                        p.bk_KreditorId AS KreditorId,
                        Kreditornummer,
                        Kreditornavn,
                        JuridiskEnhed,
                        DW_LoadTimestamp,
                        1 AS has_posting
                    FROM [stg].[pre_postering_sydax] p
                    WHERE p.CleanCode_Kreditor IS NOT NULL
                      AND p.CleanCode_Kreditor <> ''

                    UNION ALL

                    -- Source 2: All kreditors from ext.kreditor_sydax (CleanCode generated inline)
                    SELECT
                        CASE
                            WHEN k.JuridiskEnhed IS NOT NULL AND LTRIM(RTRIM(k.JuridiskEnhed)) <> ''
                            THEN 'CVR_' + CASE WHEN LTRIM(RTRIM(k.JuridiskEnhed)) LIKE 'DK[0-9]%'
                                               THEN SUBSTRING(LTRIM(RTRIM(k.JuridiskEnhed)), 3, LEN(LTRIM(RTRIM(k.JuridiskEnhed))))
                                               ELSE LTRIM(RTRIM(k.JuridiskEnhed)) END
                            ELSE NULL
                        END AS CleanCode_Kreditor,
                        k.KreditorId,
                        k.Kreditornummer,
                        k.Kreditornavn,
                        k.JuridiskEnhed,
                        k.DW_LoadTimestamp,
                        0 AS has_posting
                    FROM [ext].[kreditor_sydax] k
                    WHERE k.JuridiskEnhed IS NOT NULL
                      AND LTRIM(RTRIM(k.JuridiskEnhed)) <> ''
                )
                SELECT
                    CleanCode_Kreditor,
                    KreditorId,
                    Kreditornummer,
                    Kreditornavn,
                    JuridiskEnhed,
                    DW_LoadTimestamp
                INTO [stg].[dim_kreditor_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Kreditor
                            ORDER BY
                                has_posting DESC,  -- Prefer kreditors with postings
                                CASE WHEN Kreditornavn IS NOT NULL AND Kreditornavn <> '' THEN 0 ELSE 1 END,
                                DW_LoadTimestamp DESC
                        ) AS rn
                    FROM KreditorSources
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_kreditor_sydax with {row_count} rows")

            # Create unique clustered index
            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_kreditor_sydax_CleanCode
                ON [stg].[dim_kreditor_sydax](CleanCode_Kreditor);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            # Diagnostic
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Kreditornavn) AS has_navn,
                    COUNT(JuridiskEnhed) AS has_juridisk
                FROM [stg].[dim_kreditor_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with Kreditornavn, "
                f"{stats[2]}/{stats[0]} with JuridiskEnhed"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_kreditor_sydax",
            "row_count": row_count,
            "source_tables": "stg.pre_postering_sydax + ext.kreditor_sydax",
            "deduplication": "ROW_NUMBER() on CleanCode_Kreditor (prefer posting kreditors)",
            "has_kreditornavn": stats[1],
            "has_juridiskenhed": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
