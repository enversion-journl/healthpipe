# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Kontoplan from SydAX (Dimension Layer)

Aggregate SydAX kontoplan data at CleanCode_Kontoplan level with full hierarchy attributes.
Derived from pre_stg_postering_sydax + stg_finanskonto_sydax.

Includes hierarchy columns (F1-F3, D1, G1-G2, S1-S7, A1-A2, tekst_*, Omkostningssted)
so that DimUpd_kontoplan can read from this single stg table.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_kontoplan_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_postering_sydax"]),
        AssetKey(["ext_finanskonto_sydax_daily"]),
        AssetKey(["ext_finanskonto_sydax_monthly"]),
        AssetKey(["ext_finanskonto_sydax_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "kontoplan",
    },
    description="Aggregate SydAX kontoplan at CleanCode_Kontoplan level with full hierarchy attributes",
)
def stg_dim_kontoplan_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX kontoplan (chart of accounts) data with hierarchy enrichment.

    Combines accounts from:
    - pre_stg_postering_sydax: Accounts with postings (carries hierarchy attributes)
    - stg_finanskonto_sydax: All accounts (including those without postings)

    Deduplicates at CleanCode_Kontoplan level.
    Hierarchy columns come from pre_stg_postering_sydax only (the enriched source).
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_kontoplan_sydax from base layer...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_kontoplan_sydax];")

            cursor.execute(
                """
                WITH KontoplanSources AS (
                    -- Source 1: Accounts from postings (pre_stg_postering_sydax) with hierarchy
                    SELECT DISTINCT
                        p.CleanCode_Kontoplan,
                        p.bk_FinanskontoId AS FinanskontoId,
                        p.Kontonummer,
                        CAST(p.F1 AS NVARCHAR(50)) AS F1,
                        CAST(p.F2 AS NVARCHAR(50)) AS F2,
                        CAST(p.F3 AS NVARCHAR(50)) AS F3,
                        CAST(p.D1 AS NVARCHAR(50)) AS D1,
                        CAST(p.G1 AS NVARCHAR(50)) AS G1,
                        CAST(p.G2 AS NVARCHAR(50)) AS G2,
                        CAST(p.S1 AS NVARCHAR(50)) AS S1,
                        CAST(p.S2 AS NVARCHAR(50)) AS S2,
                        CAST(p.S3 AS NVARCHAR(50)) AS S3,
                        CAST(p.S4 AS NVARCHAR(50)) AS S4,
                        CAST(p.S5 AS NVARCHAR(50)) AS S5,
                        CAST(p.S6 AS NVARCHAR(50)) AS S6,
                        CAST(p.S7 AS NVARCHAR(50)) AS S7,
                        CAST(p.A1 AS NVARCHAR(50)) AS A1,
                        CAST(p.A2 AS NVARCHAR(50)) AS A2,
                        CAST(p.tekst_S1 AS NVARCHAR(255)) AS tekst_S1,
                        CAST(p.tekst_S2 AS NVARCHAR(255)) AS tekst_S2,
                        CAST(p.tekst_S3 AS NVARCHAR(255)) AS tekst_S3,
                        CAST(p.tekst_S4 AS NVARCHAR(255)) AS tekst_S4,
                        CAST(p.tekst_S5 AS NVARCHAR(255)) AS tekst_S5,
                        CAST(p.tekst_S6 AS NVARCHAR(255)) AS tekst_S6,
                        CAST(p.tekst_S7 AS NVARCHAR(255)) AS tekst_S7,
                        CAST(p.tekst_G1 AS NVARCHAR(255)) AS tekst_G1,
                        CAST(p.tekst_G2 AS NVARCHAR(255)) AS tekst_G2,
                        CAST(p.tekst_A1 AS NVARCHAR(255)) AS tekst_A1,
                        CAST(p.tekst_A2 AS NVARCHAR(255)) AS tekst_A2,
                        CAST(p.tekst_F1 AS NVARCHAR(255)) AS tekst_F1,
                        CAST(p.tekst_F2 AS NVARCHAR(255)) AS tekst_F2,
                        CAST(p.tekst_F3 AS NVARCHAR(255)) AS tekst_F3,
                        CAST(p.tekst_D1 AS NVARCHAR(255)) AS tekst_D1,
                        CAST(p.Omkostningsstednummer AS NVARCHAR(50)) AS Omkostningsstednummer,
                        TRY_CAST(p.Omkostningssted_niveau AS INT) AS Omkostningssted_niveau,
                        p.DW_LoadTimestamp,
                        1 AS has_posting
                    FROM [stg].[pre_postering_sydax] p
                    WHERE p.CleanCode_Kontoplan IS NOT NULL
                      AND p.CleanCode_Kontoplan <> ''

                    UNION ALL

                    -- Source 2: All accounts from ext.finanskonto_sydax (no hierarchy columns)
                    SELECT
                        'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(fk.FinanskontoId AS NVARCHAR(100)), ' ', '')))) AS CleanCode_Kontoplan,
                        fk.FinanskontoId,
                        fk.Kontonummer,
                        NULL, NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL,
                        NULL, NULL,
                        fk.DW_LoadTimestamp,
                        0 AS has_posting
                    FROM [ext].[finanskonto_sydax] fk
                    WHERE fk.FinanskontoId IS NOT NULL
                )
                SELECT
                    CleanCode_Kontoplan,
                    FinanskontoId,
                    Kontonummer,
                    F1, F2, F3, D1, G1, G2,
                    S1, S2, S3, S4, S5, S6, S7,
                    A1, A2,
                    tekst_S1, tekst_S2, tekst_S3, tekst_S4, tekst_S5, tekst_S6, tekst_S7,
                    tekst_G1, tekst_G2, tekst_A1, tekst_A2,
                    tekst_F1, tekst_F2, tekst_F3, tekst_D1,
                    Omkostningsstednummer, Omkostningssted_niveau,
                    DW_LoadTimestamp
                INTO [stg].[dim_kontoplan_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Kontoplan
                            ORDER BY
                                has_posting DESC,
                                DW_LoadTimestamp DESC
                        ) AS rn
                    FROM KontoplanSources
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_kontoplan_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_kontoplan_sydax_CleanCode
                ON [stg].[dim_kontoplan_sydax](CleanCode_Kontoplan);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Kontonummer) AS has_kontonummer,
                    COUNT(S1) AS has_s1,
                    COUNT(F1) AS has_f1,
                    COUNT(Omkostningsstednummer) AS has_omkost
                FROM [stg].[dim_kontoplan_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with Kontonummer, "
                f"{stats[2]}/{stats[0]} with S1, {stats[3]}/{stats[0]} with F1, "
                f"{stats[4]}/{stats[0]} with Omkostningsstednummer"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_kontoplan_sydax",
            "row_count": row_count,
            "source_tables": "stg.pre_postering_sydax + ext.finanskonto_sydax",
            "deduplication": "ROW_NUMBER() on CleanCode_Kontoplan (prefer posting accounts)",
            "has_kontonummer": stats[1],
            "has_s1": stats[2],
            "has_f1": stats[3],
            "has_omkost": stats[4],
            "layer": "dim (Layer 1)",
        }
    )
