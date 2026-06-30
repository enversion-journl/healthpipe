# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladdelinje from SydAX (Fact Layer)

Select columns needed for fct_finanskladdelinje from pre_stg_finanskladdelinje_sydax.
Intermediate staging table for the fact table load.

Grain contract: one row per ref_LedgertransId (deterministic dedup applied here).
Business key: bk_ref_LedgertransId
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_fct_finanskladdelinje_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_finanskladdelinje_sydax"]),
        "map_finanskladdelinje",
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "finanskladdelinje",
    },
    description=(
        "Select columns needed for fct_finanskladdelinje from "
        "pre_stg_finanskladdelinje_sydax. Grain: ref_LedgertransId."
    ),
)
def stg_fct_finanskladdelinje_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX finanskladdelinje fact table.

    Enforces grain (one row per ref_LedgertransId) via deterministic dedup and
    performs measure and date key conversions for downstream MERGE.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_finanskladdelinje_sydax...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_finanskladdelinje_sydax];")

            cursor.execute(
                """
                ;WITH grain_dedup AS (
                    SELECT
                        p.ref_LedgertransId,
                        p.bk_ref_LedgertransId,
                        p.CleanCode_Finanskladdelinje,
                        p.CleanCode_Finanskladde,
                        p.CleanCode_Kontoplan,
                        p.Omkostningsstednummer,
                        p.Debet,
                        p.Kredit,
                        p.dato,
                        p.Linje_CreatedOn,
                        p.DW_LoadTimestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY p.ref_LedgertransId
                            ORDER BY p.DW_LoadTimestamp DESC
                        ) AS grain_rn
                    FROM [stg].[pre_finanskladdelinje_sydax] p
                    WHERE p.ref_LedgertransId IS NOT NULL
                )

                SELECT
                    g.ref_LedgertransId,
                    g.bk_ref_LedgertransId,
                    g.CleanCode_Finanskladdelinje,
                    g.CleanCode_Finanskladde,
                    g.CleanCode_Kontoplan,
                    g.Omkostningsstednummer,

                    TRY_CONVERT(DECIMAL(30,12), g.Debet) AS Debet,
                    TRY_CONVERT(DECIMAL(30,12), g.Kredit) AS Kredit,

                    COALESCE(
                        dbo.fn_DateTime_To_DateKey(
                            TRY_CONVERT(DATETIME2, g.dato)
                        ),
                        -1
                    ) AS DW_EK_Dato_Linje,
                    COALESCE(
                        dbo.fn_DateTime_To_DateKey(
                            TRY_CONVERT(DATETIME2, g.Linje_CreatedOn)
                        ),
                        -1
                    ) AS DW_EK_Dato_Oprettet,

                    TRY_CONVERT(DATETIME2, g.Linje_CreatedOn) AS DW_haendelsestidspunkt
                INTO [stg].[fct_finanskladdelinje_sydax]
                FROM grain_dedup g
                WHERE g.grain_rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_finanskladdelinje_sydax with {row_count} rows")

            cursor.execute(
                """
                SELECT COUNT(*) FROM [stg].[pre_finanskladdelinje_sydax]
                WHERE ref_LedgertransId IS NOT NULL
            """
            )
            pre_stg_count = cursor.fetchone()[0]
            duplicates_removed = pre_stg_count - row_count
            if duplicates_removed > 0:
                context.log.warning(
                    f"Removed {duplicates_removed} duplicate ref_LedgertransId rows "
                    "during grain enforcement (kept latest by DW_LoadTimestamp)"
                )

            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_fct_finanskladdelinje_sydax_PK
                ON [stg].[fct_finanskladdelinje_sydax](ref_LedgertransId);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_finanskladdelinje_sydax_Finanskladdelinje
                ON [stg].[fct_finanskladdelinje_sydax](CleanCode_Finanskladdelinje);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_finanskladdelinje_sydax_Finanskladde
                ON [stg].[fct_finanskladdelinje_sydax](CleanCode_Finanskladde);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_finanskladdelinje_sydax_Kontoplan
                ON [stg].[fct_finanskladdelinje_sydax](CleanCode_Kontoplan);
            """
            )

            context.log.info("Created indexes for grain and map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Finanskladdelinje) AS has_linje,
                    COUNT(CleanCode_Finanskladde) AS has_kladde,
                    COUNT(CleanCode_Kontoplan) AS has_kontoplan
                FROM [stg].[fct_finanskladdelinje_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} CleanCode_Finanskladdelinje, "
                f"{stats[2]}/{stats[0]} CleanCode_Finanskladde, "
                f"{stats[3]}/{stats[0]} CleanCode_Kontoplan"
            )

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN DW_EK_Dato_Linje <> -1 THEN 1 ELSE 0 END) AS has_linje_dato,
                    SUM(CASE WHEN DW_EK_Dato_Oprettet <> -1 THEN 1 ELSE 0 END) AS has_oprettet,
                    SUM(CASE WHEN DW_haendelsestidspunkt IS NOT NULL THEN 1 ELSE 0 END) AS has_haendelse
                FROM [stg].[fct_finanskladdelinje_sydax]
            """
            )
            date_stats = cursor.fetchone()
            context.log.info(
                f"Date key coverage (non -1): Linje={date_stats[1]}/{date_stats[0]}, "
                f"Oprettet={date_stats[2]}/{date_stats[0]}, "
                f"Haendelse={date_stats[3]}/{date_stats[0]}"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_finanskladdelinje_sydax",
            "target_table": "stg.fct_finanskladdelinje_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_finanskladdelinje_sydax",
            "grain": "ref_LedgertransId",
            "duplicates_removed": duplicates_removed,
            "has_cleancode_finanskladdelinje": stats[1],
            "has_cleancode_finanskladde": stats[2],
            "has_cleancode_kontoplan": stats[3],
            "layer": "stg_fct (Layer 1)",
        }
    )
