# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Budget from SydAX (Fact Layer)

Select columns needed for fct_budget from ext.budgetpost_enriched.
Intermediate staging table for the fact table load.

Grain contract: one row per bk_ref_LedgerbudgetId (deterministic dedup applied here).
All type conversions (date keys, measures) are performed in this layer
so the downstream fact asset only needs map lookups + MERGE.

Includes Omkostningsstednummer and CleanCode_Kontoplan for map lookups in FctUpd
(neither is persisted to fct.Budget -- only their resolved DW_EKs are).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_fct_budget_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["ext_budgetpost_enriched_sydax_daily"]),
        AssetKey(["ext_budgetpost_enriched_sydax_monthly"]),
        AssetKey(["ext_budgetpost_enriched_sydax_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "budget",
    },
    description=(
        "Select columns needed for fct_budget from ext.budgetpost_enriched. "
        "Grain: bk_ref_LedgerbudgetId."
    ),
)
def stg_fct_budget_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Staging for SydAX budget fact table.

    Enforces grain (one row per bk_ref_LedgerbudgetId) via deterministic dedup and
    performs all type conversions so the fact layer stays focused on map lookups.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Creating staging table stg.fct_budget_sydax...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_budget_sydax];")

            cursor.execute(
                """
                ;WITH grain_dedup AS (
                    SELECT
                        b.ref_LedgerbudgetId,
                        b.CleanCode_Budget,
                        b.CleanCode_BudgetEgenskaber,
                        b.CleanCode_Kontoplan,
                        b.bk_FinanskontoId,
                        b.Omkostningsstednummer,
                        b.Budgetdato,
                        b.BudgetAdoptedDate,
                        b.Oprettelsestidspunkt,
                        b.SidstAendretTidspunkt,
                        b.Budgetbeloeb,
                        b.BudgetbeloebValuta,
                        b.DW_LoadTimestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY b.ref_LedgerbudgetId
                            ORDER BY b.DW_LoadTimestamp DESC
                        ) AS grain_rn
                    FROM [ext].[budgetpost_enriched] b
                    WHERE b.ref_LedgerbudgetId IS NOT NULL
                )

                SELECT
                    CAST(g.ref_LedgerbudgetId AS NVARCHAR(100)) AS bk_ref_LedgerbudgetId,
                    g.CleanCode_Budget,
                    g.CleanCode_BudgetEgenskaber,
                    g.CleanCode_Kontoplan,
                    g.bk_FinanskontoId,
                    g.Omkostningsstednummer,

                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Budgetdato)), -1) AS DW_EK_Dato_Budget,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.BudgetAdoptedDate)), -1) AS DW_EK_Dato_BudgetAdopted,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Oprettelsestidspunkt)), -1) AS DW_EK_Dato_BudgetOprettet,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.SidstAendretTidspunkt)), -1) AS DW_EK_Dato_BudgetAendret,

                    TRY_CONVERT(DECIMAL(30,12), g.Budgetbeloeb) AS Budgetbeloeb,
                    TRY_CONVERT(DECIMAL(30,12), g.BudgetbeloebValuta) AS BudgetbeloebValuta,

                    TRY_CONVERT(DATETIME2, g.Oprettelsestidspunkt) AS DW_haendelsestidspunkt

                INTO [stg].[fct_budget_sydax]
                FROM grain_dedup g
                WHERE g.grain_rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_budget_sydax with {row_count} rows")

            cursor.execute(
                """
                SELECT COUNT(*) FROM [ext].[budgetpost_enriched]
                WHERE ref_LedgerbudgetId IS NOT NULL
            """
            )
            ext_count = cursor.fetchone()[0]
            duplicates_removed = ext_count - row_count
            if duplicates_removed > 0:
                context.log.warning(
                    f"Removed {duplicates_removed} duplicate ref_LedgerbudgetId rows "
                    "during grain enforcement (kept latest by DW_LoadTimestamp)"
                )

            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_fct_budget_sydax_PK
                ON [stg].[fct_budget_sydax](bk_ref_LedgerbudgetId);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_budget_sydax_Kontoplan
                ON [stg].[fct_budget_sydax](CleanCode_Kontoplan);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_budget_sydax_Budget
                ON [stg].[fct_budget_sydax](CleanCode_Budget);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_budget_sydax_BudgetEgenskaber
                ON [stg].[fct_budget_sydax](CleanCode_BudgetEgenskaber);
            """
            )

            context.log.info("Created indexes for map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Kontoplan) AS has_cleancode_kontoplan,
                    COUNT(CleanCode_Budget) AS has_cleancode_budget,
                    COUNT(CleanCode_BudgetEgenskaber) AS has_cleancode_BudgetEgenskaber,
                    COUNT(Omkostningsstednummer) AS has_okosted
                FROM [stg].[fct_budget_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kontoplan, "
                f"{stats[2]}/{stats[0]} with CleanCode_Budget, "
                f"{stats[3]}/{stats[0]} with CleanCode_BudgetEgenskaber, "
                f"{stats[4]}/{stats[0]} with Omkostningsstednummer (OKOSTED)"
            )

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN DW_EK_Dato_Budget <> -1 THEN 1 ELSE 0 END) AS has_budget_date,
                    SUM(CASE WHEN DW_EK_Dato_BudgetAdopted <> -1 THEN 1 ELSE 0 END) AS has_adopted_date,
                    SUM(CASE WHEN DW_EK_Dato_BudgetOprettet <> -1 THEN 1 ELSE 0 END) AS has_oprettet_date,
                    SUM(CASE WHEN DW_EK_Dato_BudgetAendret <> -1 THEN 1 ELSE 0 END) AS has_aendret_date,
                    SUM(CASE WHEN DW_haendelsestidspunkt IS NOT NULL THEN 1 ELSE 0 END) AS has_haendelse
                FROM [stg].[fct_budget_sydax]
            """
            )
            date_stats = cursor.fetchone()
            context.log.info(
                f"Date key coverage (non -1): "
                f"Budget={date_stats[1]}/{date_stats[0]}, "
                f"Adopted={date_stats[2]}/{date_stats[0]}, "
                f"Oprettet={date_stats[3]}/{date_stats[0]}, "
                f"Aendret={date_stats[4]}/{date_stats[0]}, "
                f"Haendelse={date_stats[5]}/{date_stats[0]}"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_budget_sydax",
            "row_count": row_count,
            "source_table": "ext.budgetpost_enriched",
            "grain": "bk_ref_LedgerbudgetId",
            "duplicates_removed": duplicates_removed,
            "has_kontoplan_clean": stats[1],
            "has_cleancode_budget": stats[2],
            "has_cleancode_BudgetEgenskaber": stats[3],
            "has_okosted": stats[4],
            "date_columns": 4,
            "measure_columns": 2,
            "layer": "stg_fct (Layer 1)",
        }
    )
