# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: BudgetEgenskaber from SydAX (Dimension Layer)

Derives the BudgetEgenskaber dimension from ext.budgetpost_enriched.
Deduplicates at CleanCode_BudgetEgenskaber grain, keeping the most recent row.

Grain: CleanCode_BudgetEgenskaber (composite of 12 descriptive column values)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_budget_egenskaber_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["ext_budgetpost_enriched_sydax_daily"]),
        AssetKey(["ext_budgetpost_enriched_sydax_monthly"]),
        AssetKey(["ext_budgetpost_enriched_sydax_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_dim",
        "source": "sydax",
        "entity": "BudgetEgenskaber",
    },
    description=(
        "BudgetEgenskaber dimension staging from ext.budgetpost_enriched. "
        "Grain: CleanCode_BudgetEgenskaber."
    ),
)
def stg_dim_budget_egenskaber_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Deduplicate ext.budgetpost_enriched at CleanCode_BudgetEgenskaber grain for dim.BudgetEgenskaber."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(
                "Creating stg.dim_BudgetEgenskaber_sydax from ext.budgetpost_enriched..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_BudgetEgenskaber_sydax];")

            cursor.execute(
                """
                SELECT
                    CleanCode_BudgetEgenskaber,
                    bk_ref_BudgetmodelId,
                    Budgetmodelnummer,
                    Budgetmodeltekst,
                    Budgetmodeltype,
                    ErLukket,
                    Valutakode,
                    DebetKredit,
                    Budgetkode,
                    Aendringstype,
                    AendringstypeBeskrivelse,
                    BeloebstypeId,
                    BudgetbeloebTypetekst,
                    BudgetpostStatusId,
                    BudgetpostStatusTekst,
                    Omkostningsstednummer
                INTO [stg].[dim_BudgetEgenskaber_sydax]
                FROM (
                    SELECT
                        CleanCode_BudgetEgenskaber,
                        ref_BudgetmodelId AS bk_ref_BudgetmodelId,
                        Budgetmodelnummer,
                        Budgetmodeltekst,
                        Budgetmodeltype,
                        ErLukket_Budgetmodel AS ErLukket,
                        Valutakode,
                        DebetKredit,
                        Budgetkode,
                        Aendringstype,
                        AendringstypeBeskrivelse,
                        BeloebstypeId,
                        BudgetbeloebTypetekst,
                        BudgetpostStatusId,
                        BudgetpostStatusTekst,
                        Omkostningsstednummer,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_BudgetEgenskaber
                            ORDER BY DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [ext].[budgetpost_enriched]
                    WHERE CleanCode_BudgetEgenskaber IS NOT NULL
                ) ranked
                WHERE rn = 1;
                """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_BudgetEgenskaber_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_BudgetEgenskaber_sydax_PK
                ON [stg].[dim_BudgetEgenskaber_sydax](CleanCode_BudgetEgenskaber);
                """
            )

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Budgetmodelnummer) AS has_nummer,
                    COUNT(Budgetmodeltekst) AS has_tekst,
                    COUNT(Budgetmodeltype) AS has_type,
                    SUM(CASE WHEN ErLukket = 1 THEN 1 ELSE 0 END) AS lukket_count,
                    COUNT(Valutakode) AS has_valutakode,
                    COUNT(Omkostningsstednummer) AS has_okosted
                FROM [stg].[dim_BudgetEgenskaber_sydax]
                """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with Budgetmodelnummer, "
                f"{stats[2]}/{stats[0]} with Budgetmodeltekst, "
                f"{stats[3]}/{stats[0]} with Budgetmodeltype. "
                f"Lukket: {stats[4]}/{stats[0]}. "
                f"Valutakode: {stats[5]}/{stats[0]}. "
                f"Omkostningsstednummer: {stats[6]}/{stats[0]}."
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_BudgetEgenskaber_sydax",
            "row_count": row_count,
            "source_table": "ext.budgetpost_enriched",
            "grain": "CleanCode_BudgetEgenskaber (composite)",
            "has_nummer": stats[1],
            "has_tekst": stats[2],
            "has_type": stats[3],
            "lukket_count": stats[4],
            "has_valutakode": stats[5],
            "has_okosted": stats[6],
            "layer": "stg_dim (Layer 1)",
        }
    )
