# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: BudgetEgenskaber from SydAX (Dimension Layer)

Derives the BudgetEgenskaber dimension from stg.pre_Budget_sydax.
Deduplicates at clean_code_BudgetEgenskaber grain, keeping the most recent row.

Grain: clean_code_BudgetEgenskaber (composite of 12 descriptive column values)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_BudgetEgenskaber_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Budget_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_dim",
        "source": "sydax",
        "entity": "BudgetEgenskaber",
    },
    description=(
        "BudgetEgenskaber dimension staging from stg.pre_Budget_sydax. "
        "Grain: clean_code_BudgetEgenskaber."
    ),
)
def stg_dim_BudgetEgenskaber_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Deduplicate stg.pre_Budget_sydax at clean_code_BudgetEgenskaber grain for dim.BudgetEgenskaber."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Creating stg.dim_BudgetEgenskaber_sydax from stg.pre_Budget_sydax...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_BudgetEgenskaber_sydax];")

            cursor.execute(
                """
                SELECT
                    clean_code_BudgetEgenskaber,
                    bk_ref_budgetmodel_id,
                    budgetmodelnummer,
                    budgetmodeltekst,
                    budgetmodeltype,
                    er_lukket,
                    valutakode,
                    debet_kredit,
                    budgetkode,
                    aendringstype,
                    aendringstype_beskrivelse,
                    beloebstype_id,
                    budgetbeloeb_typetekst,
                    budgetpost_status_id,
                    budgetpost_status_tekst,
                    omkostningsstednummer
                INTO [stg].[dim_BudgetEgenskaber_sydax]
                FROM (
                    SELECT
                        clean_code_BudgetEgenskaber,
                        ref_BudgetmodelId AS bk_ref_budgetmodel_id,
                        budgetmodelnummer,
                        budgetmodeltekst,
                        budgetmodeltype,
                        ErLukket_Budgetmodel AS er_lukket,
                        valutakode,
                        debet_kredit,
                        budgetkode,
                        aendringstype,
                        aendringstype_beskrivelse,
                        beloebstype_id,
                        budgetbeloeb_typetekst,
                        budgetpost_status_id,
                        budgetpost_status_tekst,
                        omkostningsstednummer,
                        ROW_NUMBER() OVER (
                            PARTITION BY clean_code_BudgetEgenskaber
                            ORDER BY dw_timestamp_update DESC
                        ) AS rn
                    FROM [stg].[pre_Budget_sydax]
                    WHERE clean_code_BudgetEgenskaber IS NOT NULL
                ) ranked
                WHERE rn = 1;
                """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_BudgetEgenskaber_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_BudgetEgenskaber_sydax_PK
                ON [stg].[dim_BudgetEgenskaber_sydax](clean_code_BudgetEgenskaber);
                """
            )

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(budgetmodelnummer) AS has_nummer,
                    COUNT(budgetmodeltekst) AS has_tekst,
                    COUNT(budgetmodeltype) AS has_type,
                    SUM(CASE WHEN er_lukket = 1 THEN 1 ELSE 0 END) AS lukket_count,
                    COUNT(valutakode) AS has_valutakode,
                    COUNT(omkostningsstednummer) AS has_okosted
                FROM [stg].[dim_BudgetEgenskaber_sydax]
                """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with budgetmodelnummer, "
                f"{stats[2]}/{stats[0]} with budgetmodeltekst, "
                f"{stats[3]}/{stats[0]} with budgetmodeltype. "
                f"Lukket: {stats[4]}/{stats[0]}. "
                f"valutakode: {stats[5]}/{stats[0]}. "
                f"omkostningsstednummer: {stats[6]}/{stats[0]}."
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_BudgetEgenskaber_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_Budget_sydax",
            "grain": "clean_code_BudgetEgenskaber (composite)",
            "has_nummer": stats[1],
            "has_tekst": stats[2],
            "has_type": stats[3],
            "lukket_count": stats[4],
            "has_valutakode": stats[5],
            "has_okosted": stats[6],
            "layer": "stg_dim (Layer 1)",
        }
    )
