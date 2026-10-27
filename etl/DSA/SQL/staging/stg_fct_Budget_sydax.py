# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Budget from SydAX (Fact Layer)

Select columns needed for fct_Budget from stg.pre_Budget_sydax.
Intermediate staging table for the fact table load.

Grain contract: one row per bk_ref_ledgerbudget_id (deterministic dedup applied here).
All type conversions (date keys, measures) are performed in this layer
so the downstream fact asset only needs map lookups + MERGE.

Includes okosted_kode and clean_code_Kontoplan for map lookups in FctUpd
(neither is persisted to fct.Budget -- only their resolved DW_EKs are).
omkostningsstednummer is also carried along, raw, for debugging.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.CodeSystem.okosted.okosted import LEVEL_SUFFIX, MAX_CODE_LENGTH
from DSA.SQL.common.dato_dimension import dato_key, dato_tid


@asset(
    key=AssetKey(["erp", "stg_fct_Budget_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Budget_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "budget",
    },
    description=(
        "Select columns needed for fct_Budget from stg.pre_Budget_sydax. "
        "Grain: bk_ref_ledgerbudget_id."
    ),
)
def stg_fct_Budget_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Staging for SydAX budget fact table.

    Enforces grain (one row per bk_ref_ledgerbudget_id) via deterministic dedup and
    performs all type conversions so the fact layer stays focused on map lookups.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Creating staging table stg.fct_Budget_sydax...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_Budget_sydax];")

            cursor.execute(
                f"""
                ;WITH grain_dedup AS (
                    SELECT
                        b.ref_LedgerbudgetId,
                        b.clean_code_Budget,
                        b.clean_code_BudgetEgenskaber,
                        b.clean_code_Kontoplan,
                        b.bk_finanskonto_id,
                        b.omkostningsstednummer,
                        b.omkostningssted_niveau,
                        b.Budgetdato,
                        b.BudgetAdoptedDate,
                        b.Oprettelsestidspunkt,
                        b.SidstAendretTidspunkt,
                        b.budgetbeloeb,
                        b.budgetbeloeb_valuta,
                        b.dw_timestamp_update,
                        ROW_NUMBER() OVER (
                            PARTITION BY b.ref_LedgerbudgetId
                            ORDER BY b.dw_timestamp_update DESC
                        ) AS grain_rn
                    FROM [stg].[pre_Budget_sydax] b
                    WHERE b.ref_LedgerbudgetId IS NOT NULL
                )

                SELECT
                    CAST(g.ref_LedgerbudgetId AS NVARCHAR(100)) AS bk_ref_ledgerbudget_id,
                    g.clean_code_Budget,
                    g.clean_code_BudgetEgenskaber,
                    g.clean_code_Kontoplan,
                    g.bk_finanskonto_id,
                    g.omkostningsstednummer,
                    CAST(CASE
                        WHEN g.omkostningsstednummer IS NOT NULL
                        THEN CONCAT(LTRIM(RTRIM(g.omkostningsstednummer)), '{LEVEL_SUFFIX}', g.omkostningssted_niveau)
                    END AS NVARCHAR({MAX_CODE_LENGTH})) AS okosted_kode,

                    {dato_key('g.Budgetdato')} AS dw_ek_Dato_budget,
                    {dato_tid('g.Budgetdato')} AS dato_tid_budget,
                    {dato_key('g.BudgetAdoptedDate')} AS dw_ek_Dato_budget_adopted,
                    {dato_tid('g.BudgetAdoptedDate')} AS dato_tid_budget_adopted,
                    {dato_key('g.Oprettelsestidspunkt')} AS dw_ek_Dato_budget_oprettet,
                    {dato_tid('g.Oprettelsestidspunkt')} AS dato_tid_budget_oprettet,
                    {dato_key('g.SidstAendretTidspunkt')} AS dw_ek_Dato_budget_aendret,
                    {dato_tid('g.SidstAendretTidspunkt')} AS dato_tid_budget_aendret,

                    TRY_CONVERT(DECIMAL(30,12), g.budgetbeloeb) AS budgetbeloeb,
                    TRY_CONVERT(DECIMAL(30,12), g.budgetbeloeb_valuta) AS budgetbeloeb_valuta,

                    TRY_CONVERT(DATETIME2, g.Oprettelsestidspunkt) AS dw_haendelsestidspunkt

                INTO [stg].[fct_Budget_sydax]
                FROM grain_dedup g
                WHERE g.grain_rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_Budget_sydax with {row_count} rows")

            cursor.execute(
                """
                SELECT COUNT(*) FROM [stg].[pre_Budget_sydax]
                WHERE ref_LedgerbudgetId IS NOT NULL
            """
            )
            ext_count = cursor.fetchone()[0]
            duplicates_removed = ext_count - row_count
            if duplicates_removed > 0:
                context.log.warning(
                    f"Removed {duplicates_removed} duplicate ref_LedgerbudgetId rows "
                    "during grain enforcement (kept latest by dw_timestamp_update)"
                )

            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_fct_budget_sydax_PK
                ON [stg].[fct_Budget_sydax](bk_ref_ledgerbudget_id);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_budget_sydax_Kontoplan
                ON [stg].[fct_Budget_sydax](clean_code_Kontoplan);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_budget_sydax_Budget
                ON [stg].[fct_Budget_sydax](clean_code_Budget);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_budget_sydax_BudgetEgenskaber
                ON [stg].[fct_Budget_sydax](clean_code_BudgetEgenskaber);
            """
            )

            context.log.info("Created indexes for map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(clean_code_Kontoplan) AS has_cleancode_kontoplan,
                    COUNT(clean_code_Budget) AS has_cleancode_budget,
                    COUNT(clean_code_BudgetEgenskaber) AS has_cleancode_BudgetEgenskaber,
                    COUNT(omkostningsstednummer) AS has_okosted
                FROM [stg].[fct_Budget_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with clean_code_Kontoplan, "
                f"{stats[2]}/{stats[0]} with clean_code_Budget, "
                f"{stats[3]}/{stats[0]} with clean_code_BudgetEgenskaber, "
                f"{stats[4]}/{stats[0]} with omkostningsstednummer (OKOSTED)"
            )

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN dw_ek_Dato_budget <> -1 THEN 1 ELSE 0 END) AS has_budget_date,
                    SUM(CASE WHEN dw_ek_Dato_budget_adopted <> -1 THEN 1 ELSE 0 END) AS has_adopted_date,
                    SUM(CASE WHEN dw_ek_Dato_budget_oprettet <> -1 THEN 1 ELSE 0 END) AS has_oprettet_date,
                    SUM(CASE WHEN dw_ek_Dato_budget_aendret <> -1 THEN 1 ELSE 0 END) AS has_aendret_date,
                    SUM(CASE WHEN dw_haendelsestidspunkt IS NOT NULL THEN 1 ELSE 0 END) AS has_haendelse
                FROM [stg].[fct_Budget_sydax]
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
            "table": "stg.fct_Budget_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_Budget_sydax",
            "grain": "bk_ref_ledgerbudget_id",
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
