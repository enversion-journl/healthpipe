# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Budget

Fact table for budget postings with DW_EK lookups from maps.
Grain: DW_EK_Budget (surrogate key from map.Budget, resolved via CleanCode_Budget)

Reads from stg.fct_budget_sydax (Layer 1 staging) which contains
precomputed date keys, typed measures, and the grain contract
(one row per bk_ref_LedgerbudgetId).

This asset is responsible only for map lookups + MERGE + diagnostics.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_facts",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_budget_sydax"]),
        "map_budget",
        "map_budget_egenskaber",
        "map_kontoplan",
        "map_okosted",
        "dim_budget_egenskaber",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "budget",
    },
    description=(
        "Fact table for budget postings - reads precomputed staging columns "
        "and looks up DW_EK_Budget, DW_EK_BudgetEgenskaber, DW_EK_Kontoplan, "
        "DW_EK_Okosted from maps. Grain: DW_EK_Budget."
    ),
)
def fct_budget(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map lookups + MERGE into fct.Budget. All conversions done in staging."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting budget fact table load with DW_EK lookups...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_budget_sydax]")
            staging_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {staging_count}")

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Budget_Facts;

            SELECT
                src.bk_ref_LedgerbudgetId,

                b_map.DW_EK_Budget,
                COALESCE(be_map.DW_EK_BudgetEgenskaber, -1) AS DW_EK_BudgetEgenskaber,
                COALESCE(konto_map.DW_EK_Kontoplan, -1) AS DW_EK_Kontoplan,
                COALESCE(oko_map.DW_EK_Okosted, -1) AS DW_EK_Okosted,

                src.DW_EK_Dato_Budget,
                src.DW_EK_Dato_BudgetAdopted,
                src.DW_EK_Dato_BudgetOprettet,
                src.DW_EK_Dato_BudgetAendret,

                src.Budgetbeloeb,
                src.BudgetbeloebValuta,

                src.DW_haendelsestidspunkt

            INTO #Budget_Facts
            FROM [stg].[fct_budget_sydax] src

            INNER JOIN [map].[Budget] b_map
                ON b_map.CleanCode_Budget = src.CleanCode_Budget
                AND src.CleanCode_Budget IS NOT NULL
                AND src.CleanCode_Budget <> ''

            LEFT JOIN [map].[BudgetEgenskaber] be_map
                ON be_map.CleanCode_BudgetEgenskaber = src.CleanCode_BudgetEgenskaber
                AND src.CleanCode_BudgetEgenskaber IS NOT NULL
                AND src.CleanCode_BudgetEgenskaber <> ''

            LEFT JOIN [map].[Kontoplan] konto_map
                ON konto_map.CleanCode_Kontoplan = src.CleanCode_Kontoplan
                AND src.CleanCode_Kontoplan IS NOT NULL
                AND src.CleanCode_Kontoplan <> ''

            LEFT JOIN [map].[Okosted] oko_map
                ON oko_map.CleanCode_Okosted = src.Omkostningsstednummer
               AND src.Omkostningsstednummer IS NOT NULL
               AND src.Omkostningsstednummer <> '';
        """
            )

            cursor.execute(
                """
            SELECT COUNT(*) FROM #Budget_Facts WHERE DW_EK_Budget IS NULL
            """
            )
            null_ek_count = cursor.fetchone()[0]
            if null_ek_count > 0:
                context.log.warning(
                    f"{null_ek_count} rows have NULL DW_EK_Budget after INNER JOIN "
                    "(should not happen - indicates map.Budget is missing entries)"
                )

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_source_rows,
                COUNT(DISTINCT DW_EK_Budget) AS distinct_ids
            FROM #Budget_Facts
        """
            )
            source_stats = cursor.fetchone()
            duplicate_source_rows = source_stats[0] - source_stats[1]
            if duplicate_source_rows > 0:
                message = (
                    "Duplicate grain violation in fct_budget: "
                    f"{duplicate_source_rows} duplicate DW_EK_Budget rows found in #Budget_Facts. "
                    "Expected one row per DW_EK_Budget."
                )
                context.log.error(message)
                raise ValueError(message)

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Budget_Facts
            ON #Budget_Facts (DW_EK_Budget)
            """
            )

            cursor.execute(
                """
            MERGE [fct].[Budget] AS tgt
            USING #Budget_Facts AS src
            ON tgt.DW_EK_Budget = src.DW_EK_Budget

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.bk_ref_LedgerbudgetId = src.bk_ref_LedgerbudgetId,
                    tgt.DW_EK_BudgetEgenskaber = src.DW_EK_BudgetEgenskaber,
                    tgt.DW_EK_Kontoplan = src.DW_EK_Kontoplan,
                    tgt.DW_EK_Okosted = src.DW_EK_Okosted,
                    tgt.DW_EK_Dato_Budget = src.DW_EK_Dato_Budget,
                    tgt.DW_EK_Dato_BudgetAdopted = src.DW_EK_Dato_BudgetAdopted,
                    tgt.DW_EK_Dato_BudgetOprettet = src.DW_EK_Dato_BudgetOprettet,
                    tgt.DW_EK_Dato_BudgetAendret = src.DW_EK_Dato_BudgetAendret,
                    tgt.Budgetbeloeb = src.Budgetbeloeb,
                    tgt.BudgetbeloebValuta = src.BudgetbeloebValuta,
                    tgt.DW_haendelsestidspunkt = src.DW_haendelsestidspunkt,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Budget, bk_ref_LedgerbudgetId,
                    DW_EK_BudgetEgenskaber, DW_EK_Kontoplan, DW_EK_Okosted,
                    DW_EK_Dato_Budget, DW_EK_Dato_BudgetAdopted,
                    DW_EK_Dato_BudgetOprettet, DW_EK_Dato_BudgetAendret,
                    Budgetbeloeb, BudgetbeloebValuta,
                    DW_haendelsestidspunkt, DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_Budget, src.bk_ref_LedgerbudgetId,
                    src.DW_EK_BudgetEgenskaber, src.DW_EK_Kontoplan, src.DW_EK_Okosted,
                    src.DW_EK_Dato_Budget, src.DW_EK_Dato_BudgetAdopted,
                    src.DW_EK_Dato_BudgetOprettet, src.DW_EK_Dato_BudgetAendret,
                    src.Budgetbeloeb, src.BudgetbeloebValuta,
                    src.DW_haendelsestidspunkt, CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_facts,
                SUM(CASE WHEN DW_EK_BudgetEgenskaber = -1 THEN 1 ELSE 0 END) AS missing_BudgetEgenskaber_fk,
                SUM(CASE WHEN DW_EK_Kontoplan = -1 THEN 1 ELSE 0 END) AS missing_kontoplan_fk,
                SUM(CASE WHEN DW_EK_Okosted = -1 THEN 1 ELSE 0 END) AS missing_okosted_fk
            FROM [fct].[Budget]
        """
            )
            diag = cursor.fetchone()

            context.log.info(
                f"FK Matching: Total={diag[0]}, "
                f"Missing BudgetEgenskaber={diag[1]}, "
                f"Missing Kontoplan={diag[2]}, "
                f"Missing Okosted={diag[3]}"
            )

            cursor.execute("DROP TABLE IF EXISTS #Budget_Facts")
            cursor.execute("UPDATE STATISTICS [fct].[Budget]")

            context.log.info(f"Fact table load complete: {rows_affected} rows processed")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Budget",
            "grain": "DW_EK_Budget",
            "staging_source": "stg.fct_budget_sydax (Layer 1)",
            "dw_ek_lookups": "Budget (PK), BudgetEgenskaber, Kontoplan, Okosted",
            "duplicate_source_rows": duplicate_source_rows,
        }
    )
