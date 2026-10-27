# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Budget

Fact table for budget postings with DW_EK lookups from maps.
Grain: dw_ek_Budget (surrogate key from map.Budget, resolved via clean_code_Budget)

Reads from stg.fct_Budget_sydax (Layer 1 staging) which contains
precomputed date keys, typed measures, and the grain contract
(one row per bk_ref_ledgerbudget_id).

This asset is responsible only for map lookups + MERGE + diagnostics.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_Budget_sydax"]),
        "map_Budget",
        "map_BudgetEgenskaber",
        "map_Kontoplan",
        "map_Okosted",
        "dim_BudgetEgenskaber",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "budget",
    },
    description=(
        "Fact table for budget postings - reads precomputed staging columns "
        "and looks up dw_ek_Budget, dw_ek_BudgetEgenskaber, dw_ek_Kontoplan, "
        "dw_ek_Okosted from maps. Grain: dw_ek_Budget."
    ),
)
def fct_Budget(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map lookups + MERGE into fct.Budget. All conversions done in staging."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("fct", "Budget", dagster_run_id=context.run_id, mode=config.mode)
    logged_cp: str | None = None

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting budget fact table load with DW_EK lookups...")

                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_Budget_sydax]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging count: {staging_count}")

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Budget_Facts;

                SELECT
                    src.bk_ref_ledgerbudget_id,

                    b_map.dw_ek_Budget,
                    COALESCE(be_map.dw_ek_BudgetEgenskaber, -1) AS dw_ek_BudgetEgenskaber,
                    COALESCE(konto_map.dw_ek_Kontoplan, -1) AS dw_ek_Kontoplan,
                    COALESCE(oko_map.dw_ek_Okosted, -1) AS dw_ek_Okosted,

                    src.dw_ek_Dato_budget,
                    src.dato_tid_budget,
                    src.dw_ek_Dato_budget_adopted,
                    src.dato_tid_budget_adopted,
                    src.dw_ek_Dato_budget_oprettet,
                    src.dato_tid_budget_oprettet,
                    src.dw_ek_Dato_budget_aendret,
                    src.dato_tid_budget_aendret,

                    src.budgetbeloeb,
                    src.budgetbeloeb_valuta,

                    src.dw_haendelsestidspunkt

                INTO #Budget_Facts
                FROM [stg].[fct_Budget_sydax] src

                INNER JOIN [map].[Budget] b_map
                    ON b_map.clean_code_Budget = src.clean_code_Budget
                    AND src.clean_code_Budget IS NOT NULL
                    AND src.clean_code_Budget <> ''

                LEFT JOIN [map].[BudgetEgenskaber] be_map
                    ON be_map.clean_code_BudgetEgenskaber = src.clean_code_BudgetEgenskaber
                    AND src.clean_code_BudgetEgenskaber IS NOT NULL
                    AND src.clean_code_BudgetEgenskaber <> ''

                LEFT JOIN [map].[Kontoplan] konto_map
                    ON konto_map.clean_code_Kontoplan = src.clean_code_Kontoplan
                    AND src.clean_code_Kontoplan IS NOT NULL
                    AND src.clean_code_Kontoplan <> ''

                LEFT JOIN [map].[Okosted] oko_map
                    ON oko_map.clean_code_Okosted = src.okosted_kode;
            """
                )

                cursor.execute(
                    """
                SELECT COUNT(*) FROM #Budget_Facts WHERE dw_ek_Budget IS NULL
                """
                )
                null_ek_count = cursor.fetchone()[0]
                if null_ek_count > 0:
                    context.log.warning(
                        f"{null_ek_count} rows have NULL dw_ek_Budget after INNER JOIN "
                        "(should not happen - indicates map.Budget is missing entries)"
                    )

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_source_rows,
                    COUNT(DISTINCT dw_ek_Budget) AS distinct_ids
                FROM #Budget_Facts
            """
                )
                source_stats = cursor.fetchone()
                duplicate_source_rows = source_stats[0] - source_stats[1]
                if duplicate_source_rows > 0:
                    message = (
                        "Duplicate grain violation in fct_Budget: "
                        f"{duplicate_source_rows} duplicate dw_ek_Budget rows found in #Budget_Facts. "
                        "Expected one row per dw_ek_Budget."
                    )
                    context.log.error(message)
                    raise ValueError(message)

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Budget_Facts
                ON #Budget_Facts (dw_ek_Budget)
                """
                )

                cursor.execute(
                    """
                MERGE [fct].[Budget] AS tgt
                USING #Budget_Facts AS src
                ON tgt.dw_ek_Budget = src.dw_ek_Budget

                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.bk_ref_ledgerbudget_id = src.bk_ref_ledgerbudget_id,
                        tgt.dw_ek_BudgetEgenskaber = src.dw_ek_BudgetEgenskaber,
                        tgt.dw_ek_Kontoplan = src.dw_ek_Kontoplan,
                        tgt.dw_ek_Okosted = src.dw_ek_Okosted,
                        tgt.dw_ek_Dato_budget = src.dw_ek_Dato_budget,
                        tgt.dato_tid_budget = src.dato_tid_budget,
                        tgt.dw_ek_Dato_budget_adopted = src.dw_ek_Dato_budget_adopted,
                        tgt.dato_tid_budget_adopted = src.dato_tid_budget_adopted,
                        tgt.dw_ek_Dato_budget_oprettet = src.dw_ek_Dato_budget_oprettet,
                        tgt.dato_tid_budget_oprettet = src.dato_tid_budget_oprettet,
                        tgt.dw_ek_Dato_budget_aendret = src.dw_ek_Dato_budget_aendret,
                        tgt.dato_tid_budget_aendret = src.dato_tid_budget_aendret,
                        tgt.budgetbeloeb = src.budgetbeloeb,
                        tgt.budgetbeloeb_valuta = src.budgetbeloeb_valuta,
                        tgt.dw_haendelsestidspunkt = src.dw_haendelsestidspunkt,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Budget, bk_ref_ledgerbudget_id,
                        dw_ek_BudgetEgenskaber, dw_ek_Kontoplan, dw_ek_Okosted,
                        dw_ek_Dato_budget, dato_tid_budget,
                        dw_ek_Dato_budget_adopted, dato_tid_budget_adopted,
                        dw_ek_Dato_budget_oprettet, dato_tid_budget_oprettet,
                        dw_ek_Dato_budget_aendret, dato_tid_budget_aendret,
                        budgetbeloeb, budgetbeloeb_valuta,
                        dw_haendelsestidspunkt, dw_timestamp_insert
                    )
                    VALUES (
                        src.dw_ek_Budget, src.bk_ref_ledgerbudget_id,
                        src.dw_ek_BudgetEgenskaber, src.dw_ek_Kontoplan, src.dw_ek_Okosted,
                        src.dw_ek_Dato_budget, src.dato_tid_budget,
                        src.dw_ek_Dato_budget_adopted, src.dato_tid_budget_adopted,
                        src.dw_ek_Dato_budget_oprettet, src.dato_tid_budget_oprettet,
                        src.dw_ek_Dato_budget_aendret, src.dato_tid_budget_aendret,
                        src.budgetbeloeb, src.budgetbeloeb_valuta,
                        src.dw_haendelsestidspunkt, CAST(SYSDATETIME() AS datetime2(0))
                    );
            """
                )

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_facts,
                    SUM(CASE WHEN dw_ek_BudgetEgenskaber = -1 THEN 1 ELSE 0 END) AS missing_BudgetEgenskaber_fk,
                    SUM(CASE WHEN dw_ek_Kontoplan = -1 THEN 1 ELSE 0 END) AS missing_kontoplan_fk,
                    SUM(CASE WHEN dw_ek_Okosted = -1 THEN 1 ELSE 0 END) AS missing_okosted_fk
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

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Budget", mode=config.mode)
            logged_cp = dfm.get_checkpoint("fct", "Budget")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Budget",
            "grain": "dw_ek_Budget",
            "staging_source": "stg.fct_Budget_sydax (Layer 1)",
            "dw_ek_lookups": "Budget (PK), BudgetEgenskaber, Kontoplan, Okosted",
            "duplicate_source_rows": duplicate_source_rows,
            "checkpoint_after": logged_cp,
            "mode": config.mode,
        }
    )
