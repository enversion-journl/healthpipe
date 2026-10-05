# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: BudgetEgenskaber

Sync dimension from map + add descriptive attributes from staging.
Type 1 SCD (update in place).

Join Path:
  map.BudgetEgenskaber (driver, clean_code_BudgetEgenskaber)
  -> stg.dim_BudgetEgenskaber_sydax (attributes)
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_BudgetEgenskaber"]),
        AssetKey(["erp", "stg_dim_BudgetEgenskaber_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.BudgetEgenskaber from map + add attributes from staging",
)
def dim_BudgetEgenskaber(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: sync from map and enrich from staging."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "BudgetEgenskaber", dagster_run_id=context.run_id, mode=config.mode)  # fmt: skip

    logged_cp: str | None = None
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting BudgetEgenskaber dimension sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[BudgetEgenskaber]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} BudgetEgenskaber entries to sync")

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #BudgetEgenskaber_Attributes;

                SELECT
                    m.dw_ek_BudgetEgenskaber,
                    m.bk_ref_budgetmodel_id,
                    src.budgetmodelnummer,
                    src.budgetmodeltekst,
                    src.budgetmodeltype,
                    src.er_lukket,
                    src.valutakode,
                    src.debet_kredit,
                    src.budgetkode,
                    src.aendringstype,
                    src.aendringstype_beskrivelse,
                    src.beloebstype_id,
                    src.budgetbeloeb_typetekst,
                    src.budgetpost_status_id,
                    src.budgetpost_status_tekst,
                    src.omkostningsstednummer
                INTO #BudgetEgenskaber_Attributes
                FROM [stg].[dim_BudgetEgenskaber_sydax] src
                INNER JOIN [map].[BudgetEgenskaber] m
                    ON m.clean_code_BudgetEgenskaber = src.clean_code_BudgetEgenskaber;
            """
                )

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_BudgetEgenskaber_Attributes
                ON #BudgetEgenskaber_Attributes (dw_ek_BudgetEgenskaber)
                """
                )
                context.log.info("Temp table #BudgetEgenskaber_Attributes created successfully")

                cursor.execute(
                    """
                MERGE [dim].[BudgetEgenskaber] AS tgt
                USING #BudgetEgenskaber_Attributes AS src
                ON tgt.dw_ek_BudgetEgenskaber = src.dw_ek_BudgetEgenskaber

                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.bk_ref_budgetmodel_id = src.bk_ref_budgetmodel_id,
                        tgt.budgetmodelnummer = src.budgetmodelnummer,
                        tgt.budgetmodeltekst = src.budgetmodeltekst,
                        tgt.budgetmodeltype = src.budgetmodeltype,
                        tgt.er_lukket = src.er_lukket,
                        tgt.valutakode = src.valutakode,
                        tgt.debet_kredit = src.debet_kredit,
                        tgt.budgetkode = src.budgetkode,
                        tgt.aendringstype = src.aendringstype,
                        tgt.aendringstype_beskrivelse = src.aendringstype_beskrivelse,
                        tgt.beloebstype_id = src.beloebstype_id,
                        tgt.budgetbeloeb_typetekst = src.budgetbeloeb_typetekst,
                        tgt.budgetpost_status_id = src.budgetpost_status_id,
                        tgt.budgetpost_status_tekst = src.budgetpost_status_tekst,
                        tgt.omkostningsstednummer = src.omkostningsstednummer,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_BudgetEgenskaber,
                        bk_ref_budgetmodel_id, budgetmodelnummer,
                        budgetmodeltekst, budgetmodeltype,
                        er_lukket, valutakode, debet_kredit,
                        budgetkode, aendringstype, aendringstype_beskrivelse,
                        beloebstype_id, budgetbeloeb_typetekst,
                        budgetpost_status_id, budgetpost_status_tekst,
                        omkostningsstednummer,
                        dw_timestamp_insert
                    )
                    VALUES (
                        src.dw_ek_BudgetEgenskaber,
                        src.bk_ref_budgetmodel_id, src.budgetmodelnummer,
                        src.budgetmodeltekst, src.budgetmodeltype,
                        src.er_lukket, src.valutakode, src.debet_kredit,
                        src.budgetkode, src.aendringstype, src.aendringstype_beskrivelse,
                        src.beloebstype_id, src.budgetbeloeb_typetekst,
                        src.budgetpost_status_id, src.budgetpost_status_tekst,
                        src.omkostningsstednummer,
                        CAST(SYSDATETIME() AS datetime2(0))
                    );
            """
                )

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #BudgetEgenskaber_Attributes")
                cursor.execute("UPDATE STATISTICS [dim].[BudgetEgenskaber]")

                context.log.info(f"Dimension sync complete: {rows_affected} rows merged")
            finally:
                cursor.close()

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "BudgetEgenskaber", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "BudgetEgenskaber")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "dim.BudgetEgenskaber",
            "source_tables": "stg.dim_BudgetEgenskaber_sydax + map.BudgetEgenskaber",
            "scd_type": "Type 1 (update in place)",
            "checkpoint_after": logged_cp,
            "mode": config.mode,
        }
    )
