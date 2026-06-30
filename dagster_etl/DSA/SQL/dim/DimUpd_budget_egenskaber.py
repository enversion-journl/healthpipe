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
  map.BudgetEgenskaber (driver, CleanCode_BudgetEgenskaber)
  -> stg.dim_BudgetEgenskaber_sydax (attributes)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_budget_egenskaber"]),
        AssetKey(["erp", "stg_dim_budget_egenskaber_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.BudgetEgenskaber from map + add attributes from staging",
)
def dim_budget_egenskaber(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: sync from map and enrich from staging."""

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
                m.DW_EK_BudgetEgenskaber,
                m.bk_ref_BudgetmodelId,
                src.Budgetmodelnummer,
                src.Budgetmodeltekst,
                src.Budgetmodeltype,
                src.ErLukket,
                src.Valutakode,
                src.DebetKredit,
                src.Budgetkode,
                src.Aendringstype,
                src.AendringstypeBeskrivelse,
                src.BeloebstypeId,
                src.BudgetbeloebTypetekst,
                src.BudgetpostStatusId,
                src.BudgetpostStatusTekst,
                src.Omkostningsstednummer
            INTO #BudgetEgenskaber_Attributes
            FROM [stg].[dim_BudgetEgenskaber_sydax] src
            INNER JOIN [map].[BudgetEgenskaber] m
                ON m.CleanCode_BudgetEgenskaber = src.CleanCode_BudgetEgenskaber;
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_BudgetEgenskaber_Attributes
            ON #BudgetEgenskaber_Attributes (DW_EK_BudgetEgenskaber)
            """
            )
            context.log.info("Temp table #BudgetEgenskaber_Attributes created successfully")

            cursor.execute(
                """
            MERGE [dim].[BudgetEgenskaber] AS tgt
            USING #BudgetEgenskaber_Attributes AS src
            ON tgt.DW_EK_BudgetEgenskaber = src.DW_EK_BudgetEgenskaber

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.bk_ref_BudgetmodelId = src.bk_ref_BudgetmodelId,
                    tgt.Budgetmodelnummer = src.Budgetmodelnummer,
                    tgt.Budgetmodeltekst = src.Budgetmodeltekst,
                    tgt.Budgetmodeltype = src.Budgetmodeltype,
                    tgt.ErLukket = src.ErLukket,
                    tgt.Valutakode = src.Valutakode,
                    tgt.DebetKredit = src.DebetKredit,
                    tgt.Budgetkode = src.Budgetkode,
                    tgt.Aendringstype = src.Aendringstype,
                    tgt.AendringstypeBeskrivelse = src.AendringstypeBeskrivelse,
                    tgt.BeloebstypeId = src.BeloebstypeId,
                    tgt.BudgetbeloebTypetekst = src.BudgetbeloebTypetekst,
                    tgt.BudgetpostStatusId = src.BudgetpostStatusId,
                    tgt.BudgetpostStatusTekst = src.BudgetpostStatusTekst,
                    tgt.Omkostningsstednummer = src.Omkostningsstednummer,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_BudgetEgenskaber,
                    bk_ref_BudgetmodelId, Budgetmodelnummer,
                    Budgetmodeltekst, Budgetmodeltype,
                    ErLukket, Valutakode, DebetKredit,
                    Budgetkode, Aendringstype, AendringstypeBeskrivelse,
                    BeloebstypeId, BudgetbeloebTypetekst,
                    BudgetpostStatusId, BudgetpostStatusTekst,
                    Omkostningsstednummer,
                    DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_BudgetEgenskaber,
                    src.bk_ref_BudgetmodelId, src.Budgetmodelnummer,
                    src.Budgetmodeltekst, src.Budgetmodeltype,
                    src.ErLukket, src.Valutakode, src.DebetKredit,
                    src.Budgetkode, src.Aendringstype, src.AendringstypeBeskrivelse,
                    src.BeloebstypeId, src.BudgetbeloebTypetekst,
                    src.BudgetpostStatusId, src.BudgetpostStatusTekst,
                    src.Omkostningsstednummer,
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

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "dim.BudgetEgenskaber",
            "source_tables": "stg.dim_BudgetEgenskaber_sydax + map.BudgetEgenskaber",
            "scd_type": "Type 1 (update in place)",
        }
    )
