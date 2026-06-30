# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Fakturalinje from MySupply (Fact Layer)

Select columns needed for fct.Fakturalinje from pre_stg_fakturalinje_mysupply.
Contains only what FctUpd_fakturalinje consumes: grain key, bk traceability,
CleanCodes for map lookups (via map.Fakturalinje), dates, measures, and currency codes.

DW_EK_Faktura is resolved through map.Fakturalinje, not directly from map.Faktura.
Invoice-level columns (for fct.Faktura) are handled by stg_fct_faktura_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Summed allowance/charges per line. Indikering separates charges
# (true/NULL -> GebyrBeloeb) from discounts (false -> RabatBeloeb);
# a single SUM would add discounts to fees.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
LAC_SUM_CTE_SQL = """\
;WITH lac_sum AS (
    SELECT
        statistic_invoice_line_id,
        SUM(CASE WHEN LOWER(FakturaUdstederGebyrKategoriIndikering) = 'false' THEN 0
                 ELSE ISNULL(TRY_CAST(FakturaUdstederGebyrKategoriTotal AS DECIMAL(18,4)), 0) END
        ) AS GebyrBeloeb,
        SUM(CASE WHEN LOWER(FakturaUdstederGebyrKategoriIndikering) = 'false'
                 THEN ISNULL(TRY_CAST(FakturaUdstederGebyrKategoriTotal AS DECIMAL(18,4)), 0)
                 ELSE 0 END
        ) AS RabatBeloeb
    FROM [ext].[statistic_invoice_line_allowance_charge_mysupply]
    GROUP BY statistic_invoice_line_id
)
"""

# Measure projection used after LEFT JOIN lac_sum lac. ISNULL keeps lines
# without any allowance rows at 0, matching lines whose rows all sum to 0.
LAC_MEASURE_COLUMNS_SQL = """\
ISNULL(lac.GebyrBeloeb, 0) AS GebyrBeloeb,
                    ISNULL(lac.RabatBeloeb, 0) AS RabatBeloeb"""


@asset(
    key=AssetKey(["erp", "stg_fct_fakturalinje_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_fakturalinje_mysupply"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_daily"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "mysupply",
        "entity": "fakturalinje",
    },
    description="Select columns needed for fct.Fakturalinje from pre_stg_fakturalinje_mysupply (DW_EK_Faktura resolved via map.Fakturalinje)",
)
def stg_fct_fakturalinje_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply fakturalinje fact table.

    Selects only the columns consumed by FctUpd_fakturalinje from pre_stg_fakturalinje_mysupply.
    DW_EK_Faktura is resolved through map.Fakturalinje (line grain), not direct map.Faktura join.
    Invoice-level columns (bk_Kreditor, bk_Faktura, LeverandorCVR, etc.) are handled
    by the standalone stg_fct_faktura_mysupply staging asset.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_fakturalinje_mysupply...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_fakturalinje_mysupply];")

            cursor.execute(
                LAC_SUM_CTE_SQL
                + f"""
                SELECT
                    -- Grain key
                    CleanCode_Fakturalinje,

                    -- Traceability keys (bk_ for FctUpd_fakturalinje)
                    CleanCode_FakturaLines AS bk_FakturaLines,
                    CleanCode_FakturaHeader AS bk_FakturaHeader,
                    CleanCode_FakturaMetadata AS bk_FakturaMetadata,

                    -- CleanCodes for map lookups (Fakturalinje, Kreditor, Vare, UNSPSC match)
                    CleanCode_Kreditor,
                    CleanCode_Faktura,
                    CleanCode_EAN_Regningsmodtager,
                    CleanCode_Kreditor + '_' + CleanCode_LeverandoerVarenummer AS CleanCode_Vare,

                    -- Date columns for DW_EK_Date conversions
                    FakturaDato,
                    OrdreDato,
                    BetalingsDato,

                    -- Business measures
                    FakturaNr,
                    AntalEnhederFaktureret,
                    LinjeBeloeb,
                    BasePrice_EnhedsPris,
                    HeaderFakturaTotal,
                    MetadataFakturaTotal,

                    -- Currency codes
                    LinjeBeloeb_Valuta,
                    BasePrice_EnhedsPris_Valuta,
                    FakturaValutaKode,

                    -- Summed allowance/charges (detail on dim.Fakturalinje as '#'-concat)
                    {LAC_MEASURE_COLUMNS_SQL}

                INTO [stg].[fct_fakturalinje_mysupply]
                FROM (
                    SELECT
                        statistic_invoice_line_id,
                        CleanCode_Fakturalinje,
                        CleanCode_FakturaLines,
                        CleanCode_FakturaHeader,
                        CleanCode_FakturaMetadata,
                        CleanCode_Kreditor,
                        CleanCode_Faktura,
                        CleanCode_EAN_Regningsmodtager,
                        CleanCode_LeverandoerVarenummer,
                        FakturaDato,
                        OrdreDato,
                        BetalingsDato,
                        FakturaNr,
                        AntalEnhederFaktureret,
                        LinjeBeloeb,
                        BasePrice_EnhedsPris,
                        HeaderFakturaTotal,
                        MetadataFakturaTotal,
                        LinjeBeloeb_Valuta,
                        BasePrice_EnhedsPris_Valuta,
                        FakturaValutaKode,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Fakturalinje
                            ORDER BY
                                Header_DW_LoadTimestamp DESC,
                                Line_DW_LoadTimestamp DESC,
                                statistic_invoice_header_id DESC,
                                statistic_invoice_line_id DESC
                        ) AS rn
                    FROM [stg].[pre_fakturalinje_mysupply]
                    WHERE CleanCode_Fakturalinje IS NOT NULL
                      AND CleanCode_Fakturalinje <> ''
                      AND statistic_invoice_line_id IS NOT NULL
                ) ranked
                LEFT JOIN lac_sum lac
                    ON lac.statistic_invoice_line_id = ranked.statistic_invoice_line_id
                WHERE rn = 1
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_fakturalinje_mysupply with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_fct_fakturalinje_mysupply_PK
                ON [stg].[fct_fakturalinje_mysupply](CleanCode_Fakturalinje);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_fakturalinje_mysupply_CVR
                ON [stg].[fct_fakturalinje_mysupply](CleanCode_Kreditor);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_fakturalinje_mysupply_Vare
                ON [stg].[fct_fakturalinje_mysupply](CleanCode_Vare);
            """
            )

            context.log.info("Created indexes for map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Kreditor) AS has_kreditor,
                    COUNT(CleanCode_Vare) AS has_vare
                FROM [stg].[fct_fakturalinje_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kreditor, "
                f"{stats[2]}/{stats[0]} with CleanCode_Vare"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_fakturalinje_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_fakturalinje_mysupply",
            "grain": "CleanCode_Fakturalinje (composite: Header_Lines_Metadata)",
            "deduplication": "ROW_NUMBER() on CleanCode_Fakturalinje ORDER BY Header_DW_LoadTimestamp DESC, Line_DW_LoadTimestamp DESC",
            "has_kreditor": stats[1],
            "has_vare": stats[2],
            "layer": "stg_fct (Layer 1)",
        }
    )
