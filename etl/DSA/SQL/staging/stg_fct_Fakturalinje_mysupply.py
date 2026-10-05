# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Fakturalinje from MySupply (Fact Layer)

Select columns needed for fct.Fakturalinje from pre_stg_Fakturalinje_mysupply.
Contains only what FctUpd_fakturalinje consumes: grain key, bk traceability,
CleanCodes for map lookups (via map.Fakturalinje), dates, measures, and currency codes.

dw_ek_Faktura is resolved through map.Fakturalinje, not directly from map.Faktura.
Invoice-level columns (for fct.Faktura) are handled by stg_fct_Faktura_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Summed allowance/charges per line. Indikering separates charges
# (true/NULL -> gebyr_beloeb) from discounts (false -> rabatbeloeb);
# a single SUM would add discounts to fees.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
LAC_SUM_CTE_SQL = """\
;WITH lac_sum AS (
    SELECT
        statistic_invoice_line_id,
        SUM(CASE WHEN LOWER(FakturaUdstederGebyrKategoriIndikering) = 'false' THEN 0
                 ELSE ISNULL(TRY_CAST(FakturaUdstederGebyrKategoriTotal AS DECIMAL(18,4)), 0) END
        ) AS gebyr_beloeb,
        SUM(CASE WHEN LOWER(FakturaUdstederGebyrKategoriIndikering) = 'false'
                 THEN ISNULL(TRY_CAST(FakturaUdstederGebyrKategoriTotal AS DECIMAL(18,4)), 0)
                 ELSE 0 END
        ) AS rabatbeloeb
    FROM [arc].[mysupply_statistic_invoice_line_allowance_charge]
    GROUP BY statistic_invoice_line_id
)
"""

# Measure projection used after LEFT JOIN lac_sum lac. ISNULL keeps lines
# without any allowance rows at 0, matching lines whose rows all sum to 0.
LAC_MEASURE_COLUMNS_SQL = """\
ISNULL(lac.gebyr_beloeb, 0) AS gebyr_beloeb,
                    ISNULL(lac.rabatbeloeb, 0) AS rabatbeloeb"""


@asset(
    key=AssetKey(["erp", "stg_fct_Fakturalinje_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Fakturalinje_mysupply"]),
        AssetKey(["arc_mysupply_statistic_invoice_line_allowance_charge"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "mysupply",
        "entity": "fakturalinje",
    },
    description="Select columns needed for fct.Fakturalinje from pre_stg_Fakturalinje_mysupply (dw_ek_Faktura resolved via map.Fakturalinje)",
)
def stg_fct_Fakturalinje_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply fakturalinje fact table.

    Selects only the columns consumed by FctUpd_fakturalinje from pre_stg_Fakturalinje_mysupply.
    dw_ek_Faktura is resolved through map.Fakturalinje (line grain), not direct map.Faktura join.
    Invoice-level columns (bk_kreditor, bk_faktura, leverandoer_cvr, etc.) are handled
    by the standalone stg_fct_Faktura_mysupply staging asset.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_Fakturalinje_mysupply...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_Fakturalinje_mysupply];")

            cursor.execute(
                LAC_SUM_CTE_SQL
                + f"""
                SELECT
                    -- Grain key
                    clean_code_Fakturalinje,

                    -- Traceability keys (bk_ for FctUpd_fakturalinje)
                    clean_code_FakturaLines AS bk_faktura_lines,
                    clean_code_FakturaHeader AS bk_faktura_header,
                    clean_code_FakturaMetadata AS bk_faktura_metadata,

                    -- CleanCodes for map lookups (Fakturalinje, Kreditor, Vare, UNSPSC match)
                    clean_code_Kreditor,
                    clean_code_Kreditor_Header AS clean_code_Kreditor_header,
                    clean_code_Faktura,
                    clean_code_EAN_Regningsmodtager AS clean_code_EAN_regningsmodtager,
                    clean_code_Kreditor + '_' + clean_code_LeverandoerVarenummer AS clean_code_Leverandoervare,

                    -- Date columns for DW_EK_Date conversions
                    FakturaDato AS faktura_dato,
                    OrdreDato AS ordre_dato,
                    BetalingsDato AS betalings_dato,

                    -- Business measures
                    faktura_nr,
                    antal_enheder_faktureret,
                    linje_beloeb,
                    BasePrice_EnhedsPris AS base_price_enheds_pris,
                    HeaderFakturaTotal AS header_faktura_total,
                    MetadataFakturaTotal AS metadata_faktura_total,

                    -- Currency codes
                    LinjeBeloeb_Valuta AS linje_beloeb_valuta,
                    BasePrice_EnhedsPris_Valuta AS base_price_enheds_pris_valuta,
                    faktura_valuta_kode,

                    -- Summed allowance/charges (detail on dim.Fakturalinje as '#'-concat)
                    {LAC_MEASURE_COLUMNS_SQL}

                INTO [stg].[fct_Fakturalinje_mysupply]
                FROM (
                    SELECT
                        statistic_invoice_line_id,
                        clean_code_Fakturalinje,
                        clean_code_FakturaLines,
                        clean_code_FakturaHeader,
                        clean_code_FakturaMetadata,
                        clean_code_Kreditor,
                        clean_code_Kreditor_Header,
                        clean_code_Faktura,
                        clean_code_EAN_Regningsmodtager,
                        clean_code_LeverandoerVarenummer,
                        FakturaDato,
                        OrdreDato,
                        BetalingsDato,
                        faktura_nr,
                        antal_enheder_faktureret,
                        linje_beloeb,
                        BasePrice_EnhedsPris,
                        HeaderFakturaTotal,
                        MetadataFakturaTotal,
                        LinjeBeloeb_Valuta,
                        BasePrice_EnhedsPris_Valuta,
                        faktura_valuta_kode,
                        ROW_NUMBER() OVER (
                            PARTITION BY clean_code_Fakturalinje
                            ORDER BY
                                header_dw_timestamp_update DESC,
                                Line_DW_Timestamp_Update DESC,
                                statistic_invoice_header_id DESC,
                                statistic_invoice_line_id DESC
                        ) AS rn
                    FROM [stg].[pre_Fakturalinje_mysupply]
                    WHERE clean_code_Fakturalinje IS NOT NULL
                      AND clean_code_Fakturalinje <> ''
                      AND statistic_invoice_line_id IS NOT NULL
                ) ranked
                LEFT JOIN lac_sum lac
                    ON lac.statistic_invoice_line_id = ranked.statistic_invoice_line_id
                WHERE rn = 1
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_Fakturalinje_mysupply with {row_count} rows")

            # Grain guard stays NONCLUSTERED on the heap: a clustered index on the
            # wide NVARCHAR(500) key rewrites all ~67M rows and becomes the row
            # locator of every other index (hours of build time, no read benefit —
            # downstream consumers full-scan this table).
            cursor.execute(
                """
                CREATE UNIQUE NONCLUSTERED INDEX IX_fct_fakturalinje_mysupply_PK
                ON [stg].[fct_Fakturalinje_mysupply](clean_code_Fakturalinje);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_fakturalinje_mysupply_CVR
                ON [stg].[fct_Fakturalinje_mysupply](clean_code_Kreditor);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_fakturalinje_mysupply_Vare
                ON [stg].[fct_Fakturalinje_mysupply](clean_code_Leverandoervare);
            """
            )

            context.log.info("Created indexes for map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(clean_code_Kreditor) AS has_kreditor,
                    COUNT(clean_code_Leverandoervare) AS has_vare
                FROM [stg].[fct_Fakturalinje_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with clean_code_Kreditor, "
                f"{stats[2]}/{stats[0]} with clean_code_Leverandoervare"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_Fakturalinje_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_Fakturalinje_mysupply",
            "grain": "clean_code_Fakturalinje (composite: Header_Lines_Metadata)",
            "deduplication": "ROW_NUMBER() on clean_code_Fakturalinje ORDER BY header_dw_timestamp_update DESC, Line_DW_Timestamp_Update DESC",
            "has_kreditor": stats[1],
            "has_vare": stats[2],
            "layer": "stg_fct (Layer 1)",
        }
    )
