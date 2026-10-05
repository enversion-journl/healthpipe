# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Fakturalinje from MySupply (Dimension Layer)

Deduplicate MySupply fakturalinje data at clean_code_Fakturalinje level.
Derived from pre_stg_Fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Allowance/charges are 1:n per line, split into Rabat (Indikering='false')
# and Tillaeg (Indikering='true'/NULL). Rows with zero/null Total are excluded.
# Aarsag is a composite '{Aarsag}:{Total}' per position. Columns are '#'-delimited,
# position-aligned (ORDER BY id), '#' stripped from source values and ':' stripped
# from Aarsag so the composite stays parseable.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
LAC_DIM_CTE_SQL = """\
;WITH lac_base AS (
    SELECT *
    FROM [arc].[mysupply_statistic_invoice_line_allowance_charge]
    WHERE FakturaUdstederGebyrKategoriTotal IS NOT NULL
      AND TRY_CAST(FakturaUdstederGebyrKategoriTotal AS DECIMAL(18,4)) <> 0
),
lac_rabat AS (
    SELECT
        statistic_invoice_line_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([FakturaUdstederGebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(FakturaUdstederGebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS faktura_udsteder_gebyr_rabat_aarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS faktura_udsteder_gebyr_rabat_kategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategoriFinanskonto, ''), '#', '')
                 AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS faktura_udsteder_gebyr_rabat_finanskonto
    FROM lac_base
    WHERE LOWER(FakturaUdstederGebyrKategoriIndikering) = 'false'
    GROUP BY statistic_invoice_line_id
),
lac_tillaeg AS (
    SELECT
        statistic_invoice_line_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([FakturaUdstederGebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(FakturaUdstederGebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS faktura_udsteder_gebyr_tillaeg_aarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS faktura_udsteder_gebyr_tillaeg_kategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategoriFinanskonto, ''), '#', '')
                 AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS faktura_udsteder_gebyr_tillaeg_finanskonto
    FROM lac_base
    WHERE LOWER(FakturaUdstederGebyrKategoriIndikering) <> 'false'
       OR FakturaUdstederGebyrKategoriIndikering IS NULL
    GROUP BY statistic_invoice_line_id
)
"""


@asset(
    key=AssetKey(["erp", "stg_dim_Fakturalinje_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Fakturalinje_mysupply"]),
        AssetKey(["arc_mysupply_statistic_invoice_line_allowance_charge"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "mysupply",
        "entity": "fakturalinje",
    },
    description="Deduplicate MySupply fakturalinje at clean_code_Fakturalinje level for dim.Fakturalinje",
)
def stg_dim_Fakturalinje_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply fakturalinje dimension data.

    Deduplicates pre_stg_Fakturalinje_mysupply at clean_code_Fakturalinje grain,
    keeping the most recent row per CleanCode.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info(
                "Creating staging table stg.dim_Fakturalinje_mysupply from base layer..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Fakturalinje_mysupply];")

            cursor.execute(
                LAC_DIM_CTE_SQL
                + """
                SELECT
                    ranked.clean_code_Fakturalinje,
                    ranked.clean_code_FakturaHeader,
                    ranked.clean_code_FakturaLines,
                    ranked.clean_code_FakturaMetadata,
                    ranked.clean_code_FakturaHeader AS bk_faktura_header,
                    ranked.clean_code_FakturaLines AS bk_faktura_lines,
                    ranked.clean_code_FakturaMetadata AS bk_faktura_metadata,
                    ranked.item_faktura_udsteder_vare_nr,
                    ranked.item_faktura_udsteder_vare_navn,
                    ranked.faktura_linje_nr,
                    ranked.antal_enheder_faktureret_type,
                    ranked.item_faktura_udstederens_vare_nr,
                    ranked.item_faktura_udstederens_vare_nr_type,
                    ranked.base_price_enheds_stoerrelse,
                    ranked.base_price_enheds_stoerrelse_type,
                    ranked.item_faktura_udsteder_vare_navn_kort,
                    ranked.line_dimensions_konto_streng,
                    ranked.leverance_krav,
                    ranked.leverings_dato_2,
                    ranked.item_kundens_vare_nr,
                    ranked.item_kundens_vare_nr_type,
                    ranked.item_kundens_vare_beskrivelse,
                    ranked.item_faktura_udstederens_vare_beskrivelse,
                    ranked.item_fabrikantens_vare_nr,
                    ranked.item_fabrikantens_vare_nr_type,
                    ranked.item_fabrikantens_vare_beskrivelse,
                    ranked.item_standardiseret_vare_nr,
                    ranked.item_standardiseret_vare_nr_type,
                    ranked.item_standardiseret_vare_beskrivelse,
                    ranked.item_vare_klassifikation,
                    ranked.item_vare_klassifikation_type,
                    ranked.item_afgifts_kategori,
                    ranked.item_afgifts_kategori_procent,
                    lac_r.faktura_udsteder_gebyr_rabat_aarsag,
                    lac_r.faktura_udsteder_gebyr_rabat_kategori,
                    lac_r.faktura_udsteder_gebyr_rabat_finanskonto,
                    lac_t.faktura_udsteder_gebyr_tillaeg_aarsag,
                    lac_t.faktura_udsteder_gebyr_tillaeg_kategori,
                    lac_t.faktura_udsteder_gebyr_tillaeg_finanskonto
                INTO [stg].[dim_Fakturalinje_mysupply]
                FROM (
                    SELECT
                        clean_code_Fakturalinje,
                        clean_code_FakturaHeader,
                        clean_code_FakturaLines,
                        clean_code_FakturaMetadata,
                        item_faktura_udsteder_vare_nr,
                        item_faktura_udsteder_vare_navn,
                        faktura_linje_nr,
                        antal_enheder_faktureret_type,
                        item_faktura_udstederens_vare_nr,
                        item_faktura_udstederens_vare_nr_type,
                        [base_price_enheds_stoerrelse],
                        [base_price_enheds_stoerrelse_type],
                        item_faktura_udsteder_vare_navn_kort,
                        line_dimensions_konto_streng,
                        leverance_krav,
                        leverings_dato_2,
                        item_kundens_vare_nr,
                        item_kundens_vare_nr_type,
                        item_kundens_vare_beskrivelse,
                        item_faktura_udstederens_vare_beskrivelse,
                        item_fabrikantens_vare_nr,
                        item_fabrikantens_vare_nr_type,
                        item_fabrikantens_vare_beskrivelse,
                        item_standardiseret_vare_nr,
                        item_standardiseret_vare_nr_type,
                        item_standardiseret_vare_beskrivelse,
                        item_vare_klassifikation,
                        item_vare_klassifikation_type,
                        item_afgifts_kategori,
                        item_afgifts_kategori_procent,
                        statistic_invoice_line_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY clean_code_Fakturalinje
                            ORDER BY header_dw_timestamp_update DESC
                        ) AS rn
                    FROM [stg].[pre_Fakturalinje_mysupply]
                    WHERE clean_code_Fakturalinje IS NOT NULL
                ) ranked
                LEFT JOIN lac_rabat lac_r
                    ON lac_r.statistic_invoice_line_id = ranked.statistic_invoice_line_id
                LEFT JOIN lac_tillaeg lac_t
                    ON lac_t.statistic_invoice_line_id = ranked.statistic_invoice_line_id
                WHERE ranked.rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_Fakturalinje_mysupply with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_fakturalinje_mysupply_CleanCode
                ON [stg].[dim_Fakturalinje_mysupply](clean_code_Fakturalinje);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(item_faktura_udsteder_vare_navn) AS has_vare_navn,
                    COUNT(faktura_udsteder_gebyr_rabat_aarsag) AS has_gebyr_rabat,
                    COUNT(faktura_udsteder_gebyr_tillaeg_aarsag) AS has_gebyr_tillaeg
                FROM [stg].[dim_Fakturalinje_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with VareNavn, "
                f"{stats[2]}/{stats[0]} with GebyrRabat, "
                f"{stats[3]}/{stats[0]} with GebyrTillaeg"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Fakturalinje_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_Fakturalinje_mysupply + arc.mysupply_statistic_invoice_line_allowance_charge",
            "deduplication": "ROW_NUMBER() on clean_code_Fakturalinje",
            "has_vare_navn": stats[1],
            "has_gebyr_rabat": stats[2],
            "has_gebyr_tillaeg": stats[3],
            "layer": "dim (Layer 1)",
        }
    )
