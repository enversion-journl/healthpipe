# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Faktura from MySupply (Dimension Layer)

Aggregate MySupply faktura data at clean_code_Faktura level (one row per faktura).
Derived from pre_stg_Fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Allowance/charges are 1:n per header, split into Rabat (Indikering='false')
# and Tillaeg (Indikering='true'/NULL). Rows with zero/null Total are excluded.
# Aarsag is a composite '{Aarsag}:{Total}' per position. Columns are '#'-delimited,
# position-aligned (ORDER BY id), '#' stripped from source values and ':' stripped
# from Aarsag so the composite stays parseable.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
HAC_DIM_CTE_SQL = """\
;WITH hac_base AS (
    SELECT *
    FROM [arc].[mysupply_statistic_invoice_header_allowance_charge]
    WHERE GebyrKategoriTotal IS NOT NULL
      AND TRY_CAST(GebyrKategoriTotal AS DECIMAL(18,4)) <> 0
),
hac_rabat AS (
    SELECT
        statistic_invoice_header_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([GebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(GebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS gebyr_rabat_aarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS gebyr_rabat_kategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategoriFinanskonto, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS gebyr_rabat_finanskonto
    FROM hac_base
    WHERE LOWER(GebyrKategoriIndikering) = 'false'
    GROUP BY statistic_invoice_header_id
),
hac_tillaeg AS (
    SELECT
        statistic_invoice_header_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([GebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(GebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS gebyr_tillaeg_aarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS gebyr_tillaeg_kategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategoriFinanskonto, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS gebyr_tillaeg_finanskonto
    FROM hac_base
    WHERE LOWER(GebyrKategoriIndikering) <> 'false' OR GebyrKategoriIndikering IS NULL
    GROUP BY statistic_invoice_header_id
)
"""


@asset(
    key=AssetKey(["erp", "stg_dim_Faktura_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Fakturalinje_mysupply"]),
        AssetKey(["arc_mysupply_statistic_invoice_header_allowance_charge"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "mysupply",
        "entity": "faktura",
    },
    description="Aggregate MySupply faktura data at clean_code_Faktura level (one row per faktura) - derived from pre_stg_Fakturalinje_mysupply",
)
def stg_dim_Faktura_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply faktura (invoice) data.

    Aggregates pre_stg_Fakturalinje_mysupply at header key grain:
    - clean_code_Faktura as DimUpd join key (header key, unique)
    - clean_code_Kreditor carried from the winning row
    - Selects most recent record (by header_dw_timestamp_update DESC)
    - Deduplicates to one row per clean_code_Faktura (matches map.Faktura grain)
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_Faktura_mysupply from base layer...")

            # Drop existing table
            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Faktura_mysupply];")

            # Create staging table aggregating from base layer
            cursor.execute(
                HAC_DIM_CTE_SQL
                + """
                SELECT
                    ranked.clean_code_Faktura,
                    ranked.clean_code_Kreditor,
                    ranked.clean_code_Faktura AS bk_faktura,
                    ranked.clean_code_Kreditor AS bk_kreditor,
                    ranked.bk_faktura_metadata,
                    ranked.bk_faktura_header,
                    ranked.faktura_nr,
                    ranked.faktura_valuta_kode,
                    ranked.firma_navn,
                    ranked.leverandoer_cvr,
                    ranked.adresse,
                    ranked.bynavn,
                    ranked.postnummer,
                    CASE
                        WHEN ranked.MetadataType IN ('PIE', 'PIP', 'OIOUBL Invoice') THEN 'Faktura'
                        WHEN ranked.MetadataType IN ('PCM', 'PCP', 'OIOUBL CreditNote') THEN 'Kreditnota'
                        ELSE 'Ukendt'
                    END AS dokument_type,
                    ranked.regnings_modtager_id,
                    ranked.regnings_modtager_id_type,
                    ranked.koebs_ordrenummer,
                    ranked.salgs_ordrenummer,
                    ranked.kunde_identification,
                    ranked.kunde_identification_type,
                    ranked.dimension_konto_streng,
                    ranked.kundenavn,
                    ranked.kunde_vejnavn,
                    ranked.kunde_hus_nr,
                    ranked.kunde_bynavn,
                    ranked.kunde_post_nr,
                    ranked.kunde_landekode,
                    ranked.kunde_reference_id,
                    ranked.kunde_reference_navn,
                    ranked.faktura_udsteder_identifikation,
                    ranked.faktura_udsteder_identifikation_type,
                    ranked.faktura_udsteder_navn,
                    ranked.faktura_udsteder_vejnavn_vareafsendelse,
                    ranked.faktura_udsteder_hus_nr_vareafsendelse,
                    ranked.faktura_udsteder_bynavn_vareafsendelse,
                    ranked.faktura_udsteder_post_nr_vareafsendelse,
                    ranked.faktura_udsteder_land_kode_vareafsendelse,
                    ranked.faktura_udsteder_cvr_nummer,
                    ranked.faktura_udsteder_cvr_nummer_type,
                    ranked.faktura_udsteder_reference_idenrifikation,
                    ranked.faktura_udsteder_reference_navn,
                    ranked.faktura_udsteder_reference_telefon_nr,
                    ranked.faktura_udsteder_reference_e_mail,
                    ranked.kort_arts_kode,
                    ranked.betalings_type,
                    ranked.betalings_id,
                    ranked.kreditnummer,
                    ranked.kontonummer,
                    ranked.kontotype,
                    ranked.afdelings_id,
                    ranked.told_kurs,
                    ranked.told_kurs_type,
                    ranked.kunde_reference_telefon_nr,
                    ranked.kunde_reference_fax_nr,
                    ranked.kunde_reference_e_mail,
                    ranked.kunde_reference_rolle,
                    ranked.juridisk_kunde_identification,
                    ranked.juridisk_kunde_identification_type,
                    ranked.juridisk_dimension_konto_streng,
                    ranked.juridisk_kundenavn,
                    ranked.juridisk_kunde_vejnavn,
                    ranked.juridisk_kunde_hus_nr,
                    ranked.juridisk_kunde_bynavn,
                    ranked.juridisk_kunde_post_nr,
                    ranked.juridisk_kunde_landekode,
                    ranked.juridisk_kunde_reference_id,
                    ranked.juridisk_kunde_reference_navn,
                    ranked.juridisk_kunde_reference_telefon_nr,
                    ranked.juridisk_kunde_reference_fax_nr,
                    ranked.juridisk_kunde_reference_e_mail,
                    ranked.juridisk_kunde_reference_rolle,
                    ranked.faktura_udsteder_adresse_id_vareafsendelse,
                    ranked.faktura_udsteder_reference_fax_nr,
                    ranked.bank_id,
                    ranked.bank_navn,
                    ranked.kontantrabatsats,
                    ranked.strafrentesats,
                    ranked.kontant_rabat_dato,
                    ranked.start_rente_dato,
                    ranked.zero_rated_afgifts_kategori,
                    ranked.zero_rated_afgifts_kategori_procent,
                    ranked.statistic_invoice_header_id,
                    ranked.header_dw_timestamp_update,
                    hac_r.gebyr_rabat_aarsag,
                    hac_r.gebyr_rabat_kategori,
                    hac_r.gebyr_rabat_finanskonto,
                    hac_t.gebyr_tillaeg_aarsag,
                    hac_t.gebyr_tillaeg_kategori,
                    hac_t.gebyr_tillaeg_finanskonto
                INTO [stg].[dim_Faktura_mysupply]
                FROM (
                    SELECT
                        -- Header key — MUST match stg_fct_Faktura_mysupply / map.Faktura
                        -- ('CleanCode_{metadata_id}_{header_id}'). pre_Fakturalinje.clean_code_Faktura
                        -- is the faktura_nr-based BUSINESS key ('FAKTURA_..'), which is NOT unique per
                        -- header and would never join map.Faktura — do not use it as the dim key.
                        CONCAT('CleanCode_',
                            CAST(metadata_id AS NVARCHAR(50)), '_',
                            CAST(statistic_invoice_header_id AS NVARCHAR(50))
                        ) AS clean_code_Faktura,
                        -- bk columns for map.Faktura seeding — same derivation as
                        -- stg_fct_Faktura_mysupply so the map key set is identical
                        clean_code_FakturaMetadata AS bk_faktura_metadata,
                        clean_code_FakturaHeader AS bk_faktura_header,
                        clean_code_Kreditor,
                        faktura_nr,
                        faktura_valuta_kode,
                        firma_navn,
                        leverandoer_cvr,
                        adresse,
                        bynavn,
                        postnummer,
                        MetadataType,
                        regnings_modtager_id,
                        regnings_modtager_id_type,
                        koebs_ordrenummer,
                        salgs_ordrenummer,
                        kunde_identification,
                        kunde_identification_type,
                        dimension_konto_streng,
                        kundenavn,
                        kunde_vejnavn,
                        kunde_hus_nr,
                        kunde_bynavn,
                        kunde_post_nr,
                        kunde_landekode,
                        kunde_reference_id,
                        kunde_reference_navn,
                        faktura_udsteder_identifikation,
                        faktura_udsteder_identifikation_type,
                        faktura_udsteder_navn,
                        faktura_udsteder_vejnavn_vareafsendelse,
                        faktura_udsteder_hus_nr_vareafsendelse,
                        faktura_udsteder_bynavn_vareafsendelse,
                        faktura_udsteder_post_nr_vareafsendelse,
                        faktura_udsteder_land_kode_vareafsendelse,
                        faktura_udsteder_cvr_nummer,
                        faktura_udsteder_cvr_nummer_type,
                        faktura_udsteder_reference_idenrifikation,
                        faktura_udsteder_reference_navn,
                        faktura_udsteder_reference_telefon_nr,
                        faktura_udsteder_reference_e_mail,
                        kort_arts_kode,
                        betalings_type,
                        betalings_id,
                        kreditnummer,
                        kontonummer,
                        kontotype,
                        afdelings_id,
                        told_kurs,
                        told_kurs_type,
                        kunde_reference_telefon_nr,
                        kunde_reference_fax_nr,
                        kunde_reference_e_mail,
                        kunde_reference_rolle,
                        juridisk_kunde_identification,
                        juridisk_kunde_identification_type,
                        juridisk_dimension_konto_streng,
                        juridisk_kundenavn,
                        juridisk_kunde_vejnavn,
                        juridisk_kunde_hus_nr,
                        juridisk_kunde_bynavn,
                        juridisk_kunde_post_nr,
                        juridisk_kunde_landekode,
                        juridisk_kunde_reference_id,
                        juridisk_kunde_reference_navn,
                        juridisk_kunde_reference_telefon_nr,
                        juridisk_kunde_reference_fax_nr,
                        juridisk_kunde_reference_e_mail,
                        juridisk_kunde_reference_rolle,
                        faktura_udsteder_adresse_id_vareafsendelse,
                        faktura_udsteder_reference_fax_nr,
                        bank_id,
                        bank_navn,
                        kontantrabatsats,
                        strafrentesats,
                        kontant_rabat_dato,
                        start_rente_dato,
                        zero_rated_afgifts_kategori,
                        zero_rated_afgifts_kategori_procent,
                        statistic_invoice_header_id,
                        header_dw_timestamp_update,
                        ROW_NUMBER() OVER (
                            PARTITION BY metadata_id, statistic_invoice_header_id
                            ORDER BY header_dw_timestamp_update DESC, statistic_invoice_header_id DESC
                        ) AS rn
                    FROM [stg].[pre_Fakturalinje_mysupply]
                    -- Header grain, matching stg_fct_Faktura_mysupply / map.Faktura. The non-null
                    -- id filter also drops the degenerate faktura_nr-only rows (e.g. 'FAKTURA_-').
                    WHERE metadata_id IS NOT NULL
                      AND statistic_invoice_header_id IS NOT NULL
                ) ranked
                LEFT JOIN hac_rabat hac_r
                    ON hac_r.statistic_invoice_header_id = ranked.statistic_invoice_header_id
                LEFT JOIN hac_tillaeg hac_t
                    ON hac_t.statistic_invoice_header_id = ranked.statistic_invoice_header_id
                WHERE ranked.rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_Faktura_mysupply with {row_count} rows")

            # Create unique clustered index on header key
            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_faktura_mysupply_CleanCode
                ON [stg].[dim_Faktura_mysupply](clean_code_Faktura);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            # Diagnostic: Check attribute coverage
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(faktura_nr) AS has_faktura_nr,
                    COUNT(firma_navn) AS has_firma_navn,
                    COUNT(leverandoer_cvr) AS has_cvr,
                    COUNT(faktura_valuta_kode) AS has_valuta,
                    COUNT(gebyr_rabat_aarsag) AS has_gebyr_rabat,
                    COUNT(gebyr_tillaeg_aarsag) AS has_gebyr_tillaeg
                FROM [stg].[dim_Faktura_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with faktura_nr, "
                f"{stats[2]}/{stats[0]} with firma_navn, "
                f"{stats[3]}/{stats[0]} with cvr, {stats[4]}/{stats[0]} with valutakode, "
                f"{stats[5]}/{stats[0]} with GebyrRabat, {stats[6]}/{stats[0]} with GebyrTillaeg"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Faktura_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_Fakturalinje_mysupply + arc.mysupply_statistic_invoice_header_allowance_charge",
            "composite_key": "clean_code_Faktura",
            "deduplication": "ROW_NUMBER() on (clean_code_Faktura) ORDER BY header_dw_timestamp_update DESC, statistic_invoice_header_id DESC (deterministic tiebreaker)",
            "gebyr_aggregation": "STRING_AGG '#'-delimited, split by Indikering (Rabat/Tillaeg), zero/null Total excluded, Aarsag={Aarsag}:{Total}",
            "has_faktura_nr": stats[1],
            "has_firma_navn": stats[2],
            "has_cvr": stats[3],
            "has_valuta": stats[4],
            "has_gebyr_rabat": stats[5],
            "has_gebyr_tillaeg": stats[6],
            "layer": "dim (Layer 1)",
        }
    )
