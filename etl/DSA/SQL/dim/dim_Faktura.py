# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Faktura (invoice header grain)

Sync dimension from map.Faktura + add descriptive attributes from MySupply pre-staging.
Logic: Type 1 SCD - update textual information for invoices.

Uses:
- map.Faktura: dw_ek_Faktura assignment via IDENTITY (canonical key: clean_code_Faktura)
  Grain: metadata_id + header_id (header grain via map.Faktura)
- stg.dim_Faktura_mysupply: Deduplicated faktura-level attributes from MySupply

BY-DESIGN: map.Faktura is populated from a UNION of MySupply and SydAX
sources, but this dimension only enriches from MySupply staging. Map rows that
originated solely from SydAX (fct_Postering_sydax) will exist in the dimension
with NULL descriptive attributes (faktura_nr, firma_navn, etc.). This is expected:
SydAX provides only the CleanCode for EK resolution, not the textual attributes.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Faktura"]),
        AssetKey(["erp", "stg_dim_Faktura_mysupply"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Faktura from map.Faktura (header grain) + add textual attributes from MySupply pre-staging",
)
def dim_Faktura(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map.Faktura + add textual attributes."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Faktura", dagster_run_id=context.run_id, mode=config.mode)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Faktura dimension sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Faktura]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Faktura entries to sync")

                cursor.execute(
                    """
            DROP TABLE IF EXISTS #Faktura_Attributes;

            SELECT
                m.dw_ek_Faktura,
                stg.faktura_nr,
                LEFT(stg.faktura_valuta_kode, 10) AS faktura_valuta_kode,
                stg.firma_navn,
                stg.leverandoer_cvr,
                stg.adresse,
                stg.bynavn,
                stg.postnummer,
                stg.dokument_type,
                stg.regnings_modtager_id,
                stg.regnings_modtager_id_type,
                stg.koebs_ordrenummer,
                stg.salgs_ordrenummer,
                stg.kunde_identification,
                stg.kunde_identification_type,
                stg.dimension_konto_streng,
                stg.kundenavn,
                stg.kunde_vejnavn,
                stg.kunde_hus_nr,
                stg.kunde_bynavn,
                stg.kunde_post_nr,
                stg.kunde_landekode,
                stg.kunde_reference_id,
                stg.kunde_reference_navn,
                stg.faktura_udsteder_identifikation,
                stg.faktura_udsteder_identifikation_type,
                stg.faktura_udsteder_navn,
                stg.faktura_udsteder_vejnavn_vareafsendelse,
                stg.faktura_udsteder_hus_nr_vareafsendelse,
                stg.faktura_udsteder_bynavn_vareafsendelse,
                stg.faktura_udsteder_post_nr_vareafsendelse,
                stg.faktura_udsteder_land_kode_vareafsendelse,
                stg.faktura_udsteder_cvr_nummer,
                stg.faktura_udsteder_cvr_nummer_type,
                stg.faktura_udsteder_reference_idenrifikation,
                stg.faktura_udsteder_reference_navn,
                stg.faktura_udsteder_reference_telefon_nr,
                stg.faktura_udsteder_reference_e_mail,
                stg.kort_arts_kode,
                stg.betalings_type,
                stg.betalings_id,
                stg.kreditnummer,
                stg.kontonummer,
                stg.kontotype,
                stg.afdelings_id,
                stg.told_kurs,
                stg.told_kurs_type,
                stg.kunde_reference_telefon_nr,
                stg.kunde_reference_fax_nr,
                stg.kunde_reference_e_mail,
                stg.kunde_reference_rolle,
                stg.juridisk_kunde_identification,
                stg.juridisk_kunde_identification_type,
                stg.juridisk_dimension_konto_streng,
                stg.juridisk_kundenavn,
                stg.juridisk_kunde_vejnavn,
                stg.juridisk_kunde_hus_nr,
                stg.juridisk_kunde_bynavn,
                stg.juridisk_kunde_post_nr,
                stg.juridisk_kunde_landekode,
                stg.juridisk_kunde_reference_id,
                stg.juridisk_kunde_reference_navn,
                stg.juridisk_kunde_reference_telefon_nr,
                stg.juridisk_kunde_reference_fax_nr,
                stg.juridisk_kunde_reference_e_mail,
                stg.juridisk_kunde_reference_rolle,
                stg.faktura_udsteder_adresse_id_vareafsendelse,
                stg.faktura_udsteder_reference_fax_nr,
                stg.bank_id,
                stg.bank_navn,
                stg.kontantrabatsats,
                stg.strafrentesats,
                stg.kontant_rabat_dato,
                stg.start_rente_dato,
                stg.zero_rated_afgifts_kategori,
                stg.zero_rated_afgifts_kategori_procent,
                stg.gebyr_rabat_aarsag,
                stg.gebyr_rabat_kategori,
                stg.gebyr_rabat_finanskonto,
                stg.gebyr_tillaeg_aarsag,
                stg.gebyr_tillaeg_kategori,
                stg.gebyr_tillaeg_finanskonto,
                stg.statistic_invoice_header_id,
                stg.header_dw_timestamp_update

            INTO #Faktura_Attributes
            FROM [stg].[dim_Faktura_mysupply] stg
            INNER JOIN [map].[Faktura] m
                ON m.clean_code_Faktura = stg.clean_code_Faktura;
        """
                )

                cursor.execute(
                    """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Faktura_Attributes
            ON #Faktura_Attributes (dw_ek_Faktura)
            """
                )
                context.log.info("Temp table #Faktura_Attributes created")

                merge_sql = """
            MERGE [dim].[Faktura] WITH (TABLOCK) AS tgt
            USING #Faktura_Attributes AS src
            ON tgt.dw_ek_Faktura = src.dw_ek_Faktura

            WHEN MATCHED AND EXISTS (
                SELECT
                    src.faktura_nr, src.faktura_valuta_kode, src.firma_navn, src.leverandoer_cvr,
                    src.adresse, src.bynavn, src.postnummer, src.dokument_type,
                    src.regnings_modtager_id, src.regnings_modtager_id_type, src.koebs_ordrenummer, src.salgs_ordrenummer,
                    src.kunde_identification, src.kunde_identification_type, src.dimension_konto_streng,
                    src.kundenavn, src.kunde_vejnavn, src.kunde_hus_nr, src.kunde_bynavn, src.kunde_post_nr, src.kunde_landekode,
                    src.kunde_reference_id, src.kunde_reference_navn,
                    src.faktura_udsteder_identifikation, src.faktura_udsteder_identifikation_type, src.faktura_udsteder_navn,
                    src.faktura_udsteder_vejnavn_vareafsendelse, src.faktura_udsteder_hus_nr_vareafsendelse,
                    src.faktura_udsteder_bynavn_vareafsendelse, src.faktura_udsteder_post_nr_vareafsendelse,
                    src.faktura_udsteder_land_kode_vareafsendelse,
                    src.faktura_udsteder_cvr_nummer, src.faktura_udsteder_cvr_nummer_type,
                    src.faktura_udsteder_reference_idenrifikation, src.faktura_udsteder_reference_navn,
                    src.faktura_udsteder_reference_telefon_nr, src.faktura_udsteder_reference_e_mail,
                    src.kort_arts_kode, src.betalings_type, src.betalings_id, src.kreditnummer, src.kontonummer, src.kontotype,
                    src.afdelings_id, src.told_kurs, src.told_kurs_type,
                    src.kunde_reference_telefon_nr, src.kunde_reference_fax_nr, src.kunde_reference_e_mail, src.kunde_reference_rolle,
                    src.juridisk_kunde_identification, src.juridisk_kunde_identification_type,
                    src.juridisk_dimension_konto_streng,
                    src.juridisk_kundenavn, src.juridisk_kunde_vejnavn, src.juridisk_kunde_hus_nr,
                    src.juridisk_kunde_bynavn, src.juridisk_kunde_post_nr, src.juridisk_kunde_landekode,
                    src.juridisk_kunde_reference_id, src.juridisk_kunde_reference_navn,
                    src.juridisk_kunde_reference_telefon_nr, src.juridisk_kunde_reference_fax_nr,
                    src.juridisk_kunde_reference_e_mail, src.juridisk_kunde_reference_rolle,
                    src.faktura_udsteder_adresse_id_vareafsendelse, src.faktura_udsteder_reference_fax_nr,
                    src.bank_id, src.bank_navn, src.kontantrabatsats, src.strafrentesats,
                    src.kontant_rabat_dato, src.start_rente_dato,
                    src.zero_rated_afgifts_kategori, src.zero_rated_afgifts_kategori_procent,
                    src.gebyr_rabat_aarsag, src.gebyr_rabat_kategori, src.gebyr_rabat_finanskonto,
                    src.gebyr_tillaeg_aarsag, src.gebyr_tillaeg_kategori, src.gebyr_tillaeg_finanskonto,
                    src.statistic_invoice_header_id, src.header_dw_timestamp_update
                EXCEPT
                SELECT
                    tgt.faktura_nr, tgt.faktura_valuta_kode, tgt.firma_navn, tgt.leverandoer_cvr,
                    tgt.adresse, tgt.bynavn, tgt.postnummer, tgt.dokument_type,
                    tgt.regnings_modtager_id, tgt.regnings_modtager_id_type, tgt.koebs_ordrenummer, tgt.salgs_ordrenummer,
                    tgt.kunde_identification, tgt.kunde_identification_type, tgt.dimension_konto_streng,
                    tgt.kundenavn, tgt.kunde_vejnavn, tgt.kunde_hus_nr, tgt.kunde_bynavn, tgt.kunde_post_nr, tgt.kunde_landekode,
                    tgt.kunde_reference_id, tgt.kunde_reference_navn,
                    tgt.faktura_udsteder_identifikation, tgt.faktura_udsteder_identifikation_type, tgt.faktura_udsteder_navn,
                    tgt.faktura_udsteder_vejnavn_vareafsendelse, tgt.faktura_udsteder_hus_nr_vareafsendelse,
                    tgt.faktura_udsteder_bynavn_vareafsendelse, tgt.faktura_udsteder_post_nr_vareafsendelse,
                    tgt.faktura_udsteder_land_kode_vareafsendelse,
                    tgt.faktura_udsteder_cvr_nummer, tgt.faktura_udsteder_cvr_nummer_type,
                    tgt.faktura_udsteder_reference_idenrifikation, tgt.faktura_udsteder_reference_navn,
                    tgt.faktura_udsteder_reference_telefon_nr, tgt.faktura_udsteder_reference_e_mail,
                    tgt.kort_arts_kode, tgt.betalings_type, tgt.betalings_id, tgt.kreditnummer, tgt.kontonummer, tgt.kontotype,
                    tgt.afdelings_id, tgt.told_kurs, tgt.told_kurs_type,
                    tgt.kunde_reference_telefon_nr, tgt.kunde_reference_fax_nr, tgt.kunde_reference_e_mail, tgt.kunde_reference_rolle,
                    tgt.juridisk_kunde_identification, tgt.juridisk_kunde_identification_type,
                    tgt.juridisk_dimension_konto_streng,
                    tgt.juridisk_kundenavn, tgt.juridisk_kunde_vejnavn, tgt.juridisk_kunde_hus_nr,
                    tgt.juridisk_kunde_bynavn, tgt.juridisk_kunde_post_nr, tgt.juridisk_kunde_landekode,
                    tgt.juridisk_kunde_reference_id, tgt.juridisk_kunde_reference_navn,
                    tgt.juridisk_kunde_reference_telefon_nr, tgt.juridisk_kunde_reference_fax_nr,
                    tgt.juridisk_kunde_reference_e_mail, tgt.juridisk_kunde_reference_rolle,
                    tgt.faktura_udsteder_adresse_id_vareafsendelse, tgt.faktura_udsteder_reference_fax_nr,
                    tgt.bank_id, tgt.bank_navn, tgt.kontantrabatsats, tgt.strafrentesats,
                    tgt.kontant_rabat_dato, tgt.start_rente_dato,
                    tgt.zero_rated_afgifts_kategori, tgt.zero_rated_afgifts_kategori_procent,
                    tgt.gebyr_rabat_aarsag, tgt.gebyr_rabat_kategori, tgt.gebyr_rabat_finanskonto,
                    tgt.gebyr_tillaeg_aarsag, tgt.gebyr_tillaeg_kategori, tgt.gebyr_tillaeg_finanskonto,
                    tgt.statistic_invoice_header_id, tgt.header_dw_timestamp_update
            ) THEN
                UPDATE SET
                    tgt.faktura_nr = src.faktura_nr,
                    tgt.faktura_valuta_kode = src.faktura_valuta_kode,
                    tgt.firma_navn = src.firma_navn,
                    tgt.leverandoer_cvr = src.leverandoer_cvr,
                    tgt.adresse = src.adresse,
                    tgt.bynavn = src.bynavn,
                    tgt.postnummer = src.postnummer,
                    tgt.dokument_type = src.dokument_type,
                    tgt.regnings_modtager_id = src.regnings_modtager_id,
                    tgt.regnings_modtager_id_type = src.regnings_modtager_id_type,
                    tgt.koebs_ordrenummer = src.koebs_ordrenummer,
                    tgt.salgs_ordrenummer = src.salgs_ordrenummer,
                    tgt.kunde_identification = src.kunde_identification,
                    tgt.kunde_identification_type = src.kunde_identification_type,
                    tgt.dimension_konto_streng = src.dimension_konto_streng,
                    tgt.kundenavn = src.kundenavn,
                    tgt.kunde_vejnavn = src.kunde_vejnavn,
                    tgt.kunde_hus_nr = src.kunde_hus_nr,
                    tgt.kunde_bynavn = src.kunde_bynavn,
                    tgt.kunde_post_nr = src.kunde_post_nr,
                    tgt.kunde_landekode = src.kunde_landekode,
                    tgt.kunde_reference_id = src.kunde_reference_id,
                    tgt.kunde_reference_navn = src.kunde_reference_navn,
                    tgt.faktura_udsteder_identifikation = src.faktura_udsteder_identifikation,
                    tgt.faktura_udsteder_identifikation_type = src.faktura_udsteder_identifikation_type,
                    tgt.faktura_udsteder_navn = src.faktura_udsteder_navn,
                    tgt.faktura_udsteder_vejnavn_vareafsendelse = src.faktura_udsteder_vejnavn_vareafsendelse,
                    tgt.faktura_udsteder_hus_nr_vareafsendelse = src.faktura_udsteder_hus_nr_vareafsendelse,
                    tgt.faktura_udsteder_bynavn_vareafsendelse = src.faktura_udsteder_bynavn_vareafsendelse,
                    tgt.faktura_udsteder_post_nr_vareafsendelse = src.faktura_udsteder_post_nr_vareafsendelse,
                    tgt.faktura_udsteder_land_kode_vareafsendelse = src.faktura_udsteder_land_kode_vareafsendelse,
                    tgt.faktura_udsteder_cvr_nummer = src.faktura_udsteder_cvr_nummer,
                    tgt.faktura_udsteder_cvr_nummer_type = src.faktura_udsteder_cvr_nummer_type,
                    tgt.faktura_udsteder_reference_idenrifikation = src.faktura_udsteder_reference_idenrifikation,
                    tgt.faktura_udsteder_reference_navn = src.faktura_udsteder_reference_navn,
                    tgt.faktura_udsteder_reference_telefon_nr = src.faktura_udsteder_reference_telefon_nr,
                    tgt.faktura_udsteder_reference_e_mail = src.faktura_udsteder_reference_e_mail,
                    tgt.kort_arts_kode = src.kort_arts_kode,
                    tgt.betalings_type = src.betalings_type,
                    tgt.betalings_id = src.betalings_id,
                    tgt.kreditnummer = src.kreditnummer,
                    tgt.kontonummer = src.kontonummer,
                    tgt.kontotype = src.kontotype,
                    tgt.afdelings_id = src.afdelings_id,
                    tgt.told_kurs = src.told_kurs,
                    tgt.told_kurs_type = src.told_kurs_type,
                    tgt.kunde_reference_telefon_nr = src.kunde_reference_telefon_nr,
                    tgt.kunde_reference_fax_nr = src.kunde_reference_fax_nr,
                    tgt.kunde_reference_e_mail = src.kunde_reference_e_mail,
                    tgt.kunde_reference_rolle = src.kunde_reference_rolle,
                    tgt.juridisk_kunde_identification = src.juridisk_kunde_identification,
                    tgt.juridisk_kunde_identification_type = src.juridisk_kunde_identification_type,
                    tgt.juridisk_dimension_konto_streng = src.juridisk_dimension_konto_streng,
                    tgt.juridisk_kundenavn = src.juridisk_kundenavn,
                    tgt.juridisk_kunde_vejnavn = src.juridisk_kunde_vejnavn,
                    tgt.juridisk_kunde_hus_nr = src.juridisk_kunde_hus_nr,
                    tgt.juridisk_kunde_bynavn = src.juridisk_kunde_bynavn,
                    tgt.juridisk_kunde_post_nr = src.juridisk_kunde_post_nr,
                    tgt.juridisk_kunde_landekode = src.juridisk_kunde_landekode,
                    tgt.juridisk_kunde_reference_id = src.juridisk_kunde_reference_id,
                    tgt.juridisk_kunde_reference_navn = src.juridisk_kunde_reference_navn,
                    tgt.juridisk_kunde_reference_telefon_nr = src.juridisk_kunde_reference_telefon_nr,
                    tgt.juridisk_kunde_reference_fax_nr = src.juridisk_kunde_reference_fax_nr,
                    tgt.juridisk_kunde_reference_e_mail = src.juridisk_kunde_reference_e_mail,
                    tgt.juridisk_kunde_reference_rolle = src.juridisk_kunde_reference_rolle,
                    tgt.faktura_udsteder_adresse_id_vareafsendelse = src.faktura_udsteder_adresse_id_vareafsendelse,
                    tgt.faktura_udsteder_reference_fax_nr = src.faktura_udsteder_reference_fax_nr,
                    tgt.bank_id = src.bank_id,
                    tgt.bank_navn = src.bank_navn,
                    tgt.kontantrabatsats = src.kontantrabatsats,
                    tgt.strafrentesats = src.strafrentesats,
                    tgt.kontant_rabat_dato = src.kontant_rabat_dato,
                    tgt.start_rente_dato = src.start_rente_dato,
                    tgt.zero_rated_afgifts_kategori = src.zero_rated_afgifts_kategori,
                    tgt.zero_rated_afgifts_kategori_procent = src.zero_rated_afgifts_kategori_procent,
                    tgt.gebyr_rabat_aarsag = src.gebyr_rabat_aarsag,
                    tgt.gebyr_rabat_kategori = src.gebyr_rabat_kategori,
                    tgt.gebyr_rabat_finanskonto = src.gebyr_rabat_finanskonto,
                    tgt.gebyr_tillaeg_aarsag = src.gebyr_tillaeg_aarsag,
                    tgt.gebyr_tillaeg_kategori = src.gebyr_tillaeg_kategori,
                    tgt.gebyr_tillaeg_finanskonto = src.gebyr_tillaeg_finanskonto,
                    tgt.statistic_invoice_header_id = src.statistic_invoice_header_id,
                    tgt.header_dw_timestamp_update = src.header_dw_timestamp_update,
                    tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    dw_ek_Faktura, faktura_nr, faktura_valuta_kode, firma_navn, leverandoer_cvr,
                    adresse, bynavn, postnummer, dokument_type,
                    regnings_modtager_id, regnings_modtager_id_type, koebs_ordrenummer, salgs_ordrenummer,
                    kunde_identification, kunde_identification_type, dimension_konto_streng,
                    kundenavn, kunde_vejnavn, kunde_hus_nr, kunde_bynavn, kunde_post_nr, kunde_landekode,
                    kunde_reference_id, kunde_reference_navn,
                    faktura_udsteder_identifikation, faktura_udsteder_identifikation_type, faktura_udsteder_navn,
                    faktura_udsteder_vejnavn_vareafsendelse, faktura_udsteder_hus_nr_vareafsendelse,
                    faktura_udsteder_bynavn_vareafsendelse, faktura_udsteder_post_nr_vareafsendelse,
                    faktura_udsteder_land_kode_vareafsendelse,
                    faktura_udsteder_cvr_nummer, faktura_udsteder_cvr_nummer_type,
                    faktura_udsteder_reference_idenrifikation, faktura_udsteder_reference_navn,
                    faktura_udsteder_reference_telefon_nr, faktura_udsteder_reference_e_mail,
                    kort_arts_kode, betalings_type, betalings_id, kreditnummer, kontonummer, kontotype,
                    afdelings_id, told_kurs, told_kurs_type,
                    kunde_reference_telefon_nr, kunde_reference_fax_nr, kunde_reference_e_mail, kunde_reference_rolle,
                    juridisk_kunde_identification, juridisk_kunde_identification_type,
                    juridisk_dimension_konto_streng,
                    juridisk_kundenavn, juridisk_kunde_vejnavn, juridisk_kunde_hus_nr,
                    juridisk_kunde_bynavn, juridisk_kunde_post_nr, juridisk_kunde_landekode,
                    juridisk_kunde_reference_id, juridisk_kunde_reference_navn,
                    juridisk_kunde_reference_telefon_nr, juridisk_kunde_reference_fax_nr,
                    juridisk_kunde_reference_e_mail, juridisk_kunde_reference_rolle,
                    faktura_udsteder_adresse_id_vareafsendelse, faktura_udsteder_reference_fax_nr,
                    bank_id, bank_navn, kontantrabatsats, strafrentesats,
                    kontant_rabat_dato, start_rente_dato,
                    zero_rated_afgifts_kategori, zero_rated_afgifts_kategori_procent,
                    gebyr_rabat_aarsag, gebyr_rabat_kategori, gebyr_rabat_finanskonto,
                    gebyr_tillaeg_aarsag, gebyr_tillaeg_kategori, gebyr_tillaeg_finanskonto,
                    statistic_invoice_header_id, header_dw_timestamp_update,
                    dw_timestamp_insert, dw_timestamp_update
                )
                VALUES (
                    src.dw_ek_Faktura, src.faktura_nr, src.faktura_valuta_kode, src.firma_navn, src.leverandoer_cvr,
                    src.adresse, src.bynavn, src.postnummer, src.dokument_type,
                    src.regnings_modtager_id, src.regnings_modtager_id_type, src.koebs_ordrenummer, src.salgs_ordrenummer,
                    src.kunde_identification, src.kunde_identification_type, src.dimension_konto_streng,
                    src.kundenavn, src.kunde_vejnavn, src.kunde_hus_nr, src.kunde_bynavn, src.kunde_post_nr, src.kunde_landekode,
                    src.kunde_reference_id, src.kunde_reference_navn,
                    src.faktura_udsteder_identifikation, src.faktura_udsteder_identifikation_type, src.faktura_udsteder_navn,
                    src.faktura_udsteder_vejnavn_vareafsendelse, src.faktura_udsteder_hus_nr_vareafsendelse,
                    src.faktura_udsteder_bynavn_vareafsendelse, src.faktura_udsteder_post_nr_vareafsendelse,
                    src.faktura_udsteder_land_kode_vareafsendelse,
                    src.faktura_udsteder_cvr_nummer, src.faktura_udsteder_cvr_nummer_type,
                    src.faktura_udsteder_reference_idenrifikation, src.faktura_udsteder_reference_navn,
                    src.faktura_udsteder_reference_telefon_nr, src.faktura_udsteder_reference_e_mail,
                    src.kort_arts_kode, src.betalings_type, src.betalings_id, src.kreditnummer, src.kontonummer, src.kontotype,
                    src.afdelings_id, src.told_kurs, src.told_kurs_type,
                    src.kunde_reference_telefon_nr, src.kunde_reference_fax_nr, src.kunde_reference_e_mail, src.kunde_reference_rolle,
                    src.juridisk_kunde_identification, src.juridisk_kunde_identification_type,
                    src.juridisk_dimension_konto_streng,
                    src.juridisk_kundenavn, src.juridisk_kunde_vejnavn, src.juridisk_kunde_hus_nr,
                    src.juridisk_kunde_bynavn, src.juridisk_kunde_post_nr, src.juridisk_kunde_landekode,
                    src.juridisk_kunde_reference_id, src.juridisk_kunde_reference_navn,
                    src.juridisk_kunde_reference_telefon_nr, src.juridisk_kunde_reference_fax_nr,
                    src.juridisk_kunde_reference_e_mail, src.juridisk_kunde_reference_rolle,
                    src.faktura_udsteder_adresse_id_vareafsendelse, src.faktura_udsteder_reference_fax_nr,
                    src.bank_id, src.bank_navn, src.kontantrabatsats, src.strafrentesats,
                    src.kontant_rabat_dato, src.start_rente_dato,
                    src.zero_rated_afgifts_kategori, src.zero_rated_afgifts_kategori_procent,
                    src.gebyr_rabat_aarsag, src.gebyr_rabat_kategori, src.gebyr_rabat_finanskonto,
                    src.gebyr_tillaeg_aarsag, src.gebyr_tillaeg_kategori, src.gebyr_tillaeg_finanskonto,
                    src.statistic_invoice_header_id, src.header_dw_timestamp_update,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                );
        """

                insert_sql = """
            INSERT INTO [dim].[Faktura] WITH (TABLOCK) (
                    dw_ek_Faktura, faktura_nr, faktura_valuta_kode,
                    firma_navn, leverandoer_cvr, adresse,
                    bynavn, postnummer, dokument_type,
                    regnings_modtager_id, regnings_modtager_id_type, koebs_ordrenummer,
                    salgs_ordrenummer, kunde_identification, kunde_identification_type,
                    dimension_konto_streng, kundenavn, kunde_vejnavn,
                    kunde_hus_nr, kunde_bynavn, kunde_post_nr,
                    kunde_landekode, kunde_reference_id, kunde_reference_navn,
                    faktura_udsteder_identifikation, faktura_udsteder_identifikation_type, faktura_udsteder_navn,
                    faktura_udsteder_vejnavn_vareafsendelse, faktura_udsteder_hus_nr_vareafsendelse, faktura_udsteder_bynavn_vareafsendelse,
                    faktura_udsteder_post_nr_vareafsendelse, faktura_udsteder_land_kode_vareafsendelse, faktura_udsteder_cvr_nummer,
                    faktura_udsteder_cvr_nummer_type, faktura_udsteder_reference_idenrifikation, faktura_udsteder_reference_navn,
                    faktura_udsteder_reference_telefon_nr, faktura_udsteder_reference_e_mail, kort_arts_kode,
                    betalings_type, betalings_id, kreditnummer,
                    kontonummer, kontotype, afdelings_id,
                    told_kurs, told_kurs_type, kunde_reference_telefon_nr,
                    kunde_reference_fax_nr, kunde_reference_e_mail, kunde_reference_rolle,
                    juridisk_kunde_identification, juridisk_kunde_identification_type, juridisk_dimension_konto_streng,
                    juridisk_kundenavn, juridisk_kunde_vejnavn, juridisk_kunde_hus_nr,
                    juridisk_kunde_bynavn, juridisk_kunde_post_nr, juridisk_kunde_landekode,
                    juridisk_kunde_reference_id, juridisk_kunde_reference_navn, juridisk_kunde_reference_telefon_nr,
                    juridisk_kunde_reference_fax_nr, juridisk_kunde_reference_e_mail, juridisk_kunde_reference_rolle,
                    faktura_udsteder_adresse_id_vareafsendelse, faktura_udsteder_reference_fax_nr, bank_id,
                    bank_navn, kontantrabatsats, strafrentesats,
                    kontant_rabat_dato, start_rente_dato, zero_rated_afgifts_kategori,
                    zero_rated_afgifts_kategori_procent, gebyr_rabat_aarsag, gebyr_rabat_kategori,
                    gebyr_rabat_finanskonto, gebyr_tillaeg_aarsag, gebyr_tillaeg_kategori,
                    gebyr_tillaeg_finanskonto, statistic_invoice_header_id, header_dw_timestamp_update,
                    dw_timestamp_insert, dw_timestamp_update
            )
            SELECT
                    src.dw_ek_Faktura, src.faktura_nr, src.faktura_valuta_kode,
                    src.firma_navn, src.leverandoer_cvr, src.adresse,
                    src.bynavn, src.postnummer, src.dokument_type,
                    src.regnings_modtager_id, src.regnings_modtager_id_type, src.koebs_ordrenummer,
                    src.salgs_ordrenummer, src.kunde_identification, src.kunde_identification_type,
                    src.dimension_konto_streng, src.kundenavn, src.kunde_vejnavn,
                    src.kunde_hus_nr, src.kunde_bynavn, src.kunde_post_nr,
                    src.kunde_landekode, src.kunde_reference_id, src.kunde_reference_navn,
                    src.faktura_udsteder_identifikation, src.faktura_udsteder_identifikation_type, src.faktura_udsteder_navn,
                    src.faktura_udsteder_vejnavn_vareafsendelse, src.faktura_udsteder_hus_nr_vareafsendelse, src.faktura_udsteder_bynavn_vareafsendelse,
                    src.faktura_udsteder_post_nr_vareafsendelse, src.faktura_udsteder_land_kode_vareafsendelse, src.faktura_udsteder_cvr_nummer,
                    src.faktura_udsteder_cvr_nummer_type, src.faktura_udsteder_reference_idenrifikation, src.faktura_udsteder_reference_navn,
                    src.faktura_udsteder_reference_telefon_nr, src.faktura_udsteder_reference_e_mail, src.kort_arts_kode,
                    src.betalings_type, src.betalings_id, src.kreditnummer,
                    src.kontonummer, src.kontotype, src.afdelings_id,
                    src.told_kurs, src.told_kurs_type, src.kunde_reference_telefon_nr,
                    src.kunde_reference_fax_nr, src.kunde_reference_e_mail, src.kunde_reference_rolle,
                    src.juridisk_kunde_identification, src.juridisk_kunde_identification_type, src.juridisk_dimension_konto_streng,
                    src.juridisk_kundenavn, src.juridisk_kunde_vejnavn, src.juridisk_kunde_hus_nr,
                    src.juridisk_kunde_bynavn, src.juridisk_kunde_post_nr, src.juridisk_kunde_landekode,
                    src.juridisk_kunde_reference_id, src.juridisk_kunde_reference_navn, src.juridisk_kunde_reference_telefon_nr,
                    src.juridisk_kunde_reference_fax_nr, src.juridisk_kunde_reference_e_mail, src.juridisk_kunde_reference_rolle,
                    src.faktura_udsteder_adresse_id_vareafsendelse, src.faktura_udsteder_reference_fax_nr, src.bank_id,
                    src.bank_navn, src.kontantrabatsats, src.strafrentesats,
                    src.kontant_rabat_dato, src.start_rente_dato, src.zero_rated_afgifts_kategori,
                    src.zero_rated_afgifts_kategori_procent, src.gebyr_rabat_aarsag, src.gebyr_rabat_kategori,
                    src.gebyr_rabat_finanskonto, src.gebyr_tillaeg_aarsag, src.gebyr_tillaeg_kategori,
                    src.gebyr_tillaeg_finanskonto, src.statistic_invoice_header_id, src.header_dw_timestamp_update,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
            FROM #Faktura_Attributes src;
        """

                # Short-circuit: if the target holds only the -1 sentinel (fresh or
                # rebuilt table), a bulk INSERT ... WITH (TABLOCK) can be minimally
                # logged, whereas MERGE is always fully logged. The source draws
                # dw_ek_Faktura from map.Faktura (IDENTITY >= 1), so it never
                # collides with the -1 sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[Faktura] "
                    "WHERE dw_ek_Faktura <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(merge_sql)
                else:
                    context.log.info(
                        "Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK), "
                        "minimally logged"
                    )
                    cursor.execute(insert_sql)

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Faktura_Attributes")

                cursor.execute("UPDATE STATISTICS [dim].[Faktura]")

                cursor.execute(
                    """
            SELECT
                COUNT(*) AS total_dim_rows,
                SUM(CASE WHEN dw_timestamp_update IS NULL THEN 1 ELSE 0 END) AS inserts_only,
                SUM(CASE WHEN dw_timestamp_update IS NOT NULL THEN 1 ELSE 0 END) AS updated,
                COUNT(faktura_nr) AS has_faktura_nr,
                COUNT(firma_navn) AS has_firma_navn
            FROM [dim].[Faktura]
            WHERE dw_ek_Faktura <> -1
        """
                )
                stats = cursor.fetchone()
                total_dim_rows = stats[0]
                inserts_only = stats[1]
                updated_count = stats[2]
                has_faktura_nr = stats[3]
                has_firma_navn = stats[4]

                context.log.info(
                    f"Dimension sync complete: {total_dim_rows} total rows "
                    f"({inserts_only} insert-only, {updated_count} updated)"
                )
                context.log.info(
                    f"Coverage: {has_faktura_nr}/{total_dim_rows} with faktura_nr, "
                    f"{has_firma_navn}/{total_dim_rows} with firma_navn"
                )
            finally:
                cursor.close()

        result: MaterializeResult = MaterializeResult(
            metadata={
                "rows_affected": rows_affected,
                "total_dim_rows": total_dim_rows,
                "inserts_only": inserts_only,
                "updated": updated_count,
                "has_faktura_nr": has_faktura_nr,
                "has_firma_navn": has_firma_navn,
                "table": "dim.Faktura",
                "source_tables": "map.Faktura + stg.dim_Faktura_mysupply",
                "scd_type": "Type 1 (overwrite)",
            }
        )

        logged_cp: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Faktura", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "Faktura")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return result
