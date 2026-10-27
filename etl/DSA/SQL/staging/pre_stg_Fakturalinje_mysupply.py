# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Fakturalinje from MySupply (Fact Layer)

Base pre-staging for MySupply - joins invoice lines, headers and metadata.
"""

from typing import cast

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from utils.sql.tax_id_sql import sql_cvr_cleancode_from_column


def resolve_min_checkpoint(owner_cps: list[str | None]) -> str | None:
    """Resolve the delta lower-bound across all downstream owner checkpoints.

    A ``None`` checkpoint means that owner is missing data entirely (never
    successfully run, or deliberately reset). It is NOT "no opinion" — it is the
    oldest possible checkpoint, so it MUST force a full rebuild; otherwise a reset
    owner is silently skipped and the delta path reprocesses nothing new.

    Therefore: if ANY owner checkpoint is None → return None (caller does a full
    rebuild). Only when every owner has a checkpoint do we take the MIN, so no
    owner can miss a change.
    """
    if not owner_cps or any(cp is None for cp in owner_cps):
        return None
    return min(cast(list[str], owner_cps))


@asset(
    key=AssetKey(["erp", "pre_stg_Fakturalinje_mysupply"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["arc_mysupply_statistic_invoice_header"]),
        AssetKey(["arc_mysupply_metadata"]),
        AssetKey(["arc_mysupply_statistic_invoice_line"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "mysupply",
        "entity": "fakturalinje",
    },
    description="Base staging for MySupply - joins invoice lines, headers and metadata.",
)
def pre_stg_Fakturalinje_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Base staging for MySupply invoice data.

    Performs all heavy joins ONCE:
    - arc.mysupply_statistic_invoice_header: Header with kreditor info (main entity)
    - arc.mysupply_metadata: 1:1 with header - kreditor enrichment
    - arc.mysupply_statistic_invoice_line: 0..M with header - line-level data

    CleanCodes are generated inline in SQL (the arc tables contain raw data only).

    Join order is important:
    - Start from header (main entity)
    - INNER JOIN metadata (1:1 relationship)
    - LEFT JOIN lines (0..M - headers can have 0 lines)

    This table is the foundation for:
    - the Vare/ILSVare entity-delta hub (pre_stg_Leverandoervare_ILSVare_delta_entiteter, stg_dim_Leverandoervare)
    - fct_Fakturalinje (fact table can also use this directly)
    """

    dfm = DfmManager(mssql_target_staging, context)

    # pre_stg is log-only; it reprocesses every header changed since the OLDEST
    # downstream owner checkpoint (so no owner can miss a change).
    owner_cps = [
        dfm.get_checkpoint("dim", "Faktura"),
        dfm.get_checkpoint("dim", "Fakturalinje"),
        dfm.get_checkpoint("fct", "Faktura"),
        dfm.get_checkpoint("fct", "Fakturalinje"),
    ]
    # If any owner checkpoint is None (reset or first run), force a full rebuild;
    # only take the MIN when every owner has a checkpoint. See resolve_min_checkpoint.
    min_checkpoint = resolve_min_checkpoint(owner_cps)

    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_Fakturalinje_mysupply",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    if config.mode == "full_load" or min_checkpoint is None:
        header_where = "h.id IS NOT NULL"
        context.log.info("Full rebuild: processing all headers from arc.mysupply_*")
    else:
        # Approach C: changed-header set. Reprocess a header WHOLE if the header,
        # any of its lines, or its metadata changed since min_checkpoint.
        header_where = f"""h.id IS NOT NULL AND h.id IN (
            SELECT id FROM [arc].[mysupply_statistic_invoice_header]
                WHERE dw_timestamp_update > '{min_checkpoint}'
            UNION
            SELECT statistic_invoice_header_id FROM [arc].[mysupply_statistic_invoice_line]
                WHERE dw_timestamp_update > '{min_checkpoint}'
            UNION
            SELECT h2.id FROM [arc].[mysupply_statistic_invoice_header] h2
                JOIN [arc].[mysupply_metadata] m2 ON m2.id = h2.metadata_id
                WHERE m2.dw_timestamp_update > '{min_checkpoint}'
        )"""
        context.log.info(f"Approach C: reprocessing headers changed since {min_checkpoint}")

    # A FULL load joins the entire arc.* tables (~15M header rows x wide columns,
    # three ROW_NUMBER dedups + SELECT INTO), which exceeds the resource's default
    # 6000s (100 min) query timeout. Use no query timeout (0) for this one-shot
    # heavy build; daily/monthly runs operate on small partition deltas and finish
    # quickly regardless.
    row_count = 0
    stats = None
    try:
        with mssql_target_staging.get_connection(timeout=0) as conn:
            cursor = conn.cursor()
            try:

                context.log.info("Creating base staging table stg.pre_Fakturalinje_mysupply...")

                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_Fakturalinje_mysupply];")
                cursor.execute(
                    f"""
                    ;WITH header_dedup AS (
                        SELECT
                            h.*,
                            CAST(h.id AS NVARCHAR(500)) AS clean_code_FakturaHeader,
                            CASE
                                WHEN h.FakturaNr IS NOT NULL
                                     AND LTRIM(RTRIM(h.FakturaNr)) <> ''
                                THEN 'FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(h.FakturaNr, ' ', ''))))
                                ELSE NULL
                            END AS clean_code_Faktura,
                            -- faktura_udsteder_cvr_nummer_type (CPR/cvr/DK:cvr/DK:VAT/VAT/SE/...) is not
                            -- used for matching; only the number value is normalised (see tax_id_sql).
                            {sql_cvr_cleancode_from_column("h.FakturaUdstederCVRNummer")} AS clean_code_Kreditor_Header,
                            ROW_NUMBER() OVER (
                                PARTITION BY h.id
                                ORDER BY h.dw_timestamp_update DESC
                            ) AS rn
                        FROM [arc].[mysupply_statistic_invoice_header] h
                        WHERE {header_where}
                    ),
                    metadata_dedup AS (
                        SELECT
                            m.*,
                            CAST(m.id AS NVARCHAR(500)) AS clean_code_FakturaMetadata,
                            {sql_cvr_cleancode_from_column("m.LeverandorCVR")} AS clean_code_Kreditor,
                            ROW_NUMBER() OVER (
                                PARTITION BY m.id
                                ORDER BY m.dw_timestamp_update DESC
                            ) AS rn
                        FROM [arc].[mysupply_metadata] m
                        WHERE m.id IS NOT NULL
                    ),
                    line_dedup AS (
                        SELECT
                            l.*,
                            CAST(l.id AS NVARCHAR(500)) AS clean_code_FakturaLines,
                            CASE
                                WHEN l.Item_FakturaUdstederVareNr IS NOT NULL
                                     AND LTRIM(RTRIM(l.Item_FakturaUdstederVareNr)) <> ''
                                THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(l.Item_FakturaUdstederVareNr, ' ', ''))))
                                ELSE NULL
                            END AS clean_code_LeverandoerVarenummer,
                            ROW_NUMBER() OVER (
                                PARTITION BY l.id
                                ORDER BY l.dw_timestamp_update DESC
                            ) AS rn
                        FROM [arc].[mysupply_statistic_invoice_line] l
                        WHERE l.id IS NOT NULL
                    )
                    SELECT
                        l.id AS statistic_invoice_line_id,
                        h.id AS statistic_invoice_header_id,
                        m.id AS metadata_id,

                        CAST(h.clean_code_FakturaHeader AS NVARCHAR(500)) AS clean_code_FakturaHeader,
                        CAST(l.clean_code_FakturaLines AS NVARCHAR(500)) AS clean_code_FakturaLines,
                        CAST(m.clean_code_FakturaMetadata AS NVARCHAR(500)) AS clean_code_FakturaMetadata,
                        CAST(
                            m.clean_code_FakturaMetadata + '_' +
                            h.clean_code_FakturaHeader + '_' +
                            COALESCE(l.clean_code_FakturaLines, 'NULL')
                            AS NVARCHAR(500)
                        ) AS clean_code_Fakturalinje,

                        -- metadata.leverandoer_cvr -> clean_code_Kreditor (primary; unchanged for map.Kreditor)
                        CAST(m.clean_code_Kreditor AS NVARCHAR(200)) AS clean_code_Kreditor,
                        -- header.faktura_udsteder_cvr_nummer -> secondary KMD match key (apply-pass)
                        CAST(h.clean_code_Kreditor_Header AS NVARCHAR(200)) AS clean_code_Kreditor_Header,

                        CAST(h.RegningsModtagerID AS NVARCHAR(200)) AS clean_code_EAN_Regningsmodtager,

                        CAST(h.clean_code_Faktura AS NVARCHAR(500)) AS clean_code_Faktura,

                        CAST(l.clean_code_LeverandoerVarenummer AS NVARCHAR(500)) AS clean_code_LeverandoerVarenummer,

                        h.FakturaUdstederCVRNummer AS faktura_udsteder_cvr_nummer,
                        CAST(
                            NULLIF(SUBSTRING(m.clean_code_Kreditor, 5, 8000), '')
                            AS NVARCHAR(50)
                        ) AS leverandoer_cvr,
                        m.FirmaNavn AS firma_navn,
                        m.[By] AS bynavn,
                        m.Adresse AS adresse,
                        m.Postnummer AS postnummer,
                        m.[Type] AS MetadataType,
                        h.FakturaValutaKode AS faktura_valuta_kode,

                        l.Item_FakturaUdstederVareNr AS item_faktura_udsteder_vare_nr,
                        l.Item_FakturaUdstederVareNavn AS item_faktura_udsteder_vare_navn,

                        h.FakturaNr AS faktura_nr,
                        h.FakturaDato,
                        h.OrdreDato,
                        h.BetalingsDato,
                        l.AntalEnhederFaktureret AS antal_enheder_faktureret,
                        l.[LinjeBeløb] AS linje_beloeb,
                        l.[LinjeBeløb_Valuta] AS LinjeBeloeb_Valuta,
                        l.BasePrice_EnhedsPris,
                        l.BasePrice_EnhedsPris_Valuta,
                        h.FakturaTotal AS HeaderFakturaTotal,
                        m.FakturaTotal AS MetadataFakturaTotal,

                        h.VAT_AfgiftsKategoriProcent AS moms_procent,
                        h.[VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag] AS moms_grundlag,
                        h.[VAT_TaxAmounts_AfgiftsKategoriMomsBeløb] AS momsbeloeb,
                        h.FakturaLinjeTotal AS linje_total,

                        -- Phase 2: Header descriptive columns
                        h.RegningsModtagerID AS regnings_modtager_id,
                        h.RegningsModtagerID_Type AS regnings_modtager_id_type,
                        h.[KøbsOrdreNummer] AS koebs_ordrenummer,
                        h.SalgsOrdreNummer AS salgs_ordrenummer,
                        h.KundeIdentification AS kunde_identification,
                        h.KundeIdentification_Type AS kunde_identification_type,
                        h.DimensionKontoStreng AS dimension_konto_streng,
                        h.KundeNavn AS kundenavn,
                        h.KundeVejNavn AS kunde_vejnavn,
                        h.KundeHusNr AS kunde_hus_nr,
                        h.KundeByNavn AS kunde_bynavn,
                        h.KundePostNr AS kunde_post_nr,
                        h.KundeLandeKode AS kunde_landekode,
                        h.KundeReferenceID AS kunde_reference_id,
                        h.KundeReferenceNavn AS kunde_reference_navn,
                        h.FakturaUdstederIdentifikation AS faktura_udsteder_identifikation,
                        h.FakturaUdstederIdentifikation_Type AS faktura_udsteder_identifikation_type,
                        h.FakturaUdstederNavn AS faktura_udsteder_navn,
                        h.FakturaUdstederVejNavn_Vareafsendelse AS faktura_udsteder_vejnavn_vareafsendelse,
                        h.FakturaUdstederHusNr_Vareafsendelse AS faktura_udsteder_hus_nr_vareafsendelse,
                        h.FakturaUdstederByNavn_Vareafsendelse AS faktura_udsteder_bynavn_vareafsendelse,
                        h.FakturaUdstederPostNr_Vareafsendelse AS faktura_udsteder_post_nr_vareafsendelse,
                        h.FakturaUdstederLandKode_Vareafsendelse AS faktura_udsteder_land_kode_vareafsendelse,
                        h.FakturaUdstederCVRNummer_Type AS faktura_udsteder_cvr_nummer_type,
                        h.FakturaUdstederReferenceIdenrifikation AS faktura_udsteder_reference_idenrifikation,
                        h.FakturaUdstederReferenceNavn AS faktura_udsteder_reference_navn,
                        h.FakturaUdstederReferenceTelefonNr AS faktura_udsteder_reference_telefon_nr,
                        h.FakturaUdstederReferenceEMail AS faktura_udsteder_reference_e_mail,
                        h.KortArtsKode AS kort_arts_kode,
                        h.BetalingsType AS betalings_type,
                        h.BetalingsID AS betalings_id,
                        h.Kreditnummer AS kreditnummer,
                        h.Kontonummer AS kontonummer,
                        h.Kontotype AS kontotype,
                        h.AfdelingsID AS afdelings_id,
                        h.ToldKurs AS told_kurs,
                        h.ToldKurs_Type AS told_kurs_type,

                        -- Phase 2: Header VAT/valuta columns (for fct)
                        h.[VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta] AS MomsGrundlag_Valuta,
                        h.[VAT_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta] AS MomsBeloeb_Valuta,
                        h.VAT_AfgiftsKategori AS vat_afgifts_kategori,
                        h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag] AS zero_rated_moms_grundlag,
                        h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta] AS ZeroRated_MomsGrundlag_Valuta,
                        h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb] AS zero_rated_momsbeloeb,
                        h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta] AS ZeroRated_MomsBeloeb_Valuta,
                        h.FakturaLinjeTotal_Valuta,
                        h.FakturaTotal_Valuta,

                        -- Header: High-NULL columns (full coverage)
                        h.KundeReferenceTelefonNr AS kunde_reference_telefon_nr,
                        h.KundeReferenceFaxNr AS kunde_reference_fax_nr,
                        h.[KundeReferenceE-Mail] AS kunde_reference_e_mail,
                        h.KundeReferenceRolle AS kunde_reference_rolle,
                        h.Juridisk_KundeIdentification AS juridisk_kunde_identification,
                        h.Juridisk_KundeIdentification_Type AS juridisk_kunde_identification_type,
                        h.Juridisk_DimensionKontoStreng AS juridisk_dimension_konto_streng,
                        h.Juridisk_KundeNavn AS juridisk_kundenavn,
                        h.Juridisk_KundeVejNavn AS juridisk_kunde_vejnavn,
                        h.Juridisk_KundeHusNr AS juridisk_kunde_hus_nr,
                        h.Juridisk_KundeByNavn AS juridisk_kunde_bynavn,
                        h.Juridisk_KundePostNr AS juridisk_kunde_post_nr,
                        h.Juridisk_KundeLandeKode AS juridisk_kunde_landekode,
                        h.Juridisk_KundeReferenceID AS juridisk_kunde_reference_id,
                        h.Juridisk_KundeReferenceNavn AS juridisk_kunde_reference_navn,
                        h.Juridisk_KundeReferenceTelefonNr AS juridisk_kunde_reference_telefon_nr,
                        h.Juridisk_KundeReferenceFaxNr AS juridisk_kunde_reference_fax_nr,
                        h.[Juridisk_KundeReferenceE-Mail] AS juridisk_kunde_reference_e_mail,
                        h.Juridisk_KundeReferenceRolle AS juridisk_kunde_reference_rolle,
                        h.FakturaUdstederAdresseID_Vareafsendelse AS faktura_udsteder_adresse_id_vareafsendelse,
                        h.FakturaUdstederReferenceFaxNr AS faktura_udsteder_reference_fax_nr,
                        h.BankID AS bank_id,
                        h.BankNavn AS bank_navn,
                        h.Kontantrabatsats AS kontantrabatsats,
                        h.Strafrentesats AS strafrentesats,
                        h.KontantRabatDato AS kontant_rabat_dato,
                        h.StartRenteDato AS start_rente_dato,
                        h.[ZERO-RATED_AfgiftsKategori] AS zero_rated_afgifts_kategori,
                        h.[ZERO-RATED_AfgiftsKategoriProcent] AS zero_rated_afgifts_kategori_procent,

                        -- Line: Phase 2 descriptive columns
                        l.FakturaLinjeNr AS faktura_linje_nr,
                        l.AntalEnhederFaktureret_Type AS antal_enheder_faktureret_type,
                        l.[Item_FakturaUdstederensVareNr] AS item_faktura_udstederens_vare_nr,
                        l.[Item_FakturaUdstederensVareNr_Type] AS item_faktura_udstederens_vare_nr_type,
                        l.[BasePrice_EnhedsStørrelse] AS base_price_enheds_stoerrelse,
                        l.[BasePrice_EnhedsStørrelse_Type] AS base_price_enheds_stoerrelse_type,
                        l.Item_FakturaUdstederVareNavnKort AS item_faktura_udsteder_vare_navn_kort,

                        -- Line: High-NULL columns (full coverage)
                        l.DimensionsKontoStreng AS line_dimensions_konto_streng,
                        l.LeveranceKrav AS leverance_krav,
                        l.LeveringsDato_2 AS leverings_dato_2,
                        l.Item_KundensVareNr AS item_kundens_vare_nr,
                        l.Item_KundensVareNr_Type AS item_kundens_vare_nr_type,
                        l.Item_KundensVareBeskrivelse AS item_kundens_vare_beskrivelse,
                        l.Item_FakturaUdstederensVareBeskrivelse AS item_faktura_udstederens_vare_beskrivelse,
                        l.Item_FabrikantensVareNr AS item_fabrikantens_vare_nr,
                        l.Item_FabrikantensVareNr_Type AS item_fabrikantens_vare_nr_type,
                        l.Item_FabrikantensVareBeskrivelse AS item_fabrikantens_vare_beskrivelse,
                        l.Item_StandardiseretVareNr AS item_standardiseret_vare_nr,
                        l.Item_StandardiseretVareNr_Type AS item_standardiseret_vare_nr_type,
                        l.Item_StandardiseretVareBeskrivelse AS item_standardiseret_vare_beskrivelse,
                        l.Item_VareKlassifikation AS item_vare_klassifikation,
                        l.Item_VareKlassifikation_Type AS item_vare_klassifikation_type,
                        l.Item_AfgiftsKategori AS item_afgifts_kategori,
                        l.Item_AfgiftsKategoriProcent AS item_afgifts_kategori_procent,

                        l.dw_timestamp_update AS Line_DW_Timestamp_Update,
                        h.dw_timestamp_update AS header_dw_timestamp_update

                    INTO [stg].[pre_Fakturalinje_mysupply]
                    FROM header_dedup h
                    INNER JOIN metadata_dedup m
                        ON m.id = h.metadata_id
                       AND m.rn = 1
                    LEFT JOIN line_dedup l
                        ON l.statistic_invoice_header_id = h.id
                       AND l.rn = 1
                    WHERE h.rn = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.pre_Fakturalinje_mysupply with {row_count} rows")

                # Create clustered index on header (always present) + line (can be NULL)
                # Use header as primary since lines are optional
                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_pre_fakturalinje_mysupply_PK
                    ON [stg].[pre_Fakturalinje_mysupply](statistic_invoice_header_id, statistic_invoice_line_id);
                """
                )

                # Create non-clustered indexes for entity aggregation
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_fakturalinje_mysupply_CVR
                    ON [stg].[pre_Fakturalinje_mysupply](clean_code_Kreditor)
                    INCLUDE (leverandoer_cvr, firma_navn, bynavn, adresse, postnummer, faktura_valuta_kode, FakturaDato);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_fakturalinje_mysupply_Vare
                    ON [stg].[pre_Fakturalinje_mysupply](clean_code_Kreditor, clean_code_LeverandoerVarenummer)
                    INCLUDE (item_faktura_udsteder_vare_nr, item_faktura_udsteder_vare_navn, faktura_udsteder_cvr_nummer, FakturaDato);
                """
                )

                context.log.info("Created indexes for entity aggregation")

                conn.commit()

                # Diagnostic: Check coverage
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(statistic_invoice_line_id) AS rows_with_lines,
                        SUM(CASE WHEN statistic_invoice_line_id IS NULL THEN 1 ELSE 0 END) AS headers_without_lines,
                        COUNT(DISTINCT statistic_invoice_header_id) AS unique_headers,
                        COUNT(clean_code_Kreditor) AS has_cvr_clean,
                        COUNT(clean_code_LeverandoerVarenummer) AS has_vare_clean,
                        COUNT(DISTINCT clean_code_Kreditor) AS unique_cvrs,
                        COUNT(DISTINCT clean_code_LeverandoerVarenummer) AS unique_varer
                    FROM [stg].[pre_Fakturalinje_mysupply]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Rows: {stats[0]} total ({stats[1]} with lines, {stats[2]} headers without lines). "
                    f"Unique headers: {stats[3]}, unique CVRs: {stats[6]}, unique varer: {stats[7]}"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_Fakturalinje_mysupply",
            "row_count": row_count,
            "source_tables": "arc.mysupply_statistic_invoice_header + arc.mysupply_metadata + arc.mysupply_statistic_invoice_line",
            "grain": "statistic_invoice_header_id + statistic_invoice_line_id (line can be NULL)",
            "unique_headers": stats[3] if stats else None,
            "headers_without_lines": stats[2] if stats else None,
            "unique_cvrs": stats[6] if stats else None,
            "unique_varer": stats[7] if stats else None,
            "layer": "stg_fct (Layer 0)",
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full rebuild)",
            "mode": config.mode,
        }
    )
