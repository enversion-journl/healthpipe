# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Fakturalinje from MySupply (Fact Layer)

Base pre-staging for MySupply - joins invoice lines, headers and metadata.
Foundation for dim_kreditor_mysupply and dim_vare_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "pre_stg_fakturalinje_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        # Daily partitioned MySupply assets
        AssetKey(["ext_statistic_invoice_line_mysupply_daily"]),
        AssetKey(["ext_statistic_invoice_header_mysupply_daily"]),
        AssetKey(["ext_metadata_mysupply_daily"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_daily"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_daily"]),
        # Monthly partitioned MySupply assets
        AssetKey(["ext_statistic_invoice_line_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_header_mysupply_monthly"]),
        AssetKey(["ext_metadata_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_line_mysupply_full_load"]),
        AssetKey(["ext_statistic_invoice_header_mysupply_full_load"]),
        AssetKey(["ext_metadata_mysupply_full_load"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_full_load"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "mysupply",
        "entity": "fakturalinje",
    },
    description="Base staging for MySupply - joins invoice lines, headers and metadata. Foundation for dim_kreditor_mysupply and dim_vare_mysupply.",
)
def pre_stg_fakturalinje_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Base staging for MySupply invoice data.

    Performs all heavy joins ONCE:
    - ext.statistic_invoice_header_mysupply: Header with kreditor info (main entity)
    - ext.metadata_mysupply: 1:1 with header - kreditor enrichment
    - ext.statistic_invoice_line_mysupply: 0..M with header - line-level data

    CleanCodes are generated inline in SQL (the ext tables contain raw data only).

    Join order is important:
    - Start from header (main entity)
    - INNER JOIN metadata (1:1 relationship)
    - LEFT JOIN lines (0..M - headers can have 0 lines)

    This table is the foundation for:
    - dim_kreditor_mysupply (aggregated kreditor view)
    - dim_vare_mysupply (aggregated vare view)
    - fct_fakturalinje (fact table can also use this directly)
    """

    # A FULL load joins the entire ext.* tables (~15M header rows x wide columns,
    # three ROW_NUMBER dedups + SELECT INTO), which exceeds the resource's default
    # 6000s (100 min) query timeout. Use no query timeout (0) for this one-shot
    # heavy build; daily/monthly runs operate on small partition deltas and finish
    # quickly regardless.
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating base staging table stg.pre_fakturalinje_mysupply...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[pre_fakturalinje_mysupply];")
            cursor.execute(
                """
                ;WITH header_dedup AS (
                    SELECT
                        h.*,
                        CAST(h.id AS NVARCHAR(500)) AS FakturaHeader_CleanCode,
                        CASE
                            WHEN h.FakturaNr IS NOT NULL
                                 AND LTRIM(RTRIM(h.FakturaNr)) <> ''
                            THEN 'FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(h.FakturaNr, ' ', ''))))
                            ELSE NULL
                        END AS Faktura_CleanCode,
                        ROW_NUMBER() OVER (
                            PARTITION BY h.id
                            ORDER BY h.DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [ext].[statistic_invoice_header_mysupply] h
                    WHERE h.id IS NOT NULL
                ),
                metadata_dedup AS (
                    SELECT
                        m.*,
                        CAST(m.id AS NVARCHAR(500)) AS FakturaMetadata_CleanCode,
                        CASE
                            WHEN m.LeverandorCVR IS NOT NULL
                                 AND LTRIM(RTRIM(m.LeverandorCVR)) <> ''
                            THEN 'CVR_' + CASE WHEN LTRIM(RTRIM(m.LeverandorCVR)) LIKE 'DK[0-9]%'
                                               THEN SUBSTRING(LTRIM(RTRIM(m.LeverandorCVR)), 3, LEN(LTRIM(RTRIM(m.LeverandorCVR))))
                                               ELSE LTRIM(RTRIM(m.LeverandorCVR)) END
                            ELSE NULL
                        END AS CVR_CleanCode,
                        ROW_NUMBER() OVER (
                            PARTITION BY m.id
                            ORDER BY m.DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [ext].[metadata_mysupply] m
                    WHERE m.id IS NOT NULL
                ),
                line_dedup AS (
                    SELECT
                        l.*,
                        CAST(l.id AS NVARCHAR(500)) AS FakturaLines_CleanCode,
                        CASE
                            WHEN l.Item_FakturaUdstederVareNr IS NOT NULL
                                 AND LTRIM(RTRIM(l.Item_FakturaUdstederVareNr)) <> ''
                            THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(l.Item_FakturaUdstederVareNr, ' ', ''))))
                            ELSE NULL
                        END AS Leverandoer_Varenummer_CleanCode,
                        ROW_NUMBER() OVER (
                            PARTITION BY l.id
                            ORDER BY l.DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [ext].[statistic_invoice_line_mysupply] l
                    WHERE l.id IS NOT NULL
                )
                SELECT
                    l.id AS statistic_invoice_line_id,
                    h.id AS statistic_invoice_header_id,
                    m.id AS metadata_id,

                    CAST(h.FakturaHeader_CleanCode AS NVARCHAR(500)) AS CleanCode_FakturaHeader,
                    CAST(l.FakturaLines_CleanCode AS NVARCHAR(500)) AS CleanCode_FakturaLines,
                    CAST(m.FakturaMetadata_CleanCode AS NVARCHAR(500)) AS CleanCode_FakturaMetadata,
                    CAST(
                        m.FakturaMetadata_CleanCode + '_' +
                        h.FakturaHeader_CleanCode + '_' +
                        COALESCE(l.FakturaLines_CleanCode, 'NULL')
                        AS NVARCHAR(500)
                    ) AS CleanCode_Fakturalinje,

                    -- metadata.LeverandorCVR → CVR_CleanCode (strip DK prefix) → LeverandorCVR without prefix
                    CAST(m.CVR_CleanCode AS NVARCHAR(200)) AS CleanCode_Kreditor,

                    CAST(h.RegningsModtagerID AS NVARCHAR(200)) AS CleanCode_EAN_Regningsmodtager,

                    CAST(h.Faktura_CleanCode AS NVARCHAR(500)) AS CleanCode_Faktura,

                    CAST(l.Leverandoer_Varenummer_CleanCode AS NVARCHAR(500)) AS CleanCode_LeverandoerVarenummer,

                    h.FakturaUdstederCVRNummer,
                    CAST(
                        NULLIF(SUBSTRING(m.CVR_CleanCode, 5, 8000), '')
                        AS NVARCHAR(50)
                    ) AS LeverandorCVR,
                    m.FirmaNavn,
                    m.[By] AS ByNavn,
                    m.Adresse,
                    m.Postnummer,
                    m.[Type] AS MetadataType,
                    h.FakturaValutaKode,

                    l.Item_FakturaUdstederVareNr,
                    l.Item_FakturaUdstederVareNavn,

                    h.FakturaNr,
                    h.FakturaDato,
                    h.OrdreDato,
                    h.BetalingsDato,
                    l.AntalEnhederFaktureret,
                    l.[LinjeBeløb] AS LinjeBeloeb,
                    l.[LinjeBeløb_Valuta] AS LinjeBeloeb_Valuta,
                    l.BasePrice_EnhedsPris,
                    l.BasePrice_EnhedsPris_Valuta,
                    h.FakturaTotal AS HeaderFakturaTotal,
                    m.FakturaTotal AS MetadataFakturaTotal,

                    h.VAT_AfgiftsKategoriProcent AS MomsProcent,
                    h.[VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag] AS MomsGrundlag,
                    h.[VAT_TaxAmounts_AfgiftsKategoriMomsBeløb] AS MomsBeloeb,
                    h.FakturaLinjeTotal AS LinjeTotal,

                    -- Phase 2: Header descriptive columns
                    h.RegningsModtagerID,
                    h.RegningsModtagerID_Type,
                    h.[KøbsOrdreNummer],
                    h.SalgsOrdreNummer,
                    h.KundeIdentification,
                    h.KundeIdentification_Type,
                    h.DimensionKontoStreng,
                    h.KundeNavn,
                    h.KundeVejNavn,
                    h.KundeHusNr,
                    h.KundeByNavn,
                    h.KundePostNr,
                    h.KundeLandeKode,
                    h.KundeReferenceID,
                    h.KundeReferenceNavn,
                    h.FakturaUdstederIdentifikation,
                    h.FakturaUdstederIdentifikation_Type,
                    h.FakturaUdstederNavn,
                    h.FakturaUdstederVejNavn_Vareafsendelse,
                    h.FakturaUdstederHusNr_Vareafsendelse,
                    h.FakturaUdstederByNavn_Vareafsendelse,
                    h.FakturaUdstederPostNr_Vareafsendelse,
                    h.FakturaUdstederLandKode_Vareafsendelse,
                    h.FakturaUdstederCVRNummer_Type,
                    h.FakturaUdstederReferenceIdenrifikation,
                    h.FakturaUdstederReferenceNavn,
                    h.FakturaUdstederReferenceTelefonNr,
                    h.FakturaUdstederReferenceEMail,
                    h.KortArtsKode,
                    h.BetalingsType,
                    h.BetalingsID,
                    h.Kreditnummer,
                    h.Kontonummer,
                    h.Kontotype,
                    h.AfdelingsID,
                    h.ToldKurs,
                    h.ToldKurs_Type,

                    -- Phase 2: Header VAT/valuta columns (for fct)
                    h.[VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta] AS MomsGrundlag_Valuta,
                    h.[VAT_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta] AS MomsBeloeb_Valuta,
                    h.VAT_AfgiftsKategori,
                    h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag] AS ZeroRated_MomsGrundlag,
                    h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta] AS ZeroRated_MomsGrundlag_Valuta,
                    h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb] AS ZeroRated_MomsBeloeb,
                    h.[ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta] AS ZeroRated_MomsBeloeb_Valuta,
                    h.FakturaLinjeTotal_Valuta,
                    h.FakturaTotal_Valuta,

                    -- Header: High-NULL columns (full coverage)
                    h.KundeReferenceTelefonNr,
                    h.KundeReferenceFaxNr,
                    h.[KundeReferenceE-Mail] AS KundeReferenceEMail,
                    h.KundeReferenceRolle,
                    h.Juridisk_KundeIdentification,
                    h.Juridisk_KundeIdentification_Type,
                    h.Juridisk_DimensionKontoStreng,
                    h.Juridisk_KundeNavn,
                    h.Juridisk_KundeVejNavn,
                    h.Juridisk_KundeHusNr,
                    h.Juridisk_KundeByNavn,
                    h.Juridisk_KundePostNr,
                    h.Juridisk_KundeLandeKode,
                    h.Juridisk_KundeReferenceID,
                    h.Juridisk_KundeReferenceNavn,
                    h.Juridisk_KundeReferenceTelefonNr,
                    h.Juridisk_KundeReferenceFaxNr,
                    h.[Juridisk_KundeReferenceE-Mail] AS Juridisk_KundeReferenceEMail,
                    h.Juridisk_KundeReferenceRolle,
                    h.FakturaUdstederAdresseID_Vareafsendelse,
                    h.FakturaUdstederReferenceFaxNr,
                    h.BankID,
                    h.BankNavn,
                    h.Kontantrabatsats,
                    h.Strafrentesats,
                    h.KontantRabatDato,
                    h.StartRenteDato,
                    h.[ZERO-RATED_AfgiftsKategori] AS ZeroRated_AfgiftsKategori,
                    h.[ZERO-RATED_AfgiftsKategoriProcent] AS ZeroRated_AfgiftsKategoriProcent,

                    -- Line: Phase 2 descriptive columns
                    l.FakturaLinjeNr,
                    l.AntalEnhederFaktureret_Type,
                    l.[Item_FakturaUdstederensVareNr],
                    l.[Item_FakturaUdstederensVareNr_Type],
                    l.[BasePrice_EnhedsStørrelse],
                    l.[BasePrice_EnhedsStørrelse_Type],
                    l.Item_FakturaUdstederVareNavnKort,

                    -- Line: High-NULL columns (full coverage)
                    l.DimensionsKontoStreng AS Line_DimensionsKontoStreng,
                    l.LeveranceKrav,
                    l.LeveringsDato_2,
                    l.Item_KundensVareNr,
                    l.Item_KundensVareNr_Type,
                    l.Item_KundensVareBeskrivelse,
                    l.Item_FakturaUdstederensVareBeskrivelse,
                    l.Item_FabrikantensVareNr,
                    l.Item_FabrikantensVareNr_Type,
                    l.Item_FabrikantensVareBeskrivelse,
                    l.Item_StandardiseretVareNr,
                    l.Item_StandardiseretVareNr_Type,
                    l.Item_StandardiseretVareBeskrivelse,
                    l.Item_VareKlassifikation,
                    l.Item_VareKlassifikation_Type,
                    l.Item_AfgiftsKategori,
                    l.Item_AfgiftsKategoriProcent,

                    l.DW_LoadTimestamp AS Line_DW_LoadTimestamp,
                    h.DW_LoadTimestamp AS Header_DW_LoadTimestamp

                INTO [stg].[pre_fakturalinje_mysupply]
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
            context.log.info(f"Created stg.pre_fakturalinje_mysupply with {row_count} rows")

            # Create clustered index on header (always present) + line (can be NULL)
            # Use header as primary since lines are optional
            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_pre_fakturalinje_mysupply_PK
                ON [stg].[pre_fakturalinje_mysupply](statistic_invoice_header_id, statistic_invoice_line_id);
            """
            )

            # Create non-clustered indexes for entity aggregation
            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_fakturalinje_mysupply_CVR
                ON [stg].[pre_fakturalinje_mysupply](CleanCode_Kreditor)
                INCLUDE (LeverandorCVR, FirmaNavn, ByNavn, Adresse, Postnummer, FakturaValutaKode, FakturaDato);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_fakturalinje_mysupply_Vare
                ON [stg].[pre_fakturalinje_mysupply](CleanCode_Kreditor, CleanCode_LeverandoerVarenummer)
                INCLUDE (Item_FakturaUdstederVareNr, Item_FakturaUdstederVareNavn, FakturaUdstederCVRNummer, FakturaDato);
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
                    COUNT(CleanCode_Kreditor) AS has_cvr_clean,
                    COUNT(CleanCode_LeverandoerVarenummer) AS has_vare_clean,
                    COUNT(DISTINCT CleanCode_Kreditor) AS unique_cvrs,
                    COUNT(DISTINCT CleanCode_LeverandoerVarenummer) AS unique_varer
                FROM [stg].[pre_fakturalinje_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Rows: {stats[0]} total ({stats[1]} with lines, {stats[2]} headers without lines). "
                f"Unique headers: {stats[3]}, unique CVRs: {stats[6]}, unique varer: {stats[7]}"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.pre_fakturalinje_mysupply",
            "row_count": row_count,
            "source_tables": "ext.statistic_invoice_header_mysupply + ext.metadata_mysupply + ext.statistic_invoice_line_mysupply",
            "grain": "statistic_invoice_header_id + statistic_invoice_line_id (line can be NULL)",
            "unique_headers": stats[3],
            "headers_without_lines": stats[2],
            "unique_cvrs": stats[6],
            "unique_varer": stats[7],
            "layer": "stg_fct (Layer 0)",
        }
    )
