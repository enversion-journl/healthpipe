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
- map.Faktura: DW_EK_Faktura assignment via IDENTITY (canonical key: CleanCode_Faktura)
  Grain: metadata_id + header_id (header grain via map.Faktura)
- stg.dim_faktura_mysupply: Deduplicated faktura-level attributes from MySupply

BY-DESIGN: map.Faktura is populated from a UNION of MySupply and SydAX
sources, but this dimension only enriches from MySupply staging. Map rows that
originated solely from SydAX (fct_postering_sydax) will exist in the dimension
with NULL descriptive attributes (FakturaNr, FirmaNavn, etc.). This is expected:
SydAX provides only the CleanCode for EK resolution, not the textual attributes.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_faktura"]),
        AssetKey(["erp", "stg_dim_faktura_mysupply"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Faktura from map.Faktura (header grain) + add textual attributes from MySupply pre-staging",
)
def dim_faktura(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map.Faktura + add textual attributes."""

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
                m.DW_EK_Faktura,
                stg.FakturaNr,
                LEFT(stg.FakturaValutaKode, 10) AS FakturaValutaKode,
                stg.FirmaNavn,
                stg.LeverandorCVR,
                stg.Adresse,
                stg.ByNavn,
                stg.Postnummer,
                stg.DokumentType,
                stg.RegningsModtagerID,
                stg.RegningsModtagerID_Type,
                stg.KøbsOrdreNummer,
                stg.SalgsOrdreNummer,
                stg.KundeIdentification,
                stg.KundeIdentification_Type,
                stg.DimensionKontoStreng,
                stg.KundeNavn,
                stg.KundeVejNavn,
                stg.KundeHusNr,
                stg.KundeByNavn,
                stg.KundePostNr,
                stg.KundeLandeKode,
                stg.KundeReferenceID,
                stg.KundeReferenceNavn,
                stg.FakturaUdstederIdentifikation,
                stg.FakturaUdstederIdentifikation_Type,
                stg.FakturaUdstederNavn,
                stg.FakturaUdstederVejNavn_Vareafsendelse,
                stg.FakturaUdstederHusNr_Vareafsendelse,
                stg.FakturaUdstederByNavn_Vareafsendelse,
                stg.FakturaUdstederPostNr_Vareafsendelse,
                stg.FakturaUdstederLandKode_Vareafsendelse,
                stg.FakturaUdstederCVRNummer,
                stg.FakturaUdstederCVRNummer_Type,
                stg.FakturaUdstederReferenceIdenrifikation,
                stg.FakturaUdstederReferenceNavn,
                stg.FakturaUdstederReferenceTelefonNr,
                stg.FakturaUdstederReferenceEMail,
                stg.KortArtsKode,
                stg.BetalingsType,
                stg.BetalingsID,
                stg.Kreditnummer,
                stg.Kontonummer,
                stg.Kontotype,
                stg.AfdelingsID,
                stg.ToldKurs,
                stg.ToldKurs_Type,
                stg.KundeReferenceTelefonNr,
                stg.KundeReferenceFaxNr,
                stg.KundeReferenceEMail,
                stg.KundeReferenceRolle,
                stg.Juridisk_KundeIdentification,
                stg.Juridisk_KundeIdentification_Type,
                stg.Juridisk_DimensionKontoStreng,
                stg.Juridisk_KundeNavn,
                stg.Juridisk_KundeVejNavn,
                stg.Juridisk_KundeHusNr,
                stg.Juridisk_KundeByNavn,
                stg.Juridisk_KundePostNr,
                stg.Juridisk_KundeLandeKode,
                stg.Juridisk_KundeReferenceID,
                stg.Juridisk_KundeReferenceNavn,
                stg.Juridisk_KundeReferenceTelefonNr,
                stg.Juridisk_KundeReferenceFaxNr,
                stg.Juridisk_KundeReferenceEMail,
                stg.Juridisk_KundeReferenceRolle,
                stg.FakturaUdstederAdresseID_Vareafsendelse,
                stg.FakturaUdstederReferenceFaxNr,
                stg.BankID,
                stg.BankNavn,
                stg.Kontantrabatsats,
                stg.Strafrentesats,
                stg.KontantRabatDato,
                stg.StartRenteDato,
                stg.ZeroRated_AfgiftsKategori,
                stg.ZeroRated_AfgiftsKategoriProcent,
                stg.GebyrRabatAarsag,
                stg.GebyrRabatKategori,
                stg.GebyrRabatFinanskonto,
                stg.GebyrTillaegAarsag,
                stg.GebyrTillaegKategori,
                stg.GebyrTillaegFinanskonto,
                stg.statistic_invoice_header_id,
                stg.Header_DW_LoadTimestamp

            INTO #Faktura_Attributes
            FROM [stg].[dim_faktura_mysupply] stg
            INNER JOIN [map].[Faktura] m
                ON m.CleanCode_Faktura = stg.CleanCode_Faktura;
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Faktura_Attributes
            ON #Faktura_Attributes (DW_EK_Faktura)
            """
            )
            context.log.info("Temp table #Faktura_Attributes created")

            cursor.execute(
                """
            MERGE [dim].[Faktura] WITH (TABLOCK) AS tgt
            USING #Faktura_Attributes AS src
            ON tgt.DW_EK_Faktura = src.DW_EK_Faktura

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.FakturaNr = src.FakturaNr,
                    tgt.FakturaValutaKode = src.FakturaValutaKode,
                    tgt.FirmaNavn = src.FirmaNavn,
                    tgt.LeverandorCVR = src.LeverandorCVR,
                    tgt.Adresse = src.Adresse,
                    tgt.ByNavn = src.ByNavn,
                    tgt.Postnummer = src.Postnummer,
                    tgt.DokumentType = src.DokumentType,
                    tgt.RegningsModtagerID = src.RegningsModtagerID,
                    tgt.RegningsModtagerID_Type = src.RegningsModtagerID_Type,
                    tgt.KøbsOrdreNummer = src.KøbsOrdreNummer,
                    tgt.SalgsOrdreNummer = src.SalgsOrdreNummer,
                    tgt.KundeIdentification = src.KundeIdentification,
                    tgt.KundeIdentification_Type = src.KundeIdentification_Type,
                    tgt.DimensionKontoStreng = src.DimensionKontoStreng,
                    tgt.KundeNavn = src.KundeNavn,
                    tgt.KundeVejNavn = src.KundeVejNavn,
                    tgt.KundeHusNr = src.KundeHusNr,
                    tgt.KundeByNavn = src.KundeByNavn,
                    tgt.KundePostNr = src.KundePostNr,
                    tgt.KundeLandeKode = src.KundeLandeKode,
                    tgt.KundeReferenceID = src.KundeReferenceID,
                    tgt.KundeReferenceNavn = src.KundeReferenceNavn,
                    tgt.FakturaUdstederIdentifikation = src.FakturaUdstederIdentifikation,
                    tgt.FakturaUdstederIdentifikation_Type = src.FakturaUdstederIdentifikation_Type,
                    tgt.FakturaUdstederNavn = src.FakturaUdstederNavn,
                    tgt.FakturaUdstederVejNavn_Vareafsendelse = src.FakturaUdstederVejNavn_Vareafsendelse,
                    tgt.FakturaUdstederHusNr_Vareafsendelse = src.FakturaUdstederHusNr_Vareafsendelse,
                    tgt.FakturaUdstederByNavn_Vareafsendelse = src.FakturaUdstederByNavn_Vareafsendelse,
                    tgt.FakturaUdstederPostNr_Vareafsendelse = src.FakturaUdstederPostNr_Vareafsendelse,
                    tgt.FakturaUdstederLandKode_Vareafsendelse = src.FakturaUdstederLandKode_Vareafsendelse,
                    tgt.FakturaUdstederCVRNummer = src.FakturaUdstederCVRNummer,
                    tgt.FakturaUdstederCVRNummer_Type = src.FakturaUdstederCVRNummer_Type,
                    tgt.FakturaUdstederReferenceIdenrifikation = src.FakturaUdstederReferenceIdenrifikation,
                    tgt.FakturaUdstederReferenceNavn = src.FakturaUdstederReferenceNavn,
                    tgt.FakturaUdstederReferenceTelefonNr = src.FakturaUdstederReferenceTelefonNr,
                    tgt.FakturaUdstederReferenceEMail = src.FakturaUdstederReferenceEMail,
                    tgt.KortArtsKode = src.KortArtsKode,
                    tgt.BetalingsType = src.BetalingsType,
                    tgt.BetalingsID = src.BetalingsID,
                    tgt.Kreditnummer = src.Kreditnummer,
                    tgt.Kontonummer = src.Kontonummer,
                    tgt.Kontotype = src.Kontotype,
                    tgt.AfdelingsID = src.AfdelingsID,
                    tgt.ToldKurs = src.ToldKurs,
                    tgt.ToldKurs_Type = src.ToldKurs_Type,
                    tgt.KundeReferenceTelefonNr = src.KundeReferenceTelefonNr,
                    tgt.KundeReferenceFaxNr = src.KundeReferenceFaxNr,
                    tgt.KundeReferenceEMail = src.KundeReferenceEMail,
                    tgt.KundeReferenceRolle = src.KundeReferenceRolle,
                    tgt.Juridisk_KundeIdentification = src.Juridisk_KundeIdentification,
                    tgt.Juridisk_KundeIdentification_Type = src.Juridisk_KundeIdentification_Type,
                    tgt.Juridisk_DimensionKontoStreng = src.Juridisk_DimensionKontoStreng,
                    tgt.Juridisk_KundeNavn = src.Juridisk_KundeNavn,
                    tgt.Juridisk_KundeVejNavn = src.Juridisk_KundeVejNavn,
                    tgt.Juridisk_KundeHusNr = src.Juridisk_KundeHusNr,
                    tgt.Juridisk_KundeByNavn = src.Juridisk_KundeByNavn,
                    tgt.Juridisk_KundePostNr = src.Juridisk_KundePostNr,
                    tgt.Juridisk_KundeLandeKode = src.Juridisk_KundeLandeKode,
                    tgt.Juridisk_KundeReferenceID = src.Juridisk_KundeReferenceID,
                    tgt.Juridisk_KundeReferenceNavn = src.Juridisk_KundeReferenceNavn,
                    tgt.Juridisk_KundeReferenceTelefonNr = src.Juridisk_KundeReferenceTelefonNr,
                    tgt.Juridisk_KundeReferenceFaxNr = src.Juridisk_KundeReferenceFaxNr,
                    tgt.Juridisk_KundeReferenceEMail = src.Juridisk_KundeReferenceEMail,
                    tgt.Juridisk_KundeReferenceRolle = src.Juridisk_KundeReferenceRolle,
                    tgt.FakturaUdstederAdresseID_Vareafsendelse = src.FakturaUdstederAdresseID_Vareafsendelse,
                    tgt.FakturaUdstederReferenceFaxNr = src.FakturaUdstederReferenceFaxNr,
                    tgt.BankID = src.BankID,
                    tgt.BankNavn = src.BankNavn,
                    tgt.Kontantrabatsats = src.Kontantrabatsats,
                    tgt.Strafrentesats = src.Strafrentesats,
                    tgt.KontantRabatDato = src.KontantRabatDato,
                    tgt.StartRenteDato = src.StartRenteDato,
                    tgt.ZeroRated_AfgiftsKategori = src.ZeroRated_AfgiftsKategori,
                    tgt.ZeroRated_AfgiftsKategoriProcent = src.ZeroRated_AfgiftsKategoriProcent,
                    tgt.GebyrRabatAarsag = src.GebyrRabatAarsag,
                    tgt.GebyrRabatKategori = src.GebyrRabatKategori,
                    tgt.GebyrRabatFinanskonto = src.GebyrRabatFinanskonto,
                    tgt.GebyrTillaegAarsag = src.GebyrTillaegAarsag,
                    tgt.GebyrTillaegKategori = src.GebyrTillaegKategori,
                    tgt.GebyrTillaegFinanskonto = src.GebyrTillaegFinanskonto,
                    tgt.statistic_invoice_header_id = src.statistic_invoice_header_id,
                    tgt.Header_DW_LoadTimestamp = src.Header_DW_LoadTimestamp,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Faktura, FakturaNr, FakturaValutaKode, FirmaNavn, LeverandorCVR,
                    Adresse, ByNavn, Postnummer, DokumentType,
                    RegningsModtagerID, RegningsModtagerID_Type, KøbsOrdreNummer, SalgsOrdreNummer,
                    KundeIdentification, KundeIdentification_Type, DimensionKontoStreng,
                    KundeNavn, KundeVejNavn, KundeHusNr, KundeByNavn, KundePostNr, KundeLandeKode,
                    KundeReferenceID, KundeReferenceNavn,
                    FakturaUdstederIdentifikation, FakturaUdstederIdentifikation_Type, FakturaUdstederNavn,
                    FakturaUdstederVejNavn_Vareafsendelse, FakturaUdstederHusNr_Vareafsendelse,
                    FakturaUdstederByNavn_Vareafsendelse, FakturaUdstederPostNr_Vareafsendelse,
                    FakturaUdstederLandKode_Vareafsendelse,
                    FakturaUdstederCVRNummer, FakturaUdstederCVRNummer_Type,
                    FakturaUdstederReferenceIdenrifikation, FakturaUdstederReferenceNavn,
                    FakturaUdstederReferenceTelefonNr, FakturaUdstederReferenceEMail,
                    KortArtsKode, BetalingsType, BetalingsID, Kreditnummer, Kontonummer, Kontotype,
                    AfdelingsID, ToldKurs, ToldKurs_Type,
                    KundeReferenceTelefonNr, KundeReferenceFaxNr, KundeReferenceEMail, KundeReferenceRolle,
                    Juridisk_KundeIdentification, Juridisk_KundeIdentification_Type,
                    Juridisk_DimensionKontoStreng,
                    Juridisk_KundeNavn, Juridisk_KundeVejNavn, Juridisk_KundeHusNr,
                    Juridisk_KundeByNavn, Juridisk_KundePostNr, Juridisk_KundeLandeKode,
                    Juridisk_KundeReferenceID, Juridisk_KundeReferenceNavn,
                    Juridisk_KundeReferenceTelefonNr, Juridisk_KundeReferenceFaxNr,
                    Juridisk_KundeReferenceEMail, Juridisk_KundeReferenceRolle,
                    FakturaUdstederAdresseID_Vareafsendelse, FakturaUdstederReferenceFaxNr,
                    BankID, BankNavn, Kontantrabatsats, Strafrentesats,
                    KontantRabatDato, StartRenteDato,
                    ZeroRated_AfgiftsKategori, ZeroRated_AfgiftsKategoriProcent,
                    GebyrRabatAarsag, GebyrRabatKategori, GebyrRabatFinanskonto,
                    GebyrTillaegAarsag, GebyrTillaegKategori, GebyrTillaegFinanskonto,
                    statistic_invoice_header_id, Header_DW_LoadTimestamp,
                    DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_Faktura, src.FakturaNr, src.FakturaValutaKode, src.FirmaNavn, src.LeverandorCVR,
                    src.Adresse, src.ByNavn, src.Postnummer, src.DokumentType,
                    src.RegningsModtagerID, src.RegningsModtagerID_Type, src.KøbsOrdreNummer, src.SalgsOrdreNummer,
                    src.KundeIdentification, src.KundeIdentification_Type, src.DimensionKontoStreng,
                    src.KundeNavn, src.KundeVejNavn, src.KundeHusNr, src.KundeByNavn, src.KundePostNr, src.KundeLandeKode,
                    src.KundeReferenceID, src.KundeReferenceNavn,
                    src.FakturaUdstederIdentifikation, src.FakturaUdstederIdentifikation_Type, src.FakturaUdstederNavn,
                    src.FakturaUdstederVejNavn_Vareafsendelse, src.FakturaUdstederHusNr_Vareafsendelse,
                    src.FakturaUdstederByNavn_Vareafsendelse, src.FakturaUdstederPostNr_Vareafsendelse,
                    src.FakturaUdstederLandKode_Vareafsendelse,
                    src.FakturaUdstederCVRNummer, src.FakturaUdstederCVRNummer_Type,
                    src.FakturaUdstederReferenceIdenrifikation, src.FakturaUdstederReferenceNavn,
                    src.FakturaUdstederReferenceTelefonNr, src.FakturaUdstederReferenceEMail,
                    src.KortArtsKode, src.BetalingsType, src.BetalingsID, src.Kreditnummer, src.Kontonummer, src.Kontotype,
                    src.AfdelingsID, src.ToldKurs, src.ToldKurs_Type,
                    src.KundeReferenceTelefonNr, src.KundeReferenceFaxNr, src.KundeReferenceEMail, src.KundeReferenceRolle,
                    src.Juridisk_KundeIdentification, src.Juridisk_KundeIdentification_Type,
                    src.Juridisk_DimensionKontoStreng,
                    src.Juridisk_KundeNavn, src.Juridisk_KundeVejNavn, src.Juridisk_KundeHusNr,
                    src.Juridisk_KundeByNavn, src.Juridisk_KundePostNr, src.Juridisk_KundeLandeKode,
                    src.Juridisk_KundeReferenceID, src.Juridisk_KundeReferenceNavn,
                    src.Juridisk_KundeReferenceTelefonNr, src.Juridisk_KundeReferenceFaxNr,
                    src.Juridisk_KundeReferenceEMail, src.Juridisk_KundeReferenceRolle,
                    src.FakturaUdstederAdresseID_Vareafsendelse, src.FakturaUdstederReferenceFaxNr,
                    src.BankID, src.BankNavn, src.Kontantrabatsats, src.Strafrentesats,
                    src.KontantRabatDato, src.StartRenteDato,
                    src.ZeroRated_AfgiftsKategori, src.ZeroRated_AfgiftsKategoriProcent,
                    src.GebyrRabatAarsag, src.GebyrRabatKategori, src.GebyrRabatFinanskonto,
                    src.GebyrTillaegAarsag, src.GebyrTillaegKategori, src.GebyrTillaegFinanskonto,
                    src.statistic_invoice_header_id, src.Header_DW_LoadTimestamp,
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Faktura_Attributes")

            cursor.execute("UPDATE STATISTICS [dim].[Faktura]")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_dim_rows,
                SUM(CASE WHEN DW_Timestamp_Update IS NULL THEN 1 ELSE 0 END) AS inserts_only,
                SUM(CASE WHEN DW_Timestamp_Update IS NOT NULL THEN 1 ELSE 0 END) AS updated,
                COUNT(FakturaNr) AS has_faktura_nr,
                COUNT(FirmaNavn) AS has_firma_navn
            FROM [dim].[Faktura]
            WHERE DW_EK_Faktura <> -1
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
                f"Coverage: {has_faktura_nr}/{total_dim_rows} with FakturaNr, "
                f"{has_firma_navn}/{total_dim_rows} with FirmaNavn"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "inserts_only": inserts_only,
            "updated": updated_count,
            "has_faktura_nr": has_faktura_nr,
            "has_firma_navn": has_firma_navn,
            "table": "dim.Faktura",
            "source_tables": "map.Faktura + stg.dim_faktura_mysupply",
            "scd_type": "Type 1 (overwrite)",
        }
    )
