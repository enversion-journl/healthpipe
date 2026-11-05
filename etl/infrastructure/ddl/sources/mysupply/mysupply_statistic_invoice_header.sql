-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: MySupply (invoice header + dependents)
--
-- 1:1 reflection of mySupply_Statistic source tables. UPSERT-merged on the
-- entity key `id`. Delta filtering happens at extract time via the DFM
-- pipeline using TRY_CAST([FakturaDato] AS DATETIME2) with a 7-day lookback;
-- dependents filter via correlated subquery to header.
--
-- Source extracts populate ext.*_mysupply_dfm (postfixed during cutover from
-- the legacy partition-based extract that uses ext.*_mysupply). When the
-- legacy extract is retired, ext.*_mysupply_dfm should be renamed via a
-- migration in infrastructure/ddl/migrations/.
--
-- Note: The two allowance_charge tables are not yet present in
-- docs/source_systems/mysupply/mysupply_schema_complete.sql; their column
-- definitions are taken from the canonical source CREATE TABLE statements
-- provided by the data owner.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.statistic_invoice_header_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_statistic_invoice_header'
)
BEGIN
    CREATE TABLE [arc].[mysupply_statistic_invoice_header] (
        [id]                                                          INT             NOT NULL,
        [metadata_id]                                                 INT             NOT NULL,
        [FakturaNr]                                                   NVARCHAR(255)   NULL,
        [FakturaDato]                                                 NVARCHAR(255)   NULL,
        [FakturaType]                                                 NVARCHAR(255)   NULL,
        [FakturaValutaKode]                                           NVARCHAR(255)   NULL,
        [RegningsModtagerID]                                          NVARCHAR(255)   NULL,
        [RegningsModtagerID_Type]                                     NVARCHAR(255)   NULL,
        [KøbsOrdreNummer]                                             NVARCHAR(255)   NULL,
        [SalgsOrdreNummer]                                            NVARCHAR(255)   NULL,
        [OrdreDato]                                                   NVARCHAR(255)   NULL,
        [KundeIdentification]                                         NVARCHAR(255)   NULL,
        [KundeIdentification_Type]                                    NVARCHAR(255)   NULL,
        [DimensionKontoStreng]                                        NVARCHAR(255)   NULL,
        [KundeNavn]                                                   NVARCHAR(255)   NULL,
        [KundeAdresseTypeID]                                          NVARCHAR(255)   NULL,
        [KundeVejNavn]                                                NVARCHAR(255)   NULL,
        [KundeHusNr]                                                  NVARCHAR(255)   NULL,
        [KundeByNavn]                                                 NVARCHAR(255)   NULL,
        [KundePostNr]                                                 NVARCHAR(255)   NULL,
        [KundeLandeKode]                                              NVARCHAR(255)   NULL,
        [KundeReferenceID]                                            NVARCHAR(512)   NULL,
        [KundeReferenceNavn]                                          NVARCHAR(255)   NULL,
        [KundeReferenceTelefonNr]                                     NVARCHAR(255)   NULL,
        [KundeReferenceFaxNr]                                         NVARCHAR(255)   NULL,
        [KundeReferenceE-Mail]                                        NVARCHAR(255)   NULL,
        [KundeReferenceRolle]                                         NVARCHAR(255)   NULL,
        [KundeReferenceID_Bogholder]                                  NVARCHAR(255)   NULL,
        [KundeReferenceNavn_Bogholder]                                NVARCHAR(255)   NULL,
        [KundeReferenceTelefonNr_Bogholder]                           NVARCHAR(255)   NULL,
        [KundeReferenceFaxNr_Bogholder]                               NVARCHAR(255)   NULL,
        [KundeReferenceE-Mail_Bogholder]                              NVARCHAR(255)   NULL,
        [KundeReferenceRolle_Bogholder]                               NVARCHAR(255)   NULL,
        [KundeReferenceID_Budgetansvarlig]                            NVARCHAR(255)   NULL,
        [KundeReferenceNavn_Budgetansvarlig]                          NVARCHAR(255)   NULL,
        [KundeReferenceTelefonNr_Budgetansvarlig]                     NVARCHAR(255)   NULL,
        [KundeReferenceFaxNr_Budgetansvarlig]                         NVARCHAR(255)   NULL,
        [KundeReferenceE-Mail_Budgetansvarlig]                        NVARCHAR(255)   NULL,
        [KundeReferenceRolle_Budgetansvarlig]                         NVARCHAR(255)   NULL,
        [KundeReferenceID_Rekvirent]                                  NVARCHAR(255)   NULL,
        [KundeReferenceNavn_Rekvirent]                                NVARCHAR(255)   NULL,
        [KundeReferenceTelefonNr_Rekvirent]                           NVARCHAR(255)   NULL,
        [KundeReferenceFaxNr_Rekvirent]                               NVARCHAR(255)   NULL,
        [KundeReferenceE-Mail_Rekvirent]                              NVARCHAR(255)   NULL,
        [KundeReferenceRolle_Rekvirent]                               NVARCHAR(255)   NULL,
        [Juridisk_KundeIdentification]                                NVARCHAR(255)   NULL,
        [Juridisk_KundeIdentification_Type]                           NVARCHAR(255)   NULL,
        [Juridisk_DimensionKontoStreng]                               NVARCHAR(255)   NULL,
        [Juridisk_KundeNavn]                                          NVARCHAR(255)   NULL,
        [Juridisk_KundeAdresseTypeID]                                 NVARCHAR(255)   NULL,
        [Juridisk_KundeVejNavn]                                       NVARCHAR(255)   NULL,
        [Juridisk_KundeHusNr]                                         NVARCHAR(255)   NULL,
        [Juridisk_KundeByNavn]                                        NVARCHAR(255)   NULL,
        [Juridisk_KundePostNr]                                        NVARCHAR(255)   NULL,
        [Juridisk_KundeLandeKode]                                     NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceID]                                   NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceNavn]                                 NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceTelefonNr]                            NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceFaxNr]                                NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceE-Mail]                               NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceRolle]                                NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceID_Bogholder]                         NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceNavn_Bogholder]                       NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceTelefonNr_Bogholder]                  NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceFaxNr_Bogholder]                      NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceE-Mail_Bogholder]                     NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceRolle_Bogholder]                      NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceID_Budgetansvarlig]                   NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceNavn_Budgetansvarlig]                 NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceTelefonNr_Budgetansvarlig]            NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceFaxNr_Budgetansvarlig]                NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceE-Mail_Budgetansvarlig]               NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceRolle_Budgetansvarlig]                NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceID_Rekvirent]                         NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceNavn_Rekvirent]                       NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceTelefonNr_Rekvirent]                  NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceFaxNr_Rekvirent]                      NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceE-Mail_Rekvirent]                     NVARCHAR(255)   NULL,
        [Juridisk_KundeReferenceRolle_Rekvirent]                      NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsIdentifikation]               NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsIdentifikation_Type]          NVARCHAR(255)   NULL,
        [DestinationParty_KundeNavn]                                  NVARCHAR(255)   NULL,
        [DestinationParty_KundeReferenceIdentifikation]               NVARCHAR(255)   NULL,
        [DestinationParty_KundeReferenceNavn]                         NVARCHAR(255)   NULL,
        [DestinationParty_KundeReferenceTelefonNr]                    NVARCHAR(255)   NULL,
        [DestinationParty_KundeReferenceFaxNr]                        NVARCHAR(255)   NULL,
        [DestinationParty_KundeReferenceEMail]                        NVARCHAR(255)   NULL,
        [DestinationParty_KundeSprog]                                 NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsAdresseID]                    NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsVejNavn]                      NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsHusNr]                        NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsByNavn]                       NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsPostNr]                       NVARCHAR(255)   NULL,
        [DestinationParty_KundeLeveringsLandeKode]                    NVARCHAR(255)   NULL,
        [FakturaUdstederIdentifikation]                               NVARCHAR(255)   NULL,
        [FakturaUdstederIdentifikation_Type]                          NVARCHAR(255)   NULL,
        [KreditorIdentifikation]                                      NVARCHAR(255)   NULL,
        [FakturaUdstederNavn]                                         NVARCHAR(255)   NULL,
        [FakturaUdstederAdresseID_Vareafsendelse]                     NVARCHAR(255)   NULL,
        [FakturaUdstederVejNavn_Vareafsendelse]                       NVARCHAR(255)   NULL,
        [FakturaUdstederHusNr_Vareafsendelse]                         NVARCHAR(255)   NULL,
        [FakturaUdstederByNavn_Vareafsendelse]                        NVARCHAR(255)   NULL,
        [FakturaUdstederPostNr_Vareafsendelse]                        NVARCHAR(255)   NULL,
        [FakturaUdstederLandKode_Vareafsendelse]                      NVARCHAR(255)   NULL,
        [FakturaUdstederAdresseID_Betaling]                           NVARCHAR(255)   NULL,
        [FakturaUdstederVejNavn_Betaling]                             NVARCHAR(255)   NULL,
        [FakturaUdstederHusNr_Betaling]                               NVARCHAR(255)   NULL,
        [FakturaUdstederByNavn_Betaling]                              NVARCHAR(255)   NULL,
        [FakturaUdstederPostNr_Betaling]                              NVARCHAR(255)   NULL,
        [FakturaUdstederLandKode_Betaling]                            NVARCHAR(255)   NULL,
        [FakturaUdstederCVRNummer]                                    NVARCHAR(255)   NULL,
        [FakturaUdstederCVRNummer_Type]                               NVARCHAR(255)   NULL,
        [FakturaUdstederReferenceIdenrifikation]                      NVARCHAR(255)   NULL,
        [FakturaUdstederReferenceNavn]                                NVARCHAR(255)   NULL,
        [FakturaUdstederReferenceTelefonNr]                           NVARCHAR(255)   NULL,
        [FakturaUdstederReferenceFaxNr]                               NVARCHAR(255)   NULL,
        [FakturaUdstederReferenceEMail]                               NVARCHAR(255)   NULL,
        [KortArtsKode]                                                NVARCHAR(255)   NULL,
        [BetalingsDato]                                               NVARCHAR(255)   NULL,
        [BetalingsType]                                               NVARCHAR(255)   NULL,
        [BetalingsID]                                                 NVARCHAR(255)   NULL,
        [Kreditnummer]                                                NVARCHAR(255)   NULL,
        [Kontonummer]                                                 NVARCHAR(255)   NULL,
        [Kontotype]                                                   NVARCHAR(255)   NULL,
        [AfdelingsID]                                                 NVARCHAR(255)   NULL,
        [BankID]                                                      NVARCHAR(255)   NULL,
        [BankNavn]                                                    NVARCHAR(255)   NULL,
        [Batalingsbetingelser]                                        NVARCHAR(255)   NULL,
        [ToldKurs]                                                    NVARCHAR(255)   NULL,
        [ToldKurs_Type]                                               NVARCHAR(255)   NULL,
        [Kontantrabatsats]                                            NVARCHAR(255)   NULL,
        [Strafrentesats]                                              NVARCHAR(255)   NULL,
        [KontantRabatDato]                                            NVARCHAR(255)   NULL,
        [StartRenteDato]                                              NVARCHAR(255)   NULL,
        [VAT_AfgiftsKategoriKode]                                     NVARCHAR(255)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag]                  NVARCHAR(255)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta]           NVARCHAR(255)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsBeløb]                     NVARCHAR(255)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta]              NVARCHAR(255)   NULL,
        [VAT_AfgiftsKategori]                                         NVARCHAR(255)   NULL,
        [VAT_AfgiftsKategoriProcent]                                  NVARCHAR(255)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag]               NVARCHAR(255)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag_Valuta]        NVARCHAR(255)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsBeløb]                  NVARCHAR(255)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsBeløb_Valuta]           NVARCHAR(255)   NULL,
        [ZERO-RATED_AfgiftsKategoriKode]                              NVARCHAR(255)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag]           NVARCHAR(255)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta]    NVARCHAR(255)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb]              NVARCHAR(255)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta]       NVARCHAR(255)   NULL,
        [ZERO-RATED_AfgiftsKategori]                                  NVARCHAR(255)   NULL,
        [ZERO-RATED_AfgiftsKategoriProcent]                           NVARCHAR(255)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsGrundlag]        NVARCHAR(255)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsGrundlag_Valuta] NVARCHAR(255)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsBeløb]           NVARCHAR(255)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsBeløb_Valuta]    NVARCHAR(255)   NULL,
        [FakturaLinjeTotal]                                           NVARCHAR(255)   NULL,
        [FakturaLinjeTotal_Valuta]                                    NVARCHAR(255)   NULL,
        [FakturaTotal]                                                NVARCHAR(255)   NULL,
        [FakturaTotal_Valuta]                                         NVARCHAR(255)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_statistic_invoice_header]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_header_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_header]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_header_dw_timestamp_update]
        ON [arc].[mysupply_statistic_invoice_header] ([dw_timestamp_update] DESC);
END
GO
