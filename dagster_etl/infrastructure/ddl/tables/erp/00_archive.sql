-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: lgrbeh from ILS (Warehouse Stock)
-- 1:1 reflection of ILS_pro2_complete.dbo.lgrbeh (26 columns)
-- Insert-only — rows are never updated, snapshots accumulate over time.
-- Used by arc_lgrbeh_ils archive merge asset
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'lgrbeh_ils'
)
BEGIN
    CREATE TABLE [arc].[lgrbeh_ils] (
        -- Source columns (1:1 from dbo.lgrbeh)
        [prrowid]          VARCHAR(36)    NOT NULL,
        [sgh]              VARCHAR(8)     NULL,
        [lgr]              VARCHAR(8)     NULL,
        [vnr]              VARCHAR(24)    NULL,
        [lgrlok]           VARCHAR(16)    NULL,
        [antal]            DECIMAL(18,3)  NULL,
        [batchnr]          VARCHAR(24)    NULL,
        [lnr]              VARCHAR(20)    NULL,
        [udlobdto]         DATETIME       NULL,
        [fralok]           VARCHAR(16)    NULL,
        [sw_egen_prod]     BIT            NULL,
        [sysdato]          DATETIME       NULL,
        [bruger]           VARCHAR(6)     NULL,
        [seqnr]            INT            NULL,
        [lotnr]            VARCHAR(24)    NULL,
        [udlevenh]         VARCHAR(40)    NULL,
        [bem]              VARCHAR(100)   NULL,
        [lvnr##1]          VARCHAR(168)   NULL,
        [lvnr##2]          VARCHAR(168)   NULL,
        [lvnr##3]          VARCHAR(168)   NULL,
        [lvnr##4]          VARCHAR(168)   NULL,
        [lgrbeh_prio]      INT            NULL,
        [sw_erstat]        BIT            NULL,
        [Pro2SrcPDB]       VARCHAR(12)    NULL,
        [pro2created]      DATETIME       NULL,
        [pro2modified]     DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert] DATETIME2  NOT NULL,
        [DW_Timestamp_Update] DATETIME2  NOT NULL
    );
END
GO

-- ============================================================================
-- Archive: afdlgrbeh from ILS (Department Stock)
-- 1:1 reflection of ILS_pro2_complete.dbo.afdlgrbeh (17 columns)
-- Insert-only — rows are never updated, snapshots accumulate over time.
-- Used by arc_afdlgrbeh_ils archive merge asset
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'afdlgrbeh_ils'
)
BEGIN
    CREATE TABLE [arc].[afdlgrbeh_ils] (
        -- Source columns (1:1 from dbo.afdlgrbeh)
        [prrowid]          VARCHAR(36)    NOT NULL,
        [sgh]              VARCHAR(8)     NULL,
        [lgr]              VARCHAR(8)     NULL,
        [brik]             INT            NULL,
        [rnr]              VARCHAR(16)    NULL,
        [vnr]              VARCHAR(24)    NULL,
        [antal]            DECIMAL(18,3)  NULL,
        [lnr]              VARCHAR(20)    NULL,
        [batchnr]          VARCHAR(24)    NULL,
        [udlobdto]         DATETIME       NULL,
        [lotnr]            VARCHAR(24)    NULL,
        [sysdato]          DATETIME       NULL,
        [bruger]           VARCHAR(6)     NULL,
        [seqnr]            INT            NULL,
        [Pro2SrcPDB]       VARCHAR(12)    NULL,
        [pro2created]      DATETIME       NULL,
        [pro2modified]     DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert] DATETIME2  NOT NULL,
        [DW_Timestamp_Update] DATETIME2  NOT NULL
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_lgrbeh_ils_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[lgrbeh_ils]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_lgrbeh_ils_DW_Timestamp_Update]
        ON [arc].[lgrbeh_ils] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_afdlgrbeh_ils_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[afdlgrbeh_ils]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_afdlgrbeh_ils_DW_Timestamp_Update]
        ON [arc].[afdlgrbeh_ils] ([DW_Timestamp_Update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.unspsc_ils  (1:1 from ILS dbo.unspsc)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'unspsc_ils'
)
BEGIN
    CREATE TABLE [arc].[unspsc_ils] (
        -- Source columns (1:1 from dbo.unspsc)
        [prrowid]               VARCHAR(36)    NOT NULL,
        [unspsc]                NVARCHAR(16)   NULL,
        [betegn]                NVARCHAR(80)   NULL,
        [un_name]               NVARCHAR(80)   NULL,
        [sysdato]               DATETIME2      NULL,
        [bruger]                NVARCHAR(6)    NULL,
        [seqnr]                 INT            NULL,
        [aktiv]                 BIT            NULL,
        [version]               NVARCHAR(16)   NULL,
        [Pro2SrcPDB]            NVARCHAR(12)   NULL,
        [pro2created]           DATETIME2      NULL,
        [pro2modified]          DATETIME2      NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_unspsc_ils] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_unspsc_ils_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[unspsc_ils]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_unspsc_ils_sysdato]
        ON [arc].[unspsc_ils] ([sysdato] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_unspsc_ils_unspsc'
      AND object_id = OBJECT_ID(N'[arc].[unspsc_ils]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_unspsc_ils_unspsc]
        ON [arc].[unspsc_ils] ([unspsc])
        WHERE [unspsc] IS NOT NULL;
END
GO

-- ----------------------------------------------------------------------------
-- arc.vare_ils  (1:1 from ILS dbo.vare — full schema mirror)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'vare_ils'
)
BEGIN
    CREATE TABLE [arc].[vare_ils] (
        [prrowid]               VARCHAR(36)    NOT NULL,
        [vnr]                   NVARCHAR(24)   NULL,
        [navn]                  NVARCHAR(80)   NULL,
        [navn2]                 NVARCHAR(80)   NULL,
        [varestr]               NVARCHAR(40)   NULL,
        [navn_conc]             NVARCHAR(100)  NULL,
        [basisenh]              NVARCHAR(10)   NULL,
        [faelvnr]               NVARCHAR(24)   NULL,
        [faelnavn]              NVARCHAR(80)   NULL,
        [faelnavn2]             NVARCHAR(80)   NULL,
        [ean_vnr]               NVARCHAR(36)   NULL,
        [vareart]               NVARCHAR(12)   NULL,
        [varegrp]               NVARCHAR(8)    NULL,
        [varetype]              NVARCHAR(12)   NULL,
        [varelis]               NVARCHAR(12)   NULL,
        [momskd]                NVARCHAR(4)    NULL,
        [afgiftkd]              NVARCHAR(4)    NULL,
        [miljo]                 NVARCHAR(16)   NULL,
        [ansvar]                NVARCHAR(20)   NULL,
        [adr_kode]              NVARCHAR(12)   NULL,
        [un_nr]                 NVARCHAR(12)   NULL,
        [sik_fak]               DECIMAL(17,2)  NULL,
        [holddage]              INT            NULL,
        [billed]                NVARCHAR(104)  NULL,
        [sw_edivare]            BIT            NULL,
        [sw_udlkontr]           BIT            NULL,
        [sw_batch]              BIT            NULL,
        [sw_ovf_gs]             BIT            NULL,
        [sw_opret_gs]           BIT            NULL,
        [sw_ovf_dk200]          BIT            NULL,
        [sw_opret_dk2000]       BIT            NULL,
        [sw_synk]               BIT            NULL,
        [sw_usyn_gs]            BIT            NULL,
        [sw_gs]                 BIT            NULL,
        [aktiv]                 BIT            NULL,
        [oprdto]                DATETIME2      NULL,
        [oprtid]                INT            NULL,
        [oprbruger]             NVARCHAR(6)    NULL,
        [billedli]              NVARCHAR(140)  NULL,
        [sw_web]                BIT            NULL,
        [unspc]                 NVARCHAR(16)   NULL,
        [sampak]                BIT            NULL,
        [txt1]                  NVARCHAR(80)   NULL,
        [txt2]                  NVARCHAR(80)   NULL,
        [bruger]                NVARCHAR(6)    NULL,
        [sysdato]               DATETIME2      NULL,
        [seqnr]                 INT            NULL,
        [varetype2]             NVARCHAR(12)   NULL,
        [skat]                  NVARCHAR(10)   NULL,
        [synonym_]              NVARCHAR(120)  NULL,
        [kategori]              NVARCHAR(40)   NULL,
        [sysklk]                INT            NULL,
        [varecpv]               NVARCHAR(20)   NULL,
        [pak1]                  NVARCHAR(40)   NULL,
        [pak2]                  NVARCHAR(40)   NULL,
        [pak3]                  NVARCHAR(40)   NULL,
        [markering]             NVARCHAR(80)   NULL,
        [sw_steril]             BIT            NULL,
        [navneng]               NVARCHAR(80)   NULL,
        [navneng2]              NVARCHAR(80)   NULL,
        [ejlukkes]              BIT            NULL,
        [sw_farlig]             BIT            NULL,
        [farligfarve]           NVARCHAR(10)   NULL,
        [pvnr]                  NVARCHAR(60)   NULL,
        [pvarenavn]             NVARCHAR(160)  NULL,
        [pvarestr]              NVARCHAR(140)  NULL,
        [pnavn]                 NVARCHAR(120)  NULL,
        [implantatside]         NVARCHAR(2)    NULL,
        [extordreenh]           NVARCHAR(40)   NULL,
        [unspsc2]               NVARCHAR(16)   NULL,
        [paktyp]                NVARCHAR(6)    NULL,
        [intrastat]             NVARCHAR(40)   NULL,
        [unspscint]             NVARCHAR(16)   NULL,
        [udbkat]                NVARCHAR(40)   NULL,
        [udbegnet]              NVARCHAR(6)    NULL,
        [appafh]                BIT            NULL,
        [steril]                NVARCHAR(24)   NULL,
        [fraafttype]            NVARCHAR(40)   NULL,
        [sw_indberet]           BIT            NULL,
        [Pro2SrcPDB]            NVARCHAR(12)   NULL,
        [pro2created]           DATETIME2      NULL,
        [pro2modified]          DATETIME2      NULL,
        [pmdmid]                INT            NULL,
        [sw_ikkeeu]             BIT            NULL,
        [sw_sygehusapotek]      BIT            NULL,
        [sw_kritiskvareliste]   BIT            NULL,
        [sw_positivliste]       BIT            NULL,
        [sw_hjemmepatient]      BIT            NULL,
        [sw_obstekst]           BIT            NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_vare_ils] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_vare_ils_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[vare_ils]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_vare_ils_sysdato]
        ON [arc].[vare_ils] ([sysdato] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_vare_ils_vnr'
      AND object_id = OBJECT_ID(N'[arc].[vare_ils]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_vare_ils_vnr]
        ON [arc].[vare_ils] ([vnr])
        WHERE [vnr] IS NOT NULL;
END
GO

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
        [FakturaNr]                                                   NVARCHAR(MAX)   NULL,
        [FakturaDato]                                                 NVARCHAR(MAX)   NULL,
        [FakturaType]                                                 NVARCHAR(MAX)   NULL,
        [FakturaValutaKode]                                           NVARCHAR(MAX)   NULL,
        [RegningsModtagerID]                                          NVARCHAR(MAX)   NULL,
        [RegningsModtagerID_Type]                                     NVARCHAR(MAX)   NULL,
        [KøbsOrdreNummer]                                             NVARCHAR(MAX)   NULL,
        [SalgsOrdreNummer]                                            NVARCHAR(MAX)   NULL,
        [OrdreDato]                                                   NVARCHAR(MAX)   NULL,
        [KundeIdentification]                                         NVARCHAR(MAX)   NULL,
        [KundeIdentification_Type]                                    NVARCHAR(MAX)   NULL,
        [DimensionKontoStreng]                                        NVARCHAR(MAX)   NULL,
        [KundeNavn]                                                   NVARCHAR(MAX)   NULL,
        [KundeAdresseTypeID]                                          NVARCHAR(MAX)   NULL,
        [KundeVejNavn]                                                NVARCHAR(MAX)   NULL,
        [KundeHusNr]                                                  NVARCHAR(MAX)   NULL,
        [KundeByNavn]                                                 NVARCHAR(MAX)   NULL,
        [KundePostNr]                                                 NVARCHAR(MAX)   NULL,
        [KundeLandeKode]                                              NVARCHAR(MAX)   NULL,
        [KundeReferenceID]                                            NVARCHAR(MAX)   NULL,
        [KundeReferenceNavn]                                          NVARCHAR(MAX)   NULL,
        [KundeReferenceTelefonNr]                                     NVARCHAR(MAX)   NULL,
        [KundeReferenceFaxNr]                                         NVARCHAR(MAX)   NULL,
        [KundeReferenceE-Mail]                                        NVARCHAR(MAX)   NULL,
        [KundeReferenceRolle]                                         NVARCHAR(MAX)   NULL,
        [KundeReferenceID_Bogholder]                                  NVARCHAR(MAX)   NULL,
        [KundeReferenceNavn_Bogholder]                                NVARCHAR(MAX)   NULL,
        [KundeReferenceTelefonNr_Bogholder]                           NVARCHAR(MAX)   NULL,
        [KundeReferenceFaxNr_Bogholder]                               NVARCHAR(MAX)   NULL,
        [KundeReferenceE-Mail_Bogholder]                              NVARCHAR(MAX)   NULL,
        [KundeReferenceRolle_Bogholder]                               NVARCHAR(MAX)   NULL,
        [KundeReferenceID_Budgetansvarlig]                            NVARCHAR(MAX)   NULL,
        [KundeReferenceNavn_Budgetansvarlig]                          NVARCHAR(MAX)   NULL,
        [KundeReferenceTelefonNr_Budgetansvarlig]                     NVARCHAR(MAX)   NULL,
        [KundeReferenceFaxNr_Budgetansvarlig]                         NVARCHAR(MAX)   NULL,
        [KundeReferenceE-Mail_Budgetansvarlig]                        NVARCHAR(MAX)   NULL,
        [KundeReferenceRolle_Budgetansvarlig]                         NVARCHAR(MAX)   NULL,
        [KundeReferenceID_Rekvirent]                                  NVARCHAR(MAX)   NULL,
        [KundeReferenceNavn_Rekvirent]                                NVARCHAR(MAX)   NULL,
        [KundeReferenceTelefonNr_Rekvirent]                           NVARCHAR(MAX)   NULL,
        [KundeReferenceFaxNr_Rekvirent]                               NVARCHAR(MAX)   NULL,
        [KundeReferenceE-Mail_Rekvirent]                              NVARCHAR(MAX)   NULL,
        [KundeReferenceRolle_Rekvirent]                               NVARCHAR(MAX)   NULL,
        [Juridisk_KundeIdentification]                                NVARCHAR(MAX)   NULL,
        [Juridisk_KundeIdentification_Type]                           NVARCHAR(MAX)   NULL,
        [Juridisk_DimensionKontoStreng]                               NVARCHAR(MAX)   NULL,
        [Juridisk_KundeNavn]                                          NVARCHAR(MAX)   NULL,
        [Juridisk_KundeAdresseTypeID]                                 NVARCHAR(MAX)   NULL,
        [Juridisk_KundeVejNavn]                                       NVARCHAR(MAX)   NULL,
        [Juridisk_KundeHusNr]                                         NVARCHAR(MAX)   NULL,
        [Juridisk_KundeByNavn]                                        NVARCHAR(MAX)   NULL,
        [Juridisk_KundePostNr]                                        NVARCHAR(MAX)   NULL,
        [Juridisk_KundeLandeKode]                                     NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceID]                                   NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceNavn]                                 NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceTelefonNr]                            NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceFaxNr]                                NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceE-Mail]                               NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceRolle]                                NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceID_Bogholder]                         NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceNavn_Bogholder]                       NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceTelefonNr_Bogholder]                  NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceFaxNr_Bogholder]                      NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceE-Mail_Bogholder]                     NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceRolle_Bogholder]                      NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceID_Budgetansvarlig]                   NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceNavn_Budgetansvarlig]                 NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceTelefonNr_Budgetansvarlig]            NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceFaxNr_Budgetansvarlig]                NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceE-Mail_Budgetansvarlig]               NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceRolle_Budgetansvarlig]                NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceID_Rekvirent]                         NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceNavn_Rekvirent]                       NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceTelefonNr_Rekvirent]                  NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceFaxNr_Rekvirent]                      NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceE-Mail_Rekvirent]                     NVARCHAR(MAX)   NULL,
        [Juridisk_KundeReferenceRolle_Rekvirent]                      NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsIdentifikation]               NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsIdentifikation_Type]          NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeNavn]                                  NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeReferenceIdentifikation]               NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeReferenceNavn]                         NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeReferenceTelefonNr]                    NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeReferenceFaxNr]                        NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeReferenceEMail]                        NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeSprog]                                 NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsAdresseID]                    NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsVejNavn]                      NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsHusNr]                        NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsByNavn]                       NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsPostNr]                       NVARCHAR(MAX)   NULL,
        [DestinationParty_KundeLeveringsLandeKode]                    NVARCHAR(MAX)   NULL,
        [FakturaUdstederIdentifikation]                               NVARCHAR(MAX)   NULL,
        [FakturaUdstederIdentifikation_Type]                          NVARCHAR(MAX)   NULL,
        [KreditorIdentifikation]                                      NVARCHAR(MAX)   NULL,
        [FakturaUdstederNavn]                                         NVARCHAR(MAX)   NULL,
        [FakturaUdstederAdresseID_Vareafsendelse]                     NVARCHAR(MAX)   NULL,
        [FakturaUdstederVejNavn_Vareafsendelse]                       NVARCHAR(MAX)   NULL,
        [FakturaUdstederHusNr_Vareafsendelse]                         NVARCHAR(MAX)   NULL,
        [FakturaUdstederByNavn_Vareafsendelse]                        NVARCHAR(MAX)   NULL,
        [FakturaUdstederPostNr_Vareafsendelse]                        NVARCHAR(MAX)   NULL,
        [FakturaUdstederLandKode_Vareafsendelse]                      NVARCHAR(MAX)   NULL,
        [FakturaUdstederAdresseID_Betaling]                           NVARCHAR(MAX)   NULL,
        [FakturaUdstederVejNavn_Betaling]                             NVARCHAR(MAX)   NULL,
        [FakturaUdstederHusNr_Betaling]                               NVARCHAR(MAX)   NULL,
        [FakturaUdstederByNavn_Betaling]                              NVARCHAR(MAX)   NULL,
        [FakturaUdstederPostNr_Betaling]                              NVARCHAR(MAX)   NULL,
        [FakturaUdstederLandKode_Betaling]                            NVARCHAR(MAX)   NULL,
        [FakturaUdstederCVRNummer]                                    NVARCHAR(MAX)   NULL,
        [FakturaUdstederCVRNummer_Type]                               NVARCHAR(MAX)   NULL,
        [FakturaUdstederReferenceIdenrifikation]                      NVARCHAR(MAX)   NULL,
        [FakturaUdstederReferenceNavn]                                NVARCHAR(MAX)   NULL,
        [FakturaUdstederReferenceTelefonNr]                           NVARCHAR(MAX)   NULL,
        [FakturaUdstederReferenceFaxNr]                               NVARCHAR(MAX)   NULL,
        [FakturaUdstederReferenceEMail]                               NVARCHAR(MAX)   NULL,
        [KortArtsKode]                                                NVARCHAR(MAX)   NULL,
        [BetalingsDato]                                               NVARCHAR(MAX)   NULL,
        [BetalingsType]                                               NVARCHAR(MAX)   NULL,
        [BetalingsID]                                                 NVARCHAR(MAX)   NULL,
        [Kreditnummer]                                                NVARCHAR(MAX)   NULL,
        [Kontonummer]                                                 NVARCHAR(MAX)   NULL,
        [Kontotype]                                                   NVARCHAR(MAX)   NULL,
        [AfdelingsID]                                                 NVARCHAR(MAX)   NULL,
        [BankID]                                                      NVARCHAR(MAX)   NULL,
        [BankNavn]                                                    NVARCHAR(MAX)   NULL,
        [Batalingsbetingelser]                                        NVARCHAR(MAX)   NULL,
        [ToldKurs]                                                    NVARCHAR(MAX)   NULL,
        [ToldKurs_Type]                                               NVARCHAR(MAX)   NULL,
        [Kontantrabatsats]                                            NVARCHAR(MAX)   NULL,
        [Strafrentesats]                                              NVARCHAR(MAX)   NULL,
        [KontantRabatDato]                                            NVARCHAR(MAX)   NULL,
        [StartRenteDato]                                              NVARCHAR(MAX)   NULL,
        [VAT_AfgiftsKategoriKode]                                     NVARCHAR(MAX)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag]                  NVARCHAR(MAX)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta]           NVARCHAR(MAX)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsBeløb]                     NVARCHAR(MAX)   NULL,
        [VAT_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta]              NVARCHAR(MAX)   NULL,
        [VAT_AfgiftsKategori]                                         NVARCHAR(MAX)   NULL,
        [VAT_AfgiftsKategoriProcent]                                  NVARCHAR(MAX)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag]               NVARCHAR(MAX)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag_Valuta]        NVARCHAR(MAX)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsBeløb]                  NVARCHAR(MAX)   NULL,
        [VAT_CategoryTotal_AfgiftsKategoriMomsBeløb_Valuta]           NVARCHAR(MAX)   NULL,
        [ZERO-RATED_AfgiftsKategoriKode]                              NVARCHAR(MAX)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag]           NVARCHAR(MAX)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta]    NVARCHAR(MAX)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb]              NVARCHAR(MAX)   NULL,
        [ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta]       NVARCHAR(MAX)   NULL,
        [ZERO-RATED_AfgiftsKategori]                                  NVARCHAR(MAX)   NULL,
        [ZERO-RATED_AfgiftsKategoriProcent]                           NVARCHAR(MAX)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsGrundlag]        NVARCHAR(MAX)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsGrundlag_Valuta] NVARCHAR(MAX)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsBeløb]           NVARCHAR(MAX)   NULL,
        [ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsBeløb_Valuta]    NVARCHAR(MAX)   NULL,
        [FakturaLinjeTotal]                                           NVARCHAR(MAX)   NULL,
        [FakturaLinjeTotal_Valuta]                                    NVARCHAR(MAX)   NULL,
        [FakturaTotal]                                                NVARCHAR(MAX)   NULL,
        [FakturaTotal_Valuta]                                         NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_statistic_invoice_header]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_header_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_header]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_header_DW_Timestamp_Update]
        ON [arc].[mysupply_statistic_invoice_header] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.metadata_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_metadata'
)
BEGIN
    CREATE TABLE [arc].[mysupply_metadata] (
        [id]                              INT             NOT NULL,
        [Guid]                            NVARCHAR(MAX)   NULL,
        [data_unit_identity_unique_name]  NVARCHAR(MAX)   NULL,
        [RowNumber]                       NVARCHAR(MAX)   NULL,
        [Type]                            NVARCHAR(MAX)   NULL,
        [FileName]                        NVARCHAR(MAX)   NULL,
        [ReceivedId]                      NVARCHAR(MAX)   NULL,
        [DestinationEP]                   NVARCHAR(MAX)   NULL,
        [SystemNavn]                      NVARCHAR(MAX)   NULL,
        [OrgUnit]                         NVARCHAR(MAX)   NULL,
        [ReceiveTime]                     NVARCHAR(MAX)   NULL,
        [ReceiveTimeFormat]               NVARCHAR(MAX)   NULL,
        [Kilde]                           NVARCHAR(MAX)   NULL,
        [FakturaTotal]                    NVARCHAR(MAX)   NULL,
        [OrdreRef]                        NVARCHAR(MAX)   NULL,
        [LeverandorCVR]                   NVARCHAR(MAX)   NULL,
        [SaelgerMomsNummer]               NVARCHAR(MAX)   NULL,
        [FirmaNavn]                       NVARCHAR(MAX)   NULL,
        [Adresse]                         NVARCHAR(MAX)   NULL,
        [Postnummer]                      NVARCHAR(MAX)   NULL,
        [By]                              NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_metadata]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_metadata_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_metadata]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_metadata_DW_Timestamp_Update]
        ON [arc].[mysupply_metadata] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.statistic_invoice_line_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_statistic_invoice_line'
)
BEGIN
    CREATE TABLE [arc].[mysupply_statistic_invoice_line] (
        [id]                                          INT             NOT NULL,
        [statistic_invoice_header_id]                 INT             NOT NULL,
        [FakturaLinjeNr]                              NVARCHAR(MAX)   NULL,
        [AntalEnhederFaktureret]                      NVARCHAR(MAX)   NULL,
        [AntalEnhederFaktureret_Type]                 NVARCHAR(MAX)   NULL,
        [AntalEnhederFaktureret_List]                 NVARCHAR(MAX)   NULL,
        [LinjeBeløb]                                  NVARCHAR(MAX)   NULL,
        [LinjeBeløb_Valuta]                           NVARCHAR(MAX)   NULL,
        [KøbsOrdreNummer]                             NVARCHAR(MAX)   NULL,
        [SalgsOrdreNummer]                            NVARCHAR(MAX)   NULL,
        [KundeLeveringsIdentifikation]                NVARCHAR(MAX)   NULL,
        [KundeLeveringsIdentifikation_Type]           NVARCHAR(MAX)   NULL,
        [DimensionsKontoStreng]                       NVARCHAR(MAX)   NULL,
        [KundeNavn]                                   NVARCHAR(MAX)   NULL,
        [KundeReferenceIdentifikation]                NVARCHAR(MAX)   NULL,
        [KundeReferenceNavn]                          NVARCHAR(MAX)   NULL,
        [KundeReferenceTelefonNr]                     NVARCHAR(MAX)   NULL,
        [KundeReferenceFaxNr]                         NVARCHAR(MAX)   NULL,
        [KundeReferenceEMail]                         NVARCHAR(MAX)   NULL,
        [Kundesprog]                                  NVARCHAR(MAX)   NULL,
        [KundeLeveringsAdresseID]                     NVARCHAR(MAX)   NULL,
        [KundeLeveringsVejNavn]                       NVARCHAR(MAX)   NULL,
        [KundeLeveringsHusNr]                         NVARCHAR(MAX)   NULL,
        [KundeLeveringsDueslag]                       NVARCHAR(MAX)   NULL,
        [KundeLeveringsByNavn]                        NVARCHAR(MAX)   NULL,
        [KundeLeveringsPostNr]                        NVARCHAR(MAX)   NULL,
        [KundeLeveringsLandeKode]                     NVARCHAR(MAX)   NULL,
        [KundeVareNr]                                 NVARCHAR(MAX)   NULL,
        [KundeVareBeskrivelse]                        NVARCHAR(MAX)   NULL,
        [KundensVareNr]                               NVARCHAR(MAX)   NULL,
        [KundensVareNr_Type]                          NVARCHAR(MAX)   NULL,
        [KundensVareBeskrivelse]                      NVARCHAR(MAX)   NULL,
        [FakturaUdstederensVareNr]                    NVARCHAR(MAX)   NULL,
        [FakturaUdstederensVareNr_Type]               NVARCHAR(MAX)   NULL,
        [FakturaUdstederensVareBeskrivelse]           NVARCHAR(MAX)   NULL,
        [FabrikantensVareNr]                          NVARCHAR(MAX)   NULL,
        [FabrikantensVareNr_Type]                     NVARCHAR(MAX)   NULL,
        [FabrikantensVareBeskrivelse]                 NVARCHAR(MAX)   NULL,
        [StandardiseretVareNr]                        NVARCHAR(MAX)   NULL,
        [StandardiseretVareNr_Type]                   NVARCHAR(MAX)   NULL,
        [StandardiseretVareBeskrivelse]               NVARCHAR(MAX)   NULL,
        [KatalogVareNr]                               NVARCHAR(MAX)   NULL,
        [KatalogVareNr_Type]                          NVARCHAR(MAX)   NULL,
        [KatalogVareBeskrivelse]                      NVARCHAR(MAX)   NULL,
        [KatalogIdentifikation]                       NVARCHAR(MAX)   NULL,
        [VareKlassifikation]                          NVARCHAR(MAX)   NULL,
        [VareKlassifikation_Type]                     NVARCHAR(MAX)   NULL,
        [VareKlassifikation_List]                     NVARCHAR(MAX)   NULL,
        [AfgiftsKategori]                             NVARCHAR(MAX)   NULL,
        [AfgiftsKategoriKode]                         NVARCHAR(MAX)   NULL,
        [AfgiftsKategoriProcent]                      NVARCHAR(MAX)   NULL,
        [EnhedsPris]                                  NVARCHAR(MAX)   NULL,
        [EnhedsPris_Valuta]                           NVARCHAR(MAX)   NULL,
        [EnhedsStørrelse]                             NVARCHAR(MAX)   NULL,
        [EnhedsStørrelse_Type]                        NVARCHAR(MAX)   NULL,
        [EnhedsStørrelse_List]                        NVARCHAR(MAX)   NULL,
        [LeveranceKrav]                               NVARCHAR(MAX)   NULL,
        [LeveringsDatoTypeIdentifikation_1]           NVARCHAR(MAX)   NULL,
        [LeveringsDato_1]                             NVARCHAR(MAX)   NULL,
        [LeveringsDatoTypeIdentifikation_2]           NVARCHAR(MAX)   NULL,
        [LeveringsDato_2]                             NVARCHAR(MAX)   NULL,
        [LeveringsDatoTypeIdentifikation_3]           NVARCHAR(MAX)   NULL,
        [LeveringsDato_3]                             NVARCHAR(MAX)   NULL,
        [LeveringsDatoTypeIdentifikation_4]           NVARCHAR(MAX)   NULL,
        [LeveringsDato_4]                             NVARCHAR(MAX)   NULL,
        [Item_FakturaUdstederVareNr]                  NVARCHAR(MAX)   NULL,
        [Item_FakturaUdstederVareNavn]                NVARCHAR(MAX)   NULL,
        [Item_KundensVareNr]                          NVARCHAR(MAX)   NULL,
        [Item_KundensVareNr_Type]                     NVARCHAR(MAX)   NULL,
        [Item_KundensVareBeskrivelse]                 NVARCHAR(MAX)   NULL,
        [Item_FakturaUdstederensVareNr]               NVARCHAR(MAX)   NULL,
        [Item_FakturaUdstederensVareNr_Type]          NVARCHAR(MAX)   NULL,
        [Item_FakturaUdstederensVareBeskrivelse]      NVARCHAR(MAX)   NULL,
        [Item_FabrikantensVareNr]                     NVARCHAR(MAX)   NULL,
        [Item_FabrikantensVareNr_Type]                NVARCHAR(MAX)   NULL,
        [Item_FabrikantensVareBeskrivelse]            NVARCHAR(MAX)   NULL,
        [Item_StandardiseretVareNr]                   NVARCHAR(MAX)   NULL,
        [Item_StandardiseretVareNr_Type]              NVARCHAR(MAX)   NULL,
        [Item_StandardiseretVareBeskrivelse]          NVARCHAR(MAX)   NULL,
        [Item_KatalogVareNr]                          NVARCHAR(MAX)   NULL,
        [Item_KatalogVareNr_Type]                     NVARCHAR(MAX)   NULL,
        [Item_KatalogVareBeskrivelse]                 NVARCHAR(MAX)   NULL,
        [Item_KatalogIdentifikation]                  NVARCHAR(MAX)   NULL,
        [Item_VareKlassifikation]                     NVARCHAR(MAX)   NULL,
        [Item_VareKlassifikation_Type]                NVARCHAR(MAX)   NULL,
        [Item_VareKlassifikation_List]                NVARCHAR(MAX)   NULL,
        [Item_AfgiftsKategori]                        NVARCHAR(MAX)   NULL,
        [Item_AfgiftsKategoriKode]                    NVARCHAR(MAX)   NULL,
        [Item_AfgiftsKategoriProcent]                 NVARCHAR(MAX)   NULL,
        [Item_EnhedsPris]                             NVARCHAR(MAX)   NULL,
        [Item_EnhedsPris_Valuta]                      NVARCHAR(MAX)   NULL,
        [Item_EnhedsStørrelse]                        NVARCHAR(MAX)   NULL,
        [Item_EnhedsStørrelse_Type]                   NVARCHAR(MAX)   NULL,
        [Item_EnhedsStørrelse_List]                   NVARCHAR(MAX)   NULL,
        [BasePrice_EnhedsPris]                        NVARCHAR(MAX)   NULL,
        [BasePrice_EnhedsPris_Valuta]                 NVARCHAR(MAX)   NULL,
        [BasePrice_EnhedsStørrelse]                   NVARCHAR(MAX)   NULL,
        [BasePrice_EnhedsStørrelse_Type]              NVARCHAR(MAX)   NULL,
        [BasePrice_EnhedsStørrelse_List]              NVARCHAR(MAX)   NULL,
        [Item_FakturaUdstederVareNavnKort]            NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_statistic_invoice_line]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_DW_Timestamp_Update]
        ON [arc].[mysupply_statistic_invoice_line] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_statistic_invoice_header_id'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_statistic_invoice_header_id]
        ON [arc].[mysupply_statistic_invoice_line] ([statistic_invoice_header_id]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.statistic_invoice_header_allowance_charge_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_statistic_invoice_header_allowance_charge'
)
BEGIN
    CREATE TABLE [arc].[mysupply_statistic_invoice_header_allowance_charge] (
        [id]                              INT             NOT NULL,
        [statistic_invoice_header_id]     INT             NOT NULL,
        [GebyrKategori]                   NVARCHAR(MAX)   NULL,
        [GebyrKategoriIndikering]         NVARCHAR(MAX)   NULL,
        [GebyrKategoriÅrsag]              NVARCHAR(MAX)   NULL,
        [GebyrKategoriKvantitet]          NVARCHAR(MAX)   NULL,
        [GebyrKategoriKvantitet_Type]     NVARCHAR(MAX)   NULL,
        [GebyrKategoriKvantitet_List]     NVARCHAR(MAX)   NULL,
        [GebyrKategoriTotal]              NVARCHAR(MAX)   NULL,
        [GebyrKategoriFinanskonto]        NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_statistic_invoice_header_allowance_charge]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_header_allowance_charge_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_header_allowance_charge]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_header_allowance_charge_DW_Timestamp_Update]
        ON [arc].[mysupply_statistic_invoice_header_allowance_charge] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_header_allowance_charge_header_id'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_header_allowance_charge]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_header_allowance_charge_header_id]
        ON [arc].[mysupply_statistic_invoice_header_allowance_charge] ([statistic_invoice_header_id]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.statistic_invoice_line_allowance_charge_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_statistic_invoice_line_allowance_charge'
)
BEGIN
    CREATE TABLE [arc].[mysupply_statistic_invoice_line_allowance_charge] (
        [id]                                          INT             NOT NULL,
        [statistic_invoice_line_id]                   INT             NOT NULL,
        [FakturaUdstederGebyrKategori]                NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriIndikering]      NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriÅrsag]           NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriKvantitet]       NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriKvantitet_Type]  NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriKvantitet_List]  NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriTotal]           NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriFinanskonto]     NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_statistic_invoice_line_allowance_charge]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_allowance_charge_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line_allowance_charge]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_allowance_charge_DW_Timestamp_Update]
        ON [arc].[mysupply_statistic_invoice_line_allowance_charge] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_allowance_charge_line_id'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line_allowance_charge]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_allowance_charge_line_id]
        ON [arc].[mysupply_statistic_invoice_line_allowance_charge] ([statistic_invoice_line_id]);
END
GO


-- ============================================================
-- ILS archive tables (17 new tables)
-- 1:1 mirror of ILS dbo.* source tables
-- Populated by DFM-managed archive merge assets
-- ============================================================


-- ----------------------------------------------------------------------------
-- arc.vareart_ils  (1:1 from ILS dbo.vareart)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_vareart'
)
BEGIN
    CREATE TABLE [arc].[ils_vareart] (
        -- Source columns (1:1 from dbo.vareart)
        [prrowid]      varchar(36)    NOT NULL,
        [vareart]      varchar(12)    NULL,
        [navn]         varchar(80)    NULL,
        [ktoart]       varchar(20)    NULL,
        [sw_synk]      bit            NULL,
        [aktiv]        bit            NULL,
        [sysklk]       int            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_vareart] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_vareart_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_vareart]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_vareart_DW_Timestamp_Update]
        ON [arc].[ils_vareart] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.varegrp_ils  (1:1 from ILS dbo.varegrp)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_varegrp'
)
BEGIN
    CREATE TABLE [arc].[ils_varegrp] (
        -- Source columns (1:1 from dbo.varegrp)
        [prrowid]      varchar(36)    NOT NULL,
        [varegrp]      varchar(8)     NULL,
        [navn]         varchar(80)    NULL,
        [ktogrp]       varchar(20)    NULL,
        [sw_synk]      bit            NULL,
        [aktiv]        bit            NULL,
        [sysklk]       int            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_varegrp] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_varegrp_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_varegrp]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_varegrp_DW_Timestamp_Update]
        ON [arc].[ils_varegrp] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.varetype_ils  (1:1 from ILS dbo.varetype)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_varetype'
)
BEGIN
    CREATE TABLE [arc].[ils_varetype] (
        -- Source columns (1:1 from dbo.varetype)
        [prrowid]      varchar(36)    NOT NULL,
        [varetype]     varchar(12)    NULL,
        [navn]         varchar(80)    NULL,
        [sw_synk]      bit            NULL,
        [aktiv]        bit            NULL,
        [sysklk]       int            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_varetype] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_varetype_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_varetype]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_varetype_DW_Timestamp_Update]
        ON [arc].[ils_varetype] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.moms_ils  (1:1 from ILS dbo.moms)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_moms'
)
BEGIN
    CREATE TABLE [arc].[ils_moms] (
        -- Source columns (1:1 from dbo.moms)
        [prrowid]      varchar(36)    NOT NULL,
        [momskd]       varchar(4)     NULL,
        [start_]       datetime       NULL,
        [txt]          varchar(80)    NULL,
        [momspct]      decimal(17,2)  NULL,
        [sw_synk]      bit            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [osmomskode]   varchar(4)     NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,
        [fradragpct]   decimal(17,2)  NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_moms] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_moms_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_moms]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_moms_DW_Timestamp_Update]
        ON [arc].[ils_moms] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.lev_ils  (1:1 from ILS dbo.lev)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lev'
)
BEGIN
    CREATE TABLE [arc].[ils_lev] (
        -- Source columns (1:1 from dbo.lev)
        [prrowid]              varchar(36)    NOT NULL,
        [lnr]                  varchar(20)    NULL,
        [navn]                 varchar(80)    NULL,
        [adr]                  varchar(80)    NULL,
        [adr2]                 varchar(80)    NULL,
        [land]                 varchar(6)     NULL,
        [postdi]               varchar(10)    NULL,
        [kontakt]              varchar(80)    NULL,
        [salger]               varchar(80)    NULL,
        [tlf]                  varchar(32)    NULL,
        [mobil]                varchar(32)    NULL,
        [email]                varchar(80)    NULL,
        [wwwside]              varchar(80)    NULL,
        [fax]                  varchar(32)    NULL,
        [giro]                 varchar(16)    NULL,
        [betform]              varchar(4)     NULL,
        [bet]                  varchar(6)     NULL,
        [bet2]                 varchar(6)     NULL,
        [kasbet]               varchar(6)     NULL,
        [kasrabpct]            decimal(17,2)  NULL,
        [levbet]               varchar(4)     NULL,
        [gebyrbel]             decimal(17,2)  NULL,
        [graensebel]           decimal(17,2)  NULL,
        [kundenr]              varchar(32)    NULL,
        [lkto]                 varchar(28)    NULL,
        [afstyp]               varchar(2)     NULL,
        [deflevdage]           int            NULL,
        [defgikfik]            varchar(66)    NULL,
        [nrkd]                 int            NULL,
        [bem]                  varchar(80)    NULL,
        [se_nr]                varchar(28)    NULL,
        [ydmodt]               varchar(20)    NULL,
        [edi_lokation]         varchar(34)    NULL,
        [edi_postks]           varchar(34)    NULL,
        [edi_pw]               varchar(28)    NULL,
        [sw_ean]               bit            NULL,
        [sw_edi_ord_fak_1_1]   bit            NULL,
        [sw_oekopost]          bit            NULL,
        [sw_synk]              bit            NULL,
        [ansvar]               varchar(20)    NULL,
        [aktiv]                bit            NULL,
        [sysdato]              datetime       NULL,
        [bruger]               varchar(6)     NULL,
        [seqnr]                int            NULL,
        [fastdag]              varchar(14)    NULL,
        [regnum]               varchar(8)     NULL,
        [bankkto]              varchar(20)    NULL,
        [sw_fik]               bit            NULL,
        [billedbi]             varchar(100)   NULL,
        [levdag]               varchar(14)    NULL,
        [doip]                 varchar(6)     NULL,
        [dunsnr]               varchar(40)    NULL,
        [e_vnr]                int            NULL,
        [levklk]               int            NULL,
        [txt1]                 varchar(80)    NULL,
        [txt2]                 varchar(80)    NULL,
        [txt3]                 varchar(80)    NULL,
        [txt4]                 varchar(20)    NULL,
        [cvr_nr]               varchar(28)    NULL,
        [type]                 varchar(10)    NULL,
        [ordredag]             varchar(14)    NULL,
        [brugerid]             varchar(20)    NULL,
        [pw]                   varchar(40)    NULL,
        [ordweb]               bit            NULL,
        [oprdto]               datetime       NULL,
        [oprbruger]            varchar(8)     NULL,
        [oprtid]               int            NULL,
        [sysklk]               int            NULL,
        [sw_andenlev]          bit            NULL,
        [ordremail]            varchar(120)   NULL,
        [ordresprog]           varchar(6)     NULL,
        [oioprofil]            varchar(24)    NULL,
        [sw_oioord_txt]        bit            NULL,
        [sw_oio_basisenh]      bit            NULL,
        [faktyratype]          varchar(20)    NULL,
        [sw_ws_lvnr]           bit            NULL,
        [ws_divvnr]            varchar(24)    NULL,
        [laasenhed]            bit            NULL,
        [intrastat]            varchar(20)    NULL,
        [sw_omvmoms]           bit            NULL,
        [osafd]                varchar(20)    NULL,
        [typenr]               varchar(20)    NULL,
        [nr]                   varchar(40)    NULL,
        [sw_oioord_txt_lin]    bit            NULL,
        [orddeadlineklk]       int            NULL,
        [fragtbel]             decimal(17,2)  NULL,
        [fragtgraensebel]      decimal(17,2)  NULL,
        [fakturamail]          varchar(120)   NULL,
        [synonym_]             varchar(120)   NULL,
        [rykkermail]           varchar(120)   NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_lev] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lev_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lev]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lev_DW_Timestamp_Update]
        ON [arc].[ils_lev] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.sgh_ils  (1:1 from ILS dbo.sgh)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_sgh'
)
BEGIN
    CREATE TABLE [arc].[ils_sgh] (
        -- Source columns (1:1 from dbo.sgh)
        [prrowid]      varchar(36)    NOT NULL,
        [sgh]          varchar(8)     NULL,
        [navn]         varchar(80)    NULL,
        [adr]          varchar(80)    NULL,
        [land]         varchar(6)     NULL,
        [postdi]       varchar(10)    NULL,
        [giro]         varchar(16)    NULL,
        [tlf]          varchar(32)    NULL,
        [fax]          varchar(32)    NULL,
        [afvigpct]     decimal(17,2)  NULL,
        [afvigpct2]    decimal(17,2)  NULL,
        [flerlagr]     bit            NULL,
        [antgnkob]     int            NULL,
        [aktiv]        bit            NULL,
        [fastomkbel]   decimal(17,2)  NULL,
        [personpris]   decimal(19,4)  NULL,
        [sterillgr]    varchar(8)     NULL,
        [usterillgr]   varchar(8)     NULL,
        [lnr]          varchar(20)    NULL,
        [levoekolog]   int            NULL,
        [edi_lokation] varchar(34)    NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [dunsnr]       varchar(40)    NULL,
        [vat_eu]       varchar(20)    NULL,
        [vat]          varchar(20)    NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_sgh] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_sgh_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_sgh]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_sgh_DW_Timestamp_Update]
        ON [arc].[ils_sgh] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.lgr_ils  (1:1 from ILS dbo.lgr)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lgr'
)
BEGIN
    CREATE TABLE [arc].[ils_lgr] (
        -- Source columns (1:1 from dbo.lgr)
        [prrowid]          varchar(36)    NOT NULL,
        [sgh]              varchar(8)     NULL,
        [lgr]              varchar(8)     NULL,
        [altlgr]           varchar(16)    NULL,
        [navn]             varchar(80)    NULL,
        [adr]              varchar(80)    NULL,
        [land]             varchar(6)     NULL,
        [postdi]           varchar(10)    NULL,
        [kontakt]          varchar(80)    NULL,
        [tlf]              varchar(32)    NULL,
        [fax]              varchar(32)    NULL,
        [giro]             varchar(16)    NULL,
        [deflevadr]        varchar(6)     NULL,
        [deflevdage]       int            NULL,
        [udlprikd]         varchar(4)     NULL,
        [opgprikd]         varchar(4)     NULL,
        [lgrejer]          varchar(16)    NULL,
        [rekvsvind]        varchar(16)    NULL,
        [rekvindk]         varchar(16)    NULL,
        [rekvsalg]         varchar(16)    NULL,
        [rekvavan]         varchar(16)    NULL,
        [rvis]             int            NULL,
        [flgsednr]         int            NULL,
        [batchnr]          varchar(24)    NULL,
        [bhtbibnavn]       varchar(16)    NULL,
        [edi_filsti]       varchar(104)   NULL,
        [ordhtxt##1]       varchar(186)   NULL,
        [ordhtxt##2]       varchar(186)   NULL,
        [ordhtxt##3]       varchar(186)   NULL,
        [flgstxt##1]       varchar(186)   NULL,
        [flgstxt##2]       varchar(186)   NULL,
        [flgstxt##3]       varchar(186)   NULL,
        [ordhatt]          varchar(60)    NULL,
        [sw_udlev]         bit            NULL,
        [sw_restord]       bit            NULL,
        [sw_genbemax]      bit            NULL,
        [sw_bht_udlev]     bit            NULL,
        [flerlok]          bit            NULL,
        [batchfm]          varchar(4)     NULL,
        [fiktiv]           bit            NULL,
        [aktiv]            bit            NULL,
        [ktolvgld]         varchar(20)    NULL,
        [ktovkob]          varchar(20)    NULL,
        [ktoporto]         varchar(20)    NULL,
        [ktofragt]         varchar(20)    NULL,
        [ktokasrb]         varchar(20)    NULL,
        [ktogebyr]         varchar(20)    NULL,
        [ktoforko]         varchar(200)   NULL,
        [ktoforsa]         varchar(200)   NULL,
        [ktoforal]         varchar(200)   NULL,
        [ktoformo]         varchar(200)   NULL,
        [ktoforsa2]        varchar(200)   NULL,
        [ktoforomp]        varchar(200)   NULL,
        [ktoresta]         varchar(20)    NULL,
        [ktoforsk]         varchar(20)    NULL,
        [ktodiff]          varchar(20)    NULL,
        [ekspnr]           int            NULL,
        [ekspnrgl]         int            NULL,
        [ekspndto]         datetime       NULL,
        [filsti]           varchar(104)   NULL,
        [filkvit]          varchar(16)    NULL,
        [filovf]           varchar(16)    NULL,
        [filudt]           varchar(16)    NULL,
        [fillaas]          varchar(16)    NULL,
        [oekosys]          varchar(6)     NULL,
        [oekoid1]          varchar(20)    NULL,
        [oekoid2]          varchar(20)    NULL,
        [oekoid3]          varchar(20)    NULL,
        [ompstdtokd]       varchar(6)     NULL,
        [sw_skabo]         bit            NULL,
        [wilsonomk##1]     decimal(38,2)  NULL,
        [wilsonomk##2]     decimal(38,2)  NULL,
        [wilsonomk##3]     decimal(38,2)  NULL,
        [wilsonpct##1]     decimal(38,2)  NULL,
        [wilsonpct##2]     decimal(38,2)  NULL,
        [wilsonpct##3]     decimal(38,2)  NULL,
        [indsenr]          varchar(20)    NULL,
        [indtlf]           varchar(32)    NULL,
        [indfax]           varchar(32)    NULL,
        [dunsnr]           varchar(40)    NULL,
        [ean_lokation]     varchar(34)    NULL,
        [bruger]           varchar(6)     NULL,
        [seqnr]            int            NULL,
        [sysdato]          datetime       NULL,
        [cvr_nr]           varchar(28)    NULL,
        [e_mail]           varchar(80)    NULL,
        [cvr_udland]       varchar(28)    NULL,
        [ktomoms]          varchar(20)    NULL,
        [fragtvnr]         varchar(24)    NULL,
        [gebyrvnr]         varchar(24)    NULL,
        [portovnr]         varchar(24)    NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,
        [ktovejafg]        varchar(20)    NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_lgr] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lgr_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lgr]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lgr_DW_Timestamp_Update]
        ON [arc].[ils_lgr] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.rekv_ils  (1:1 from ILS dbo.rekv)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_rekv'
)
BEGIN
    CREATE TABLE [arc].[ils_rekv] (
        -- Source columns (1:1 from dbo.rekv)
        [prrowid]              varchar(36)    NOT NULL,
        [sgh]                  varchar(8)     NULL,
        [rnr]                  varchar(16)    NULL,
        [altrnr]               varchar(16)    NULL,
        [navn]                 varchar(80)    NULL,
        [adr]                  varchar(80)    NULL,
        [adr2]                 varchar(80)    NULL,
        [land]                 varchar(6)     NULL,
        [postdi]               varchar(10)    NULL,
        [kontakt]              varchar(80)    NULL,
        [tlf]                  varchar(32)    NULL,
        [tlflok]               varchar(8)     NULL,
        [telefax]              varchar(32)    NULL,
        [e_mail]               varchar(80)    NULL,
        [ktosted]              varchar(34)    NULL,
        [rgrp]                 varchar(4)     NULL,
        [rtyp]                 varchar(4)     NULL,
        [budgsted]             varchar(8)     NULL,
        [levadr]               varchar(6)     NULL,
        [udlnr]                int            NULL,
        [rute]                 varchar(16)    NULL,
        [udldag]               varchar(14)    NULL,
        [udlprikd]             varchar(4)     NULL,
        [bem]                  varchar(80)    NULL,
        [edi_lokation]         varchar(34)    NULL,
        [sw_rvis]              bit            NULL,
        [sw_ktoforsa2]         bit            NULL,
        [sw_lgrstyr]           bit            NULL,
        [fiktiv]               bit            NULL,
        [aktiv]                bit            NULL,
        [primsgh]              varchar(8)     NULL,
        [primlgr]              varchar(8)     NULL,
        [sw_ekstern]           bit            NULL,
        [sw_divonline]         bit            NULL,
        [sw_ekstern_faktura]   bit            NULL,
        [bestiller]            varchar(34)    NULL,
        [dunsnr]               varchar(40)    NULL,
        [godk_dir]             bit            NULL,
        [txt1]                 varchar(80)    NULL,
        [txt2]                 varchar(80)    NULL,
        [txt3]                 varchar(80)    NULL,
        [profil]               varchar(80)    NULL,
        [bruger]               varchar(6)     NULL,
        [seqnr]                int            NULL,
        [sysdato]              datetime       NULL,
        [sw_auto]              bit            NULL,
        [afsnit]               varchar(24)    NULL,
        [placerring]           varchar(60)    NULL,
        [fysplacering]         varchar(40)    NULL,
        [sw_fly]               bit            NULL,
        [sysklk]               int            NULL,
        [aflevstreg]           varchar(40)    NULL,
        [transportor]          varchar(20)    NULL,
        [altlevadr]            varchar(6)     NULL,
        [sw_transportdok]      bit            NULL,
        [servicekodesk]        varchar(4)     NULL,
        [servicekodela]        varchar(4)     NULL,
        [vognpladsid]          int            NULL,
        [ekskl_ordadvis]       bit            NULL,
        [oprbruger]            varchar(6)     NULL,
        [oprdto]               datetime       NULL,
        [sor]                  varchar(40)    NULL,
        [salgskonto]           varchar(20)    NULL,
        [glndepot]             varchar(26)    NULL,
        [glndest]              varchar(26)    NULL,
        [glnrorpost]           varchar(26)    NULL,
        [glnrordepot]          varchar(26)    NULL,
        [ansv_led]             varchar(80)    NULL,
        [ansv_budg]            varchar(80)    NULL,
        [ansv_godk]            varchar(80)    NULL,
        [sw_modt_afd]          bit            NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_rekv] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_rekv_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_rekv]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_rekv_DW_Timestamp_Update]
        ON [arc].[ils_rekv] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.rekvgrp_ils  (1:1 from ILS dbo.rekvgrp)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_rekvgrp'
)
BEGIN
    CREATE TABLE [arc].[ils_rekvgrp] (
        -- Source columns (1:1 from dbo.rekvgrp)
        [prrowid]      varchar(36)    NOT NULL,
        [sgh]          varchar(8)     NULL,
        [rgrp]         varchar(4)     NULL,
        [navn]         varchar(80)    NULL,
        [aktiv]        bit            NULL,
        [sysklk]       int            NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_rekvgrp] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_rekvgrp_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_rekvgrp_DW_Timestamp_Update]
        ON [arc].[ils_rekvgrp] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.lafth_ils  (1:1 from ILS dbo.lafth)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lafth'
)
BEGIN
    CREATE TABLE [arc].[ils_lafth] (
        -- Source columns (1:1 from dbo.lafth)
        [prrowid]          varchar(36)    NOT NULL,
        [lnr]              varchar(20)    NULL,
        [aftale]           varchar(24)    NULL,
        [aftdto]           datetime       NULL,
        [txt]              varchar(80)    NULL,
        [start_]           datetime       NULL,
        [slut]             datetime       NULL,
        [belfra##1]        decimal(38,2)  NULL,
        [belfra##2]        decimal(38,2)  NULL,
        [belfra##3]        decimal(38,2)  NULL,
        [beltil##1]        decimal(38,2)  NULL,
        [beltil##2]        decimal(38,2)  NULL,
        [beltil##3]        decimal(38,2)  NULL,
        [rabkr##1]         decimal(38,2)  NULL,
        [rabkr##2]         decimal(38,2)  NULL,
        [rabkr##3]         decimal(38,2)  NULL,
        [rabatpct##1]      decimal(38,2)  NULL,
        [rabatpct##2]      decimal(38,2)  NULL,
        [rabatpct##3]      decimal(38,2)  NULL,
        [bem]              varchar(80)    NULL,
        [bonusgr##1]       decimal(38,2)  NULL,
        [bonusgr##2]       decimal(38,2)  NULL,
        [bonusgr##3]       decimal(38,2)  NULL,
        [bonusbel##1]      decimal(38,2)  NULL,
        [bonusbel##2]      decimal(38,2)  NULL,
        [bonusbel##3]      decimal(38,2)  NULL,
        [bonuspct##1]      decimal(38,2)  NULL,
        [bonuspct##2]      decimal(38,2)  NULL,
        [bonuspct##3]      decimal(38,2)  NULL,
        [bonusbem]         varchar(80)    NULL,
        [altlnr]           varchar(20)    NULL,
        [aktiv]            bit            NULL,
        [udbud]            varchar(24)    NULL,
        [kontakt]          varchar(80)    NULL,
        [ansvar]           varchar(20)    NULL,
        [eksansvar]        varchar(80)    NULL,
        [sysdato]          datetime       NULL,
        [bruger]           varchar(6)     NULL,
        [seqnr]            int            NULL,
        [afttype]          varchar(20)    NULL,
        [fragtbel]         decimal(17,2)  NULL,
        [sw_fragt]         bit            NULL,
        [gebyrbel]         decimal(17,2)  NULL,
        [sw_gebyr]         bit            NULL,
        [graensebel]       decimal(17,2)  NULL,
        [fragtgraensebel]  decimal(17,2)  NULL,
        [deflevdage]       int            NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_lafth] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lafth_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lafth]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lafth_DW_Timestamp_Update]
        ON [arc].[ils_lafth] ([DW_Timestamp_Update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.laftdpris_ils  (1:1 from ILS dbo.laftdpris) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_laftdpris'
)
BEGIN
    CREATE TABLE [arc].[ils_laftdpris] (
        -- Source columns (1:1 from dbo.laftdpris)
        [prrowid]      varchar(36)    NOT NULL,
        [lnr]          varchar(20)    NULL,
        [aftale]       varchar(24)    NULL,
        [vnr]          varchar(24)    NULL,
        [start_]       datetime       NULL,
        [startklk]     int            NULL,
        [slut]         datetime       NULL,
        [slutklk]      int            NULL,
        [pris##1]      decimal(38,4)  NULL,
        [pris##2]      decimal(38,4)  NULL,
        [pris##3]      decimal(38,4)  NULL,
        [pris##4]      decimal(38,4)  NULL,
        [pakstr##1]    decimal(38,3)  NULL,
        [pakstr##2]    decimal(38,3)  NULL,
        [pakstr##3]    decimal(38,3)  NULL,
        [pakstr##4]    decimal(38,3)  NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [aktiv]        bit            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_laftdpris] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdpris_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdpris]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdpris_DW_Timestamp_Update]
        ON [arc].[ils_laftdpris] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdpris_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdpris]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdpris_sysdato]
        ON [arc].[ils_laftdpris] ([sysdato] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.laftdstam_ils  (1:1 from ILS dbo.laftdstam) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_laftdstam'
)
BEGIN
    CREATE TABLE [arc].[ils_laftdstam] (
        -- Source columns (1:1 from dbo.laftdstam)
        [prrowid]      varchar(36)    NOT NULL,
        [lnr]          varchar(20)    NULL,
        [aftale]       varchar(24)    NULL,
        [vnr]          varchar(24)    NULL,
        [start_]       datetime       NULL,
        [startklk]     int            NULL,
        [slut]         datetime       NULL,
        [slutklk]      int            NULL,
        [levvarenavn]  varchar(80)    NULL,
        [distnr]       varchar(20)    NULL,
        [altlnr]       varchar(20)    NULL,
        [lvnr2]        varchar(40)    NULL,
        [vnavn1]       varchar(80)    NULL,
        [vnavn2]       varchar(80)    NULL,
        [billed]       varchar(MAX)   NULL,
        [billedli]     varchar(MAX)   NULL,
        [lvnr##1]      varchar(126)   NULL,
        [lvnr##2]      varchar(126)   NULL,
        [lvnr##3]      varchar(126)   NULL,
        [lvnr##4]      varchar(126)   NULL,
        [inder]        varchar(40)    NULL,
        [yder]         varchar(40)    NULL,
        [palle]        varchar(40)    NULL,
        [mindste]      varchar(40)    NULL,
        [ordreenh]     varchar(40)    NULL,
        [varemodenh]   varchar(40)    NULL,
        [udlevenh]     varchar(40)    NULL,
        [fakturaenh]   varchar(40)    NULL,
        [steril]       varchar(24)    NULL,
        [intrastat]    varchar(40)    NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [aktiv]        bit            NULL,
        [varestr]      varchar(40)    NULL,
        [unspc]        varchar(16)    NULL,
        [extordreenh]  varchar(40)    NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,
        [pmdmid]       int            NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_laftdstam] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdstam_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdstam]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdstam_DW_Timestamp_Update]
        ON [arc].[ils_laftdstam] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdstam_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdstam]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdstam_sysdato]
        ON [arc].[ils_laftdstam] ([sysdato] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.laftd_ils  (1:1 from ILS dbo.laftd) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_laftd'
)
BEGIN
    CREATE TABLE [arc].[ils_laftd] (
        -- Source columns (1:1 from dbo.laftd)
        [prrowid]       varchar(36)    NOT NULL,
        [lnr]           varchar(20)    NULL,
        [aftale]        varchar(24)    NULL,
        [vnr]           varchar(24)    NULL,
        [bem]           varchar(80)    NULL,
        [maerke]        varchar(10)    NULL,
        [ordmin]        decimal(18,3)  NULL,
        [start_]        datetime       NULL,
        [slut]          datetime       NULL,
        [pris##1]       decimal(38,4)  NULL,
        [pris##2]       decimal(38,4)  NULL,
        [pris##3]       decimal(38,4)  NULL,
        [pakstr##1]     int            NULL,
        [pakstr##2]     int            NULL,
        [pakstr##3]     int            NULL,
        [lvnr##1]       varchar(126)   NULL,
        [lvnr##2]       varchar(126)   NULL,
        [lvnr##3]       varchar(126)   NULL,
        [levplan]       varchar(4)     NULL,
        [distrvnr##1]   varchar(84)    NULL,
        [distrvnr##2]   varchar(84)    NULL,
        [distrvnr##3]   varchar(84)    NULL,
        [lnavn]         varchar(80)    NULL,
        [valuta]        varchar(8)     NULL,
        [kurs]          decimal(19,4)  NULL,
        [afvigpct]      decimal(17,2)  NULL,
        [regulpct]      decimal(17,2)  NULL,
        [andelpct]      decimal(17,2)  NULL,
        [faelvnr]       varchar(24)    NULL,
        [udbud]         varchar(24)    NULL,
        [sysdato]       datetime       NULL,
        [bruger]        varchar(6)     NULL,
        [seqnr]         int            NULL,
        [afttyp]        int            NULL,
        [sw_alt]        bit            NULL,
        [aktiv]         bit            NULL,
        [sw_sek]        bit            NULL,
        [slutvnr]       datetime       NULL,
        [lvnr2]         varchar(40)    NULL,
        [oprdto]        datetime       NULL,
        [Pro2SrcPDB]    varchar(12)    NULL,
        [pro2created]   datetime       NULL,
        [pro2modified]  datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_laftd] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftd_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftd]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftd_DW_Timestamp_Update]
        ON [arc].[ils_laftd] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftd_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftd]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftd_sysdato]
        ON [arc].[ils_laftd] ([sysdato] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.udlev_ils  (1:1 from ILS dbo.udlev) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_udlev'
)
BEGIN
    CREATE TABLE [arc].[ils_udlev] (
        -- Source columns (1:1 from dbo.udlev)
        [prrowid]          varchar(36)    NOT NULL,
        [sgh]              varchar(8)     NULL,
        [lgr]              varchar(8)     NULL,
        [sgh_rnr]          varchar(8)     NULL,
        [rnr]              varchar(16)    NULL,
        [vnr]              varchar(24)    NULL,
        [rvis]             int            NULL,
        [udldto]           datetime       NULL,
        [udlklk]           int            NULL,
        [flgsednr]         int            NULL,
        [udlant]           decimal(18,3)  NULL,
        [udlbel]           decimal(19,4)  NULL,
        [indkbel]          decimal(19,4)  NULL,
        [rabatbel]         decimal(19,4)  NULL,
        [brik]             int            NULL,
        [lgrlok]           varchar(16)    NULL,
        [batchnr]          varchar(24)    NULL,
        [udlobdto]         datetime       NULL,
        [lnr]              varchar(20)    NULL,
        [ordre]            int            NULL,
        [ordlinie]         int            NULL,
        [postdto]          datetime       NULL,
        [ompoststat]       varchar(4)     NULL,
        [sw_direkte]       bit            NULL,
        [sw_paksdl]        bit            NULL,
        [sw_flgsdl]        bit            NULL,
        [lagerantal]       decimal(18,3)  NULL,
        [projnr]           varchar(20)    NULL,
        [kto]              varchar(28)    NULL,
        [udlevenh]         varchar(40)    NULL,
        [bruger]           varchar(6)     NULL,
        [seqnr]            int            NULL,
        [sysdato]          datetime       NULL,
        [formaal]          varchar(24)    NULL,
        [sw_fly]           bit            NULL,
        [lotnr]            varchar(24)    NULL,
        [refnr]            int            NULL,
        [reflinie]         varchar(40)    NULL,
        [udtraknr1]        int            NULL,
        [udtraknr2]        int            NULL,
        [udtraknr3]        int            NULL,
        [sw_haster]        bit            NULL,
        [prioritet]        varchar(10)    NULL,
        [tekst1]           varchar(60)    NULL,
        [tekst2]           varchar(60)    NULL,
        [aflevoper]        varchar(20)    NULL,
        [aflevdto]         datetime       NULL,
        [aflevklk]         int            NULL,
        [aflevtxt]         varchar(80)    NULL,
        [aflevstreg]       varchar(40)    NULL,
        [id]               int            NULL,
        [rvisvareid]       int            NULL,
        [plukvareid]       int            NULL,
        [salgsordreint]    int            NULL,
        [salgsordre]       varchar(40)    NULL,
        [salgsfaknr]       varchar(20)    NULL,
        [vognnummer]       varchar(80)    NULL,
        [bem]              varchar(100)   NULL,
        [lvnr##1]          varchar(168)   NULL,
        [lvnr##2]          varchar(168)   NULL,
        [lvnr##3]          varchar(168)   NULL,
        [lvnr##4]          varchar(168)   NULL,
        [lgrbeh_prio]      int            NULL,
        [sw_erstat]        bit            NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,
        [kundeid]          int            NULL,
        [kundesortimentid] int            NULL,
        [aarsag]           varchar(16)    NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_udlev] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_udlev_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_udlev]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_udlev_DW_Timestamp_Update]
        ON [arc].[ils_udlev] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_udlev_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_udlev]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_udlev_sysdato]
        ON [arc].[ils_udlev] ([sysdato] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.lgrvare_ils  (1:1 from ILS dbo.lgrvare) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lgrvare'
)
BEGIN
    CREATE TABLE [arc].[ils_lgrvare] (
        -- Source columns (1:1 from dbo.lgrvare)
        [prrowid]              varchar(36)    NOT NULL,
        [sgh]                  varchar(8)     NULL,
        [lgr]                  varchar(8)     NULL,
        [vnr]                  varchar(24)    NULL,
        [lnr]                  varchar(20)    NULL,
        [lvnr]                 varchar(40)    NULL,
        [lvnr2]                varchar(40)    NULL,
        [lvnrstrip]            varchar(40)    NULL,
        [branche]              varchar(10)    NULL,
        [deflgrlok]            varchar(16)    NULL,
        [lagersted]            varchar(20)    NULL,
        [enhedtxt]             varchar(40)    NULL,
        [lagerenh]             varchar(10)    NULL,
        [varekode]             varchar(4)     NULL,
        [ktogrp]               varchar(20)    NULL,
        [ktoart]               varchar(20)    NULL,
        [budgart]              varchar(12)    NULL,
        [prisformel]           varchar(4)     NULL,
        [genpkt]               decimal(18,3)  NULL,
        [behmax]               decimal(18,3)  NULL,
        [behmin]               decimal(18,3)  NULL,
        [udlmax]               decimal(18,3)  NULL,
        [udlmin]               decimal(18,3)  NULL,
        [udlmult]              int            NULL,
        [ordmin]               decimal(18,3)  NULL,
        [ordvejl]              decimal(18,3)  NULL,
        [ordopt]               decimal(18,3)  NULL,
        [prisgnsl]             decimal(19,4)  NULL,
        [prisgnsx]             decimal(19,4)  NULL,
        [prisgros]             decimal(19,4)  NULL,
        [prisindk]             decimal(19,4)  NULL,
        [prislist]             decimal(19,4)  NULL,
        [prissalg]             decimal(19,4)  NULL,
        [prisstd]              decimal(19,4)  NULL,
        [prisvejl]             decimal(19,4)  NULL,
        [pakstr]               int            NULL,
        [antdeci]              int            NULL,
        [holddage]             int            NULL,
        [frekvens]             int            NULL,
        [opmetode]             varchar(8)     NULL,
        [optemp]               int            NULL,
        [sidstdto]             datetime       NULL,
        [sidstant]             decimal(18,3)  NULL,
        [gnsmdforb]            decimal(17,2)  NULL,
        [ansvar]               varchar(20)    NULL,
        [sw_flergangs_vare]    bit            NULL,
        [sw_restord]           bit            NULL,
        [sw_lgrstyr]           bit            NULL,
        [sw_batch]             bit            NULL,
        [sw_udlkontr]          bit            NULL,
        [sw_recept]            bit            NULL,
        [fritxt##1]            varchar(164)   NULL,
        [fritxt##2]            varchar(164)   NULL,
        [aktiv]                bit            NULL,
        [pakint]               int            NULL,
        [enhtxtint]            varchar(40)    NULL,
        [txt]                  varchar(80)    NULL,
        [lnavn]                varchar(80)    NULL,
        [oprdto]               datetime       NULL,
        [oprklk]               int            NULL,
        [oprini]               varchar(8)     NULL,
        [fakturaenh]           varchar(40)    NULL,
        [ordreenh]             varchar(40)    NULL,
        [udlevenh]             varchar(40)    NULL,
        [varemodenh]           varchar(40)    NULL,
        [dec1]                 decimal(18,3)  NULL,
        [dec2]                 decimal(18,3)  NULL,
        [dec3]                 decimal(18,3)  NULL,
        [dec4]                 decimal(18,3)  NULL,
        [bruger]               varchar(6)     NULL,
        [sysdato]              datetime       NULL,
        [seqnr]                int            NULL,
        [txt1]                 varchar(80)    NULL,
        [sw_web]               bit            NULL,
        [pakning1]             varchar(20)    NULL,
        [pakning2]             varchar(20)    NULL,
        [pakning3]             varchar(20)    NULL,
        [pakning4]             varchar(20)    NULL,
        [pvnr]                 varchar(50)    NULL,
        [varetype2]            varchar(12)    NULL,
        [sw_lotnr]             bit            NULL,
        [sw_kons]              bit            NULL,
        [sysklk]               int            NULL,
        [paktyp]               varchar(6)     NULL,
        [aftale]               varchar(24)    NULL,
        [indhojde]             decimal(17,3)  NULL,
        [indbrede]             decimal(17,3)  NULL,
        [indlangde]            decimal(17,3)  NULL,
        [indvagt]              decimal(17,3)  NULL,
        [yderhojde]            decimal(17,3)  NULL,
        [yderbrede]            decimal(17,3)  NULL,
        [yderlangde]           decimal(17,3)  NULL,
        [ydervagt]             decimal(17,3)  NULL,
        [pallehojde]           decimal(17,3)  NULL,
        [pallebrede]           decimal(17,3)  NULL,
        [pallelangde]          decimal(17,3)  NULL,
        [pallevagt]            decimal(17,3)  NULL,
        [deflevdage]           int            NULL,
        [bufferlok]            varchar(16)    NULL,
        [sw_beregngenbest]     bit            NULL,
        [kategori]             varchar(40)    NULL,
        [levvarenavn]          varchar(80)    NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,
        [udlob_obs]            int            NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_lgrvare] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lgrvare_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lgrvare]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lgrvare_DW_Timestamp_Update]
        ON [arc].[ils_lgrvare] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lgrvare_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_lgrvare]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lgrvare_sysdato]
        ON [arc].[ils_lgrvare] ([sysdato] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.pmdm_ils  (1:1 from ILS dbo.pmdm) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_pmdm'
)
BEGIN
    CREATE TABLE [arc].[ils_pmdm] (
        -- Source columns (1:1 from dbo.pmdm)
        [prrowid]              varchar(36)    NOT NULL,
        [vnr]                  varchar(24)    NULL,
        [pmdmid]               int            NULL,
        [aktiv]                bit            NULL,
        [navn]                 varchar(80)    NULL,
        [navn2]                varchar(80)    NULL,
        [navneng]              varchar(80)    NULL,
        [navneng2]             varchar(80)    NULL,
        [varetype]             varchar(12)    NULL,
        [varegrp]              varchar(8)     NULL,
        [vareart]              varchar(12)    NULL,
        [basisenh]             varchar(10)    NULL,
        [varestr]              varchar(40)    NULL,
        [momskd]               varchar(4)     NULL,
        [unspsc]               varchar(16)    NULL,
        [unspscint]            varchar(16)    NULL,
        [weblink]              varchar(MAX)   NULL,
        [billede]              varchar(MAX)   NULL,
        [steril]               varchar(24)    NULL,
        [udbkat]               varchar(40)    NULL,
        [sw_tdoc]              bit            NULL,
        [ejlukkes]             bit            NULL,
        [paktyp]               varchar(6)     NULL,
        [adr_kode]             varchar(12)    NULL,
        [un_nr]                varchar(14)    NULL,
        [sik_fak]              decimal(17,2)  NULL,
        [sw_brikker]           bit            NULL,
        [sw_kons]              bit            NULL,
        [sw_afdlgrstyr]        bit            NULL,
        [miljo1]               varchar(16)    NULL,
        [miljo2]               varchar(16)    NULL,
        [miljo3]               varchar(16)    NULL,
        [miljo4]               varchar(16)    NULL,
        [miljo5]               varchar(16)    NULL,
        [miljo6]               varchar(16)    NULL,
        [miljo7]               varchar(16)    NULL,
        [miljo8]               varchar(16)    NULL,
        [miljo9]               varchar(16)    NULL,
        [miljo10]              varchar(16)    NULL,
        [enhedstypemindste]    varchar(16)    NULL,
        [enhedstypeinder]      varchar(16)    NULL,
        [enhedstypeyder]       varchar(16)    NULL,
        [enhedstypepalle]      varchar(16)    NULL,
        [enheduommindste]      varchar(40)    NULL,
        [enheduominder]        varchar(40)    NULL,
        [enheduomyder]         varchar(40)    NULL,
        [enheduompalle]        varchar(40)    NULL,
        [gtinmindste]          varchar(80)    NULL,
        [gtininder]            varchar(80)    NULL,
        [gtinydre]             varchar(80)    NULL,
        [gtinpalle]            varchar(80)    NULL,
        [hojdemindste]         decimal(17,2)  NULL,
        [hojdeinder]           decimal(17,2)  NULL,
        [hojdeyder]            decimal(17,2)  NULL,
        [hojdepalle]           decimal(17,2)  NULL,
        [bredemindste]         decimal(17,2)  NULL,
        [bredeinder]           decimal(17,2)  NULL,
        [bredeyder]            decimal(17,2)  NULL,
        [bredepalle]           decimal(17,2)  NULL,
        [langdemindste]        decimal(17,2)  NULL,
        [langdeinder]          decimal(17,2)  NULL,
        [langdeyder]           decimal(17,2)  NULL,
        [langdepalle]          decimal(17,2)  NULL,
        [vagtmindste]          decimal(17,2)  NULL,
        [vagtinder]            decimal(17,2)  NULL,
        [vagtyder]             decimal(17,2)  NULL,
        [vagtpalle]            decimal(17,2)  NULL,
        [lnr]                  varchar(20)    NULL,
        [lvnr]                 varchar(24)    NULL,
        [lvnrnavn]             varchar(80)    NULL,
        [lvnr2]                varchar(40)    NULL,
        [prisgnsl]             decimal(17,2)  NULL,
        [ordreuom]             varchar(40)    NULL,
        [fakturauom]           varchar(40)    NULL,
        [aftale]               varchar(24)    NULL,
        [aftalenavn]           varchar(80)    NULL,
        [aftdto]               datetime       NULL,
        [aftstart]             datetime       NULL,
        [aftslut]              datetime       NULL,
        [afttype]              varchar(40)    NULL,
        [aftansvar]            varchar(20)    NULL,
        [mangdeenhed]          varchar(80)    NULL,
        [mindstekob]           decimal(17,2)  NULL,
        [faktor]               decimal(17,2)  NULL,
        [prodnavn]             varchar(80)    NULL,
        [prodvarenavn]         varchar(80)    NULL,
        [prodvarenummer]       varchar(80)    NULL,
        [leverandorid]         varchar(40)    NULL,
        [farenummer]           int            NULL,
        [datablad]             varchar(80)    NULL,
        [oprindelsesland]      varchar(80)    NULL,
        [flergangsvare]        varchar(40)    NULL,
        [positionsnummer]      varchar(40)    NULL,
        [leverandornavn]       varchar(80)    NULL,
        [startdatotid]         datetime       NULL,
        [slutdatotid]          datetime       NULL,
        [liniestartdatotid]    datetime       NULL,
        [linieslutdatotid]     datetime       NULL,
        [begrundelse]          varchar(80)    NULL,
        [kontaktperson]        varchar(80)    NULL,
        [sidstopdpmdm]         datetime       NULL,
        [sidstopdils]          datetime       NULL,
        [afsendt]              bit            NULL,
        [sysdato]              datetime       NULL,
        [seqnr]                int            NULL,
        [bruger]               varchar(10)    NULL,
        [txt]                  varchar(16)    NULL,
        [txt2]                 varchar(16)    NULL,
        [txt3]                 varchar(16)    NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,
        [pmdmid2]              varchar(MAX)   NULL,
        [gtinyder]             varchar(80)    NULL,
        [breddemindste]        decimal(17,3)  NULL,
        [breddeinder]          decimal(17,3)  NULL,
        [breddeyder]           decimal(17,3)  NULL,
        [breddepalle]          decimal(17,3)  NULL,
        [levnavn]              varchar(80)    NULL,
        [levvirksomhedsnr]     varchar(40)    NULL,
        [sw_indberet]          bit            NULL,
        [konverttilvnr]        varchar(24)    NULL,
        [konvertbruger]        varchar(16)    NULL,
        [indlaest]             bit            NULL,
        [sw_ikkeeu]            bit            NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_pmdm] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_pmdm_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_pmdm]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_pmdm_DW_Timestamp_Update]
        ON [arc].[ils_pmdm] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_pmdm_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_pmdm]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_pmdm_sysdato]
        ON [arc].[ils_pmdm] ([sysdato] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.indkfak_ils  (1:1 from ILS dbo.indkfak) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_indkfak'
)
BEGIN
    CREATE TABLE [arc].[ils_indkfak] (
        -- Source columns (1:1 from dbo.indkfak)
        [prrowid]          varchar(36)    NOT NULL,
        [sgh]              varchar(8)     NULL,
        [lgr]              varchar(8)     NULL,
        [vnr]              varchar(24)    NULL,
        [sgh_rnr]          varchar(8)     NULL,
        [rnr]              varchar(16)    NULL,
        [fakant]           decimal(18,3)  NULL,
        [fakbel]           decimal(17,2)  NULL,
        [prismod]          decimal(17,2)  NULL,
        [varerabbel]       decimal(17,2)  NULL,
        [varerabpct]       decimal(17,2)  NULL,
        [ordre]            int            NULL,
        [faknr]            varchar(30)    NULL,
        [batchnr]          varchar(24)    NULL,
        [lnr]              varchar(20)    NULL,
        [aftale]           varchar(12)    NULL,
        [levdto]           datetime       NULL,
        [fakdto]           datetime       NULL,
        [bgfdto]           datetime       NULL,
        [klkslet]          int            NULL,
        [lagerantal]       decimal(18,3)  NULL,
        [bilag]            int            NULL,
        [edi_status]       varchar(4)     NULL,
        [linie]            int            NULL,
        [mednr]            varchar(30)    NULL,
        [levvarenr]        varchar(40)    NULL,
        [levvarenavn]      varchar(80)    NULL,
        [enhedsstorrelse]  varchar(30)    NULL,
        [kontantrabatdato] datetime       NULL,
        [levtype]          varchar(16)    NULL,
        [levid]            varchar(16)    NULL,
        [slutlev]          bit            NULL,
        [slutfakt]         bit            NULL,
        [projnr]           varchar(20)    NULL,
        [faklinienr]       int            NULL,
        [fakant_org]       decimal(18,3)  NULL,
        [fakbel_org]       decimal(17,2)  NULL,
        [match1]           varchar(60)    NULL,
        [match2]           varchar(60)    NULL,
        [fakturaenh]       varchar(40)    NULL,
        [sysdato]          datetime       NULL,
        [seqnr]            int            NULL,
        [bruger]           varchar(6)     NULL,
        [formaal]          varchar(24)    NULL,
        [xmlantal]         decimal(17,2)  NULL,
        [xmlenhed]         varchar(40)    NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]   DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update]   DATETIME2(0)   NULL,

        CONSTRAINT [PK_arc_ils_indkfak] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_indkfak_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_indkfak]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_indkfak_DW_Timestamp_Update]
        ON [arc].[ils_indkfak] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_indkfak_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_indkfak]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_indkfak_sysdato]
        ON [arc].[ils_indkfak] ([sysdato] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.afdeling_sydax  (1:1 from SydAX [Finans].[Afdeling])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_afdeling'
)
BEGIN
    CREATE TABLE [arc].[sydax_afdeling] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [Afdelingsnummer]                      VARCHAR(20)    NOT NULL,

        -- Source columns (1:1 from [Finans].[Afdeling], minus merge keys)
        [AfdelingId]                           BIGINT         NULL,
        [Afdelingstekst]                       VARCHAR(1000)  NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [E1]                                   VARCHAR(4)     NULL,
        [O1]                                   VARCHAR(4)     NULL,
        [ref_RECID]                            BIGINT         NULL,
        [ref_DirPartyTableId]                  BIGINT         NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,
        [RECID]                                BIGINT         NOT NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                  DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_afdeling] PRIMARY KEY CLUSTERED ([Kildesystem], [RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_afdeling_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_afdeling]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_afdeling_Modified]
        ON [arc].[sydax_afdeling] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_afdeling_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_afdeling]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_afdeling_CreatedOn]
        ON [arc].[sydax_afdeling] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.finanskonto_sydax  (1:1 from SydAX [Finans].[Finanskonto])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskonto'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskonto] (
        -- Merge keys
        [Kildesystem]                                          VARCHAR(10)    NOT NULL,
        [Kontonummer]                                          VARCHAR(500)   NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanskonto], minus merge keys)
        [FinanskontoId]                                        BIGINT         NOT NULL,
        [ErLukket]                                             BIT            NULL,
        [ve_RECID]                                             BIGINT         NULL,
        [afd_RECID]                                            BIGINT         NULL,
        [os_RECID]                                             BIGINT         NULL,
        [hk_RECID]                                             BIGINT         NULL,
        [ma_RECID]                                             BIGINT         NULL,
        [grp_RECID]                                            BIGINT         NULL,
        [art_RECID]                                            BIGINT         NULL,
        [d1_RECID]                                             BIGINT         NULL,
        [d2_RECID]                                             BIGINT         NULL,
        [l_RECID]                                              BIGINT         NULL,
        [ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId]            BIGINT         NULL,
        [RunId]                                                INT            NULL,
        [StartDateTime]                                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                                          DATETIME2(0)   NULL,
        [RdwTs]                                                DATETIME2(7)   NULL,
        [ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID]         BIGINT         NULL,
        [CreatedOn]                                            DATETIME       NULL,
        [Modified]                                             DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                                  DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]                                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskonto] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanskontoId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_Modified]
        ON [arc].[sydax_finanskonto] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_CreatedOn]
        ON [arc].[sydax_finanskonto] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.finanskonto_omkostningssted_sydax  (1:1 from SydAX [Finans].[FinanskontoOmkostningssted])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskonto_omkostningssted'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskonto_omkostningssted] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [FinanskontoId]                        BIGINT         NOT NULL,
        [Omkostningsstedid]                    BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[FinanskontoOmkostningssted], minus merge keys)
        [FinanskontoOmkostningsstedid]         BIGINT         NOT NULL,
        [ref_LedgerTableId]                    BIGINT         NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                  DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskonto_omkostningssted] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanskontoOmkostningsstedid], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_omkostningssted_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto_omkostningssted]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_omkostningssted_Modified]
        ON [arc].[sydax_finanskonto_omkostningssted] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_omkostningssted_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto_omkostningssted]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_omkostningssted_CreatedOn]
        ON [arc].[sydax_finanskonto_omkostningssted] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.hovedkonto_sydax  (1:1 from SydAX [Finans].[Hovedkonto])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_hovedkonto'
)
BEGIN
    CREATE TABLE [arc].[sydax_hovedkonto] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [ref_RECID]                            BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[Hovedkonto], minus merge keys)
        [HovedkontoId]                         BIGINT         NULL,
        [Hovedkontonummer]                     VARCHAR(20)    NULL,
        [Hovedkontotekst]                      VARCHAR(60)    NULL,
        [KontoplanId]                          BIGINT         NULL,
        [HovedkontotypeId]                     BIGINT         NULL,
        [RapporteringstypeId]                  BIGINT         NULL,
        [HovedkontoKategoriId]                 BIGINT         NULL,
        [DKForslag]                            VARCHAR(10)    NULL,
        [DKKrav]                               VARCHAR(10)    NULL,
        [Saldokontrol]                         VARCHAR(10)    NULL,
        [ModkontoId]                           BIGINT         NULL,
        [PrimokontoId]                         BIGINT         NULL,
        [SRUKode]                              VARCHAR(10)    NULL,
        [Afslut]                               VARCHAR(10)    NULL,
        [TvungenReference]                     VARCHAR(10)    NULL,
        [CloseType]                            VARCHAR(10)    NULL,
        [Autoriseret]                          VARCHAR(10)    NULL,
        [Standardkoncernkonto]                 VARCHAR(10)    NULL,
        [F1]                                   VARCHAR(4)     NULL,
        [F2]                                   VARCHAR(4)     NULL,
        [F3]                                   VARCHAR(4)     NULL,
        [D1]                                   VARCHAR(4)     NULL,
        [G1]                                   VARCHAR(4)     NULL,
        [Valuta]                               VARCHAR(10)    NULL,
        [ValutaValidering]                     VARCHAR(10)    NULL,
        [BrugerId]                             VARCHAR(10)    NULL,
        [BrugerValidering]                     VARCHAR(10)    NULL,
        [FinansposteringstypeId]               BIGINT         NULL,
        [KonteringValidering]                  VARCHAR(10)    NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [ref_MainAccountId]                    BIGINT         NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                  DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_hovedkonto] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_hovedkonto_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_hovedkonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_hovedkonto_Modified]
        ON [arc].[sydax_hovedkonto] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_hovedkonto_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_hovedkonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_hovedkonto_CreatedOn]
        ON [arc].[sydax_hovedkonto] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.omkostningssted_hierarki_sydax  (1:1 from SydAX [Finans].[OmkostningsstedHierarki])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_omkostningssted_hierarki'
)
BEGIN
    CREATE TABLE [arc].[sydax_omkostningssted_hierarki] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [max_OmkostningsstedId]    BIGINT         NULL,

        -- Source columns (1:1 from [Finans].[OmkostningsstedHierarki], minus merge keys)
        [OmkostningsstedHierarkiId] BIGINT        NOT NULL,
        [jeId1]                    BIGINT         NULL,
        [veId2]                    BIGINT         NULL,
        [afId2]                    BIGINT         NULL,
        [veId3]                    BIGINT         NULL,
        [afId3]                    BIGINT         NULL,
        [osId3]                    BIGINT         NULL,
        [veId4]                    BIGINT         NULL,
        [afId4]                    BIGINT         NULL,
        [osId4]                    BIGINT         NULL,
        [veId5]                    BIGINT         NULL,
        [afId5]                    BIGINT         NULL,
        [osId5]                    BIGINT         NULL,
        [veId6]                    BIGINT         NULL,
        [afId6]                    BIGINT         NULL,
        [osId6]                    BIGINT         NULL,
        [veId7]                    BIGINT         NULL,
        [afId7]                    BIGINT         NULL,
        [osId7]                    BIGINT         NULL,
        [veId8]                    BIGINT         NULL,
        [afId8]                    BIGINT         NULL,
        [osId8]                    BIGINT         NULL,
        [veId9]                    BIGINT         NULL,
        [afId9]                    BIGINT         NULL,
        [osId9]                    BIGINT         NULL,
        [veId10]                   BIGINT         NULL,
        [afId10]                   BIGINT         NULL,
        [osId10]                   BIGINT         NULL,
        [veId11]                   BIGINT         NULL,
        [afId11]                   BIGINT         NULL,
        [osId11]                   BIGINT         NULL,
        [veId12]                   BIGINT         NULL,
        [afId12]                   BIGINT         NULL,
        [osId12]                   BIGINT         NULL,
        [veId13]                   BIGINT         NULL,
        [afId13]                   BIGINT         NULL,
        [osId13]                   BIGINT         NULL,
        [veId14]                   BIGINT         NULL,
        [afId14]                   BIGINT         NULL,
        [osId14]                   BIGINT         NULL,
        [veId15]                   BIGINT         NULL,
        [afId15]                   BIGINT         NULL,
        [osId15]                   BIGINT         NULL,
        [veId16]                   BIGINT         NULL,
        [afId16]                   BIGINT         NULL,
        [osId16]                   BIGINT         NULL,
        [veId17]                   BIGINT         NULL,
        [afId17]                   BIGINT         NULL,
        [osId17]                   BIGINT         NULL,
        [veId18]                   BIGINT         NULL,
        [afId18]                   BIGINT         NULL,
        [osId18]                   BIGINT         NULL,
        [veId19]                   BIGINT         NULL,
        [afId19]                   BIGINT         NULL,
        [osId19]                   BIGINT         NULL,
        [veId20]                   BIGINT         NULL,
        [afId20]                   BIGINT         NULL,
        [osId20]                   BIGINT         NULL,
        [max_VirksomhedsenhedId]   BIGINT         NULL,
        [max_AfdelingId]           BIGINT         NULL,
        [nummer1]                  VARCHAR(100)   NULL,
        [nummer2]                  VARCHAR(100)   NULL,
        [nummer3]                  VARCHAR(100)   NULL,
        [nummer4]                  VARCHAR(100)   NULL,
        [nummer5]                  VARCHAR(100)   NULL,
        [nummer6]                  VARCHAR(100)   NULL,
        [nummer7]                  VARCHAR(100)   NULL,
        [nummer8]                  VARCHAR(100)   NULL,
        [nummer9]                  VARCHAR(100)   NULL,
        [nummer10]                 VARCHAR(100)   NULL,
        [nummer11]                 VARCHAR(100)   NULL,
        [nummer12]                 VARCHAR(100)   NULL,
        [nummer13]                 VARCHAR(100)   NULL,
        [nummer14]                 VARCHAR(100)   NULL,
        [nummer15]                 VARCHAR(100)   NULL,
        [nummer16]                 VARCHAR(100)   NULL,
        [nummer17]                 VARCHAR(100)   NULL,
        [nummer18]                 VARCHAR(100)   NULL,
        [nummer19]                 VARCHAR(100)   NULL,
        [nummer20]                 VARCHAR(100)   NULL,
        [tekst1]                   VARCHAR(1000)  NULL,
        [tekst2]                   VARCHAR(1000)  NULL,
        [tekst3]                   VARCHAR(1000)  NULL,
        [tekst4]                   VARCHAR(1000)  NULL,
        [tekst5]                   VARCHAR(1000)  NULL,
        [tekst6]                   VARCHAR(1000)  NULL,
        [tekst7]                   VARCHAR(1000)  NULL,
        [tekst8]                   VARCHAR(1000)  NULL,
        [tekst9]                   VARCHAR(1000)  NULL,
        [tekst10]                  VARCHAR(1000)  NULL,
        [tekst11]                  VARCHAR(1000)  NULL,
        [tekst12]                  VARCHAR(1000)  NULL,
        [tekst13]                  VARCHAR(1000)  NULL,
        [tekst14]                  VARCHAR(1000)  NULL,
        [tekst15]                  VARCHAR(1000)  NULL,
        [tekst16]                  VARCHAR(1000)  NULL,
        [tekst17]                  VARCHAR(1000)  NULL,
        [tekst18]                  VARCHAR(1000)  NULL,
        [tekst19]                  VARCHAR(1000)  NULL,
        [tekst20]                  VARCHAR(1000)  NULL,
        [Aktiv]                    INT            NULL,
        [HierarkiId]               BIGINT         NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [Slettet]                  INT            NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]      DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_omkostningssted_hierarki] PRIMARY KEY CLUSTERED ([Kildesystem], [OmkostningsstedHierarkiId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_omkostningssted_hierarki_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_omkostningssted_hierarki]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_omkostningssted_hierarki_Modified]
        ON [arc].[sydax_omkostningssted_hierarki] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_omkostningssted_hierarki_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_omkostningssted_hierarki]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_omkostningssted_hierarki_CreatedOn]
        ON [arc].[sydax_omkostningssted_hierarki] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.gruppering_sydax  (1:1 from SydAX [Finans].[Gruppering])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_gruppering'
)
BEGIN
    CREATE TABLE [arc].[sydax_gruppering] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [RECID]                                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[Gruppering], minus merge keys)
        [GrupperingId]                         BIGINT         NULL,
        [Grupperingsnummer]                    VARCHAR(20)    NULL,
        [Grupperingstekst]                     VARCHAR(60)    NULL,
        [GrupperingDimension]                  VARCHAR(60)    NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [Ejerforhold]                          VARCHAR(4)     NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                  DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_gruppering] PRIMARY KEY CLUSTERED ([Kildesystem], [RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_gruppering_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_gruppering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_gruppering_Modified]
        ON [arc].[sydax_gruppering] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_gruppering_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_gruppering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_gruppering_CreatedOn]
        ON [arc].[sydax_gruppering] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.gruppering_autoriseret_sydax  (1:1 from SydAX [Finans].[GrupperingAutoriseret])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_gruppering_autoriseret'
)
BEGIN
    CREATE TABLE [arc].[sydax_gruppering_autoriseret] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [ref_RECID]                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[GrupperingAutoriseret], minus merge keys)
        [G1Id]                     BIGINT         NULL,
        [G1]                       VARCHAR(4)     NULL,
        [G1Tekst]                  VARCHAR(360)   NULL,
        [F1]                       VARCHAR(4)     NULL,
        [F2]                       VARCHAR(4)     NULL,
        [F3]                       VARCHAR(4)     NULL,
        [D1]                       VARCHAR(4)     NULL,
        [GyldigFra]                DATE           NULL,
        [GyldigTil]                DATE           NULL,
        [Autoriseret]              INT            NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [ref_LedgerTextId]         BIGINT         NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]      DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_gruppering_autoriseret] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_gruppering_autoriseret_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_gruppering_autoriseret]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_gruppering_autoriseret_Modified]
        ON [arc].[sydax_gruppering_autoriseret] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_gruppering_autoriseret_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_gruppering_autoriseret]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_gruppering_autoriseret_CreatedOn]
        ON [arc].[sydax_gruppering_autoriseret] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.art_sydax  (1:1 from SydAX [Finans].[Art])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_art'
)
BEGIN
    CREATE TABLE [arc].[sydax_art] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [RECID]                                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[Art], minus merge keys)
        [ArtId]                                BIGINT         NULL,
        [Artsnummer]                           VARCHAR(20)    NULL,
        [Artstekst]                            VARCHAR(60)    NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [A1]                                   VARCHAR(4)     NULL,
        [A2]                                   VARCHAR(4)     NULL,
        [Momsretning]                          INT            NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [ArtDimension]                         VARCHAR(60)    NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                  DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_art] PRIMARY KEY CLUSTERED ([Kildesystem], [RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_art_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_art]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_art_Modified]
        ON [arc].[sydax_art] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_art_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_art]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_art_CreatedOn]
        ON [arc].[sydax_art] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.funktion_sydax  (1:1 from SydAX [Finans].[Funktion])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_funktion'
)
BEGIN
    CREATE TABLE [arc].[sydax_funktion] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [ref_RECID]                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[Funktion], minus merge keys)
        [FunktionId]               BIGINT         NULL,
        [F1]                       VARCHAR(4)     NULL,
        [F2]                       VARCHAR(4)     NULL,
        [F3]                       VARCHAR(4)     NULL,
        [FunktionTekst]            VARCHAR(360)   NULL,
        [GyldigFra]                DATE           NULL,
        [GyldigTil]                DATE           NULL,
        [Autoriseret]              INT            NULL,
        [ref_LedgerTextId]         BIGINT         NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]      DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_funktion] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_funktion_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_funktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_funktion_Modified]
        ON [arc].[sydax_funktion] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_funktion_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_funktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_funktion_CreatedOn]
        ON [arc].[sydax_funktion] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.dranst_sydax  (1:1 from SydAX [Finans].[Dranst])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_dranst'
)
BEGIN
    CREATE TABLE [arc].[sydax_dranst] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [ref_RECID]                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[Dranst], minus merge keys)
        [DranstId]                 BIGINT         NULL,
        [D1]                       VARCHAR(4)     NULL,
        [D1Tekst]                  VARCHAR(360)   NULL,
        [GyldigFra]                DATE           NULL,
        [GyldigTil]                DATE           NULL,
        [Autoriseret]              INT            NULL,
        [ref_LedgerTextId]         BIGINT         NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]      DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_dranst] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_dranst_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_dranst]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_dranst_Modified]
        ON [arc].[sydax_dranst] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_dranst_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_dranst]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_dranst_CreatedOn]
        ON [arc].[sydax_dranst] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.kreditor_sydax  (1:1 from SydAX [Kreditor].[Kreditor])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_kreditor'
)
BEGIN
    CREATE TABLE [arc].[sydax_kreditor] (
        -- Merge keys
        [Kildesystem]                  VARCHAR(10)    NOT NULL,
        [Kreditornummer]               VARCHAR(30)    NOT NULL,

        -- Source columns (1:1 from [Kreditor].[Kreditor], minus merge keys)
        [KreditorId]                   BIGINT         NOT NULL,
        [Kreditornavn]                 VARCHAR(100)   NULL,
        [Soegenavn]                    VARCHAR(100)   NULL,
        [JuridiskEnhed]                VARCHAR(30)    NULL,
        [ErBlokeret]                   BIT            NULL,
        [Oprettelsestidspunkt]         DATETIME2(7)   NULL,
        [OprettetAf]                   VARCHAR(10)    NULL,
        [SidstAendretTidspunkt]        DATETIME2(7)   NULL,
        [SidstAendretAf]               VARCHAR(10)    NULL,
        [ref_VendtableId]              BIGINT         NULL,
        [RunId]                        INT            NULL,
        [StartDateTime]                DATETIME2(0)   NOT NULL,
        [EndDateTime]                  DATETIME2(0)   NULL,
        [RdwTs]                        DATETIME2(7)   NULL,
        [DokumentId]                   BIGINT         NULL,
        [DataAreaId]                   NVARCHAR(4)    NULL,
        [CreatedOn]                    DATETIME       NULL,
        [Modified]                     DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]          DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]          DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_kreditor] PRIMARY KEY CLUSTERED ([Kildesystem], [KreditorId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditor_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditor]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditor_Modified]
        ON [arc].[sydax_kreditor] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditor_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditor]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditor_CreatedOn]
        ON [arc].[sydax_kreditor] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.finanspostering_sydax  (1:1 from SydAX [Finans].[Finanspostering])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanspostering'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanspostering] (
        -- Merge keys
        [Kildesystem]                      VARCHAR(10)      NOT NULL,
        [ref_LedgertransId]                BIGINT           NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanspostering], minus merge keys)
        [FinansposteringId]                BIGINT           NULL,
        [FinanskontoPrismeId]              BIGINT           NULL,
        [FinanskontoId]                    BIGINT           NULL,
        [Bogfoeringsdato]                  DATE             NULL,
        [Posteringsbeloeb]                 NUMERIC(28,12)   NULL,
        [Valutakode]                       CHAR(3)          NULL,
        [PosteringsbeloebValuta]           NUMERIC(28,12)   NULL,
        [Posteringstekst]                  VARCHAR(100)     NULL,
        [AfdelingPrismeId]                 BIGINT           NULL,
        [Bilagsnummer]                     VARCHAR(100)     NULL,
        [ErModpostering]                   BIT              NULL,
        [DebetKredit]                      CHAR(1)          NULL,
        [FinanskladdeId]                   BIGINT           NULL,
        [FinanstransaktionstypeId]         BIGINT           NULL,
        [FinansposteringstypeId]           BIGINT           NULL,
        [KontrolleretDato]                 DATE             NULL,
        [KontrolleretAf]                   VARCHAR(10)      NULL,
        [Oprettelsestidspunkt]             DATETIME2(7)     NULL,
        [OprettetAf]                       VARCHAR(10)      NULL,
        [RunId]                            INT              NULL,
        [StartDateTime]                    DATETIME2(0)     NULL,
        [EndDateTime]                      DATETIME2(0)     NULL,
        [RdwTs]                            DATETIME2(7)     NULL,
        [BilagsnummerID]                   BIGINT           NULL,
        [Posteringsmoms]                   NUMERIC(28,12)   NULL,
        [PosteringsbeloebInklMoms]         NUMERIC(28,12)   NULL,
        [DokumentId]                       BIGINT           NULL,
        [BærerId]                          BIGINT           NULL,
        [FormålId]                         BIGINT           NULL,
        [RefusionModtager]                 VARCHAR(10)      NULL,
        [Lager_Kasse]                      VARCHAR(10)      NULL,
        [Ekspeditionsnummer]               INT              NULL,
        [SDIdentifier]                     NVARCHAR(26)     NULL,
        [Primosaldo]                       NUMERIC(28,12)   NULL,
        [DataAreaId]                       NVARCHAR(4)      NULL,
        [FinansrelationId]                 BIGINT           NULL,
        [Oplysningspligt]                  CHAR(1)          NULL,
        [CreatedOn]                        DATETIME         NULL,
        [Modified]                         DATETIME         NULL,
        [Varemomsgruppe]                   VARCHAR(10)      NULL,
        [Bogføringstypetekst]              VARCHAR(1000)    NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]              DATETIME2(0)     NOT NULL,
        [DW_Timestamp_Update]              DATETIME2(0)     NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanspostering] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_LedgertransId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_Modified]
        ON [arc].[sydax_finanspostering] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_CreatedOn]
        ON [arc].[sydax_finanspostering] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.kreditortransaktion_sydax  (1:1 from SydAX [Kreditor].[Kreditortransaktion])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_kreditortransaktion'
)
BEGIN
    CREATE TABLE [arc].[sydax_kreditortransaktion] (
        -- Merge keys
        [Kildesystem]                              VARCHAR(10)      NOT NULL,
        [ref_VendTransId]                          BIGINT           NOT NULL,

        -- Source columns (1:1 from [Kreditor].[Kreditortransaktion], minus merge keys)
        [KreditortransaktionId]                    BIGINT           NULL,
        [KreditorId]                               BIGINT           NULL,
        [Transaktionsdato]                         DATE             NULL,
        [Bilagsnummer]                             VARCHAR(100)     NULL,
        [Fakturanummer]                            VARCHAR(100)     NULL,
        [EANnummer]                                VARCHAR(100)     NULL,
        [Fakturadato]                              DATE             NULL,
        [Transaktionsbeloeb]                       NUMERIC(28,12)   NULL,
        [Valutakode]                               CHAR(3)          NULL,
        [TransaktionsbeloebValuta]                 NUMERIC(28,12)   NULL,
        [UdlignetBeloeb]                           NUMERIC(28,12)   NULL,
        [UdlignetBeloebValuta]                     NUMERIC(28,12)   NULL,
        [Transaktionstekst]                        VARCHAR(100)     NULL,
        [Forfaldsdato]                             DATE             NULL,
        [ErLukket]                                 BIT              NULL,
        [LukketDato]                               DATE             NULL,
        [AfdelingPrismeId]                         BIGINT           NULL,
        [JuridiskEnhed]                            VARCHAR(30)      NULL,
        [Bankregistreringsnummer]                  VARCHAR(10)      NULL,
        [Bankkontonummer]                          VARCHAR(30)      NULL,
        [ErKorrektion]                             BIT              NULL,
        [KreditortransaktionstypeId]               BIGINT           NULL,
        [ErGodkendt]                               BIT              NULL,
        [GodkendtDato]                             DATE             NULL,
        [GodkendtAf]                               VARCHAR(50)      NULL,
        [Oprettelsestidspunkt]                     DATETIME2(7)     NULL,
        [OprettetAf]                               VARCHAR(10)      NULL,
        [IsCounterpost]                            BIT              NULL,
        [CounterpostsThisKreditortransaktionId]    BIGINT           NULL,
        [Betalingstekst]                           NVARCHAR(255)    NULL,
        [BetalingReturkode]                        VARCHAR(4)       NULL,
        [BetalingYderligerInfo]                    VARCHAR(105)     NULL,
        [Virksomhedsenhed]                         NVARCHAR(20)     NULL,
        [VirksomhedsenhedNavn]                     NVARCHAR(100)    NULL,
        [RunId]                                    INT              NULL,
        [StartDateTime]                            DATETIME2(0)     NULL,
        [EndDateTime]                              DATETIME2(0)     NULL,
        [RdwTs]                                    DATETIME2(7)     NULL,
        [SidstUdlignetDato]                        DATE             NULL,
        [SidstUdlignetBilagsnummer]                VARCHAR(100)     NULL,
        [DokumentId]                               BIGINT           NULL,
        [DataAreaId]                               NVARCHAR(4)      NULL,
        [CreatedOn]                                DATETIME         NULL,
        [Modified]                                 DATETIME         NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                      DATETIME2(0)     NOT NULL,
        [DW_Timestamp_Update]                      DATETIME2(0)     NOT NULL,

        CONSTRAINT [PK_arc_sydax_kreditortransaktion] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_VendTransId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditortransaktion_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditortransaktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditortransaktion_Modified]
        ON [arc].[sydax_kreditortransaktion] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditortransaktion_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditortransaktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditortransaktion_CreatedOn]
        ON [arc].[sydax_kreditortransaktion] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.finanspostering_kreditortrans_relation_sydax  (1:1 from SydAX [Finans].[FinansposteringKreditortransRelation])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanspostering_kreditortrans_relation'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanspostering_kreditortrans_relation] (
        -- Merge keys
        [Kildesystem]                              VARCHAR(10)    NOT NULL,
        [ref_LedgertransId]                        BIGINT         NOT NULL,
        [KreditortransaktionId]                    BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[FinansposteringKreditortransRelation], minus merge keys)
        -- Note: ref_LedgertransId above is a synthetic column not present in source DDL;
        --       populated via JOIN on Finans.Finanspostering in the Task 2 extract.
        [FinansposteringKreditortransRelationId]   BIGINT         NULL,
        [FinansposteringId]                        BIGINT         NULL,
        [RunId]                                    INT            NULL,
        [StartDateTime]                            DATETIME2(0)   NULL,
        [EndDateTime]                              DATETIME2(0)   NULL,
        [RdwTs]                                    DATETIME2(7)   NULL,
        [CreatedOn]                                DATETIME       NULL,
        [Modified]                                 DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]                      DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]                      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanspostering_kreditortrans_relation] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_LedgertransId], [KreditortransaktionId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_kreditortrans_relation_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering_kreditortrans_relation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_kreditortrans_relation_Modified]
        ON [arc].[sydax_finanspostering_kreditortrans_relation] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_kreditortrans_relation_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering_kreditortrans_relation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_kreditortrans_relation_CreatedOn]
        ON [arc].[sydax_finanspostering_kreditortrans_relation] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.finanskladde_sydax  (1:1 from SydAX [Finans].[Finanskladde])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskladde'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskladde] (
        -- Merge keys
        [Kildesystem]                      VARCHAR(10)    NOT NULL,
        [Kladdenummer]                     VARCHAR(20)    NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanskladde], minus merge keys)
        [FinanskladdeId]                   BIGINT         NOT NULL,
        [Kladdetekst]                      VARCHAR(1000)  NULL,
        [Kladdenavn]                       VARCHAR(100)   NULL,
        [ErLukket]                         BIT            NULL,
        [ref_LedgerjournaltableId]         BIGINT         NULL,
        [RunId]                            INT            NULL,
        [StartDateTime]                    DATETIME2(0)   NOT NULL,
        [EndDateTime]                      DATETIME2(0)   NULL,
        [RdwTs]                            DATETIME2(7)   NULL,
        [DokumentId]                       BIGINT         NULL,
        [DataAreaId]                       NVARCHAR(4)    NULL,
        [Maskinnummer]                     VARCHAR(5)     NULL,
        [CreatedOn]                        DATETIME       NULL,
        [Modified]                         DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]              DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]              DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskladde] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanskladdeId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladde_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladde]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladde_Modified]
        ON [arc].[sydax_finanskladde] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladde_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladde]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladde_CreatedOn]
        ON [arc].[sydax_finanskladde] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.finanskladdelinje_sydax  (1:1 from SydAX [Finans].[Finanskladdelinje])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskladdelinje'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskladdelinje] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)      NOT NULL,
        [ref_LedgertransId]        BIGINT           NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanskladdelinje], minus merge keys)
        [FinanskladdelinjeId]      BIGINT           NULL,
        [Linjenummer]              DECIMAL(32,16)   NULL,
        [dato]                     DATE             NULL,
        [Beskrivelse]              VARCHAR(100)     NULL,
        [Debet]                    NUMERIC(28,12)   NULL,
        [Kredit]                   NUMERIC(28,12)   NULL,
        [FinanskladdeId]           BIGINT           NULL,
        [FinanstransaktionstypeId] BIGINT           NULL,
        [Ekspeditionsnummer]       VARCHAR(7)       NULL,
        [Maskinnummer]             VARCHAR(5)       NULL,
        [FinanskontoId]            BIGINT           NULL,
        [BilagsnummerID]           BIGINT           NULL,
        [DokumentId]               BIGINT           NULL,
        [ref_RecId]                BIGINT           NULL,
        [RunId]                    INT              NULL,
        [StartDateTime]            DATETIME2(0)     NULL,
        [EndDateTime]              DATETIME2(0)     NULL,
        [RdwTs]                    DATETIME2(7)     NULL,
        [CreatedOn]                DATETIME         NULL,
        [Modified]                 DATETIME         NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]      DATETIME2(0)     NOT NULL,
        [DW_Timestamp_Update]      DATETIME2(0)     NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskladdelinje] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_LedgertransId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladdelinje_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladdelinje]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladdelinje_Modified]
        ON [arc].[sydax_finanskladdelinje] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladdelinje_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladdelinje]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladdelinje_CreatedOn]
        ON [arc].[sydax_finanskladdelinje] ([CreatedOn] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.finanstransaktionstype_sydax  (1:1 from SydAX [Finans].[Finanstransaktionstype])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanstransaktionstype'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanstransaktionstype] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [Transaktionstypenummer]   INT            NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanstransaktionstype], minus merge keys)
        [FinanstransaktionstypeId] BIGINT         NOT NULL,
        [Transaktionstypetekst]    VARCHAR(1000)  NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [DW_Timestamp_Insert]      DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanstransaktionstype] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanstransaktionstypeId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanstransaktionstype_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanstransaktionstype]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanstransaktionstype_Modified]
        ON [arc].[sydax_finanstransaktionstype] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanstransaktionstype_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanstransaktionstype]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanstransaktionstype_CreatedOn]
        ON [arc].[sydax_finanstransaktionstype] ([CreatedOn] DESC);
END
GO
