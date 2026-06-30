-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Fact Tables
-- ============================================================================

-- fct.Fakturalinje
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fct].[Fakturalinje] (
    [DW_EK_Fakturalinje] INT IDENTITY(1, 1) NOT NULL,
    -- Canonical key + business-key traceability columns
    [CleanCode_Fakturalinje] NVARCHAR(500) NULL,
    [bk_FakturaLines] NVARCHAR(500) NULL,
    [bk_FakturaHeader] NVARCHAR(500) NULL,
    [bk_FakturaMetadata] NVARCHAR(500) NULL,
    -- Foreign key lookups
    [DW_EK_Kreditor] INT NOT NULL DEFAULT -1,
    [DW_EK_Vare] INT NOT NULL DEFAULT -1,
    [DW_EK_Faktura] INT NOT NULL DEFAULT -1,
    [DW_EK_EAN_Regningsmodtager] INT NOT NULL DEFAULT -1,
    [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Faktura] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Ordre] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Betaling] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Modtagelse] INT NOT NULL DEFAULT -1,
    [FakturaNr] NVARCHAR(100) NULL,
    [AntalEnhederFaktureret] DECIMAL(18, 4) NULL,
    [LinjeBeloeb] DECIMAL(18, 4) NULL,
    [EnhedsPris] DECIMAL(18, 4) NULL,
    [Valutakode_Linje] NVARCHAR(10) NULL,
    [Valutakode_Enhedspris] NVARCHAR(10) NULL,
    [Valutakode_Faktura] NVARCHAR(10) NULL,
    -- Summed allowance/charges per line (detail on dim.Fakturalinje as '#'-concat)
    [GebyrBeloeb] DECIMAL(18, 4) NULL,
    [RabatBeloeb] DECIMAL(18, 4) NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Fct_Fakturalinje] PRIMARY KEY CLUSTERED ([DW_EK_Fakturalinje] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_Kreditor')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_Kreditor]
    ON [fct].[Fakturalinje] ([DW_EK_Kreditor] ASC) INCLUDE ([DW_EK_Vare], [FakturaNr]) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_CleanCode')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_CleanCode]
    ON [fct].[Fakturalinje] ([CleanCode_Fakturalinje] ASC)
    WHERE [CleanCode_Fakturalinje] IS NOT NULL ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_EAN')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_EAN]
    ON [fct].[Fakturalinje] ([DW_EK_EAN_Regningsmodtager] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_UNSPSC')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
         AND name = N'DW_EK_UNSPSC'
   )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_UNSPSC]
    ON [fct].[Fakturalinje] ([DW_EK_UNSPSC] ASC) ON [PRIMARY];
END
GO

-- fct.Postering
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fct].[Postering]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fct].[Postering] (
    [ref_LedgertransId] BIGINT NOT NULL,
    [DW_EK_Postering] INT NOT NULL DEFAULT -1,
    [CleanCode_Postering] NVARCHAR(100) NULL,
    [DW_EK_Kreditor] INT NOT NULL DEFAULT -1,
    [DW_EK_Kontoplan] INT NOT NULL DEFAULT -1,
    [DW_EK_Faktura] INT NOT NULL DEFAULT -1,
    [DW_EK_Okosted] INT NOT NULL DEFAULT -1,
    [DW_EK_Finanskladdelinje] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Godkendelse] INT NULL,
    [DW_EK_Dato_Lukket] INT NULL,
    [DW_EK_Dato_TransOprettet] INT NULL,
    [DW_EK_Dato_Forfald] INT NULL,
    [DW_EK_Dato_JournalFaktura] INT NULL,
    [DW_EK_Dato_SidstUdlignet] INT NULL,
    [DW_EK_Dato_Transaktion] INT NULL,
    [DW_EK_Dato_Bogfoering] INT NULL,
    [DW_EK_Dato_PostOprettet] INT NULL,
    [TransaktionsbeloebValuta] DECIMAL(30, 12) NULL,
    [Transaktionsbeloeb] DECIMAL(30, 12) NULL,
    [UdlignetBeloebValuta] DECIMAL(30, 12) NULL,
    [UdlignetBeloeb] DECIMAL(30, 12) NULL,
    [Posteringsbeloeb] DECIMAL(30, 12) NULL,
    [FakturaNr] NVARCHAR(100) NULL,
    [DW_haendelsestidspunkt] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Fct_Postering] PRIMARY KEY CLUSTERED ([ref_LedgertransId] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Kreditor')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Kreditor] ON [fct].[Postering] (
        [DW_EK_Kreditor] ASC,
        [DW_EK_Kontoplan] ASC
    ) INCLUDE (
        [FakturaNr]
    ) ON [PRIMARY];
END
GO

-- fct.Faktura
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fct].[Faktura]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fct].[Faktura] (
    [DW_EK_Faktura] INT NOT NULL,           -- PK from map.Faktura (NOT IDENTITY)
    [DW_EK_Kreditor] INT NOT NULL DEFAULT -1,
    [DW_EK_EAN_Regningsmodtager] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Faktura] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Ordre] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Betaling] INT NOT NULL DEFAULT -1,
    [FakturaNr] NVARCHAR(100) NULL,
    [MomsProcent] DECIMAL(9, 4) NULL,
    [MomsGrundlag] DECIMAL(18, 4) NULL,
    [MomsBeloeb] DECIMAL(18, 4) NULL,
    [LinjeTotal] DECIMAL(18, 4) NULL,
    [FakturaTotal] DECIMAL(18, 4) NULL,
    [Valutakode_Faktura] NVARCHAR(10) NULL,
    [VAT_AfgiftsKategori] NVARCHAR(50) NULL,
    [Valutakode_MomsGrundlag] NVARCHAR(10) NULL,
    [Valutakode_MomsBeloeb] NVARCHAR(10) NULL,
    [Valutakode_LinjeTotal] NVARCHAR(10) NULL,
    [Valutakode_FakturaTotal] NVARCHAR(10) NULL,
    -- Zero-rated VAT measures (header grain, from ZERO-RATED_TaxAmounts_*)
    [ZeroRated_MomsGrundlag] DECIMAL(18, 4) NULL,
    [Valutakode_ZeroRated_MomsGrundlag] NVARCHAR(10) NULL,
    [ZeroRated_MomsBeloeb] DECIMAL(18, 4) NULL,
    [Valutakode_ZeroRated_MomsBeloeb] NVARCHAR(10) NULL,
    -- Summed allowance/charges per header (detail on dim.Faktura as '#'-concat)
    [GebyrBeloeb] DECIMAL(18, 4) NULL,
    [RabatBeloeb] DECIMAL(18, 4) NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Fct_Faktura] PRIMARY KEY CLUSTERED ([DW_EK_Faktura] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'IX_Fct_Faktura_EAN')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Faktura_EAN]
    ON [fct].[Faktura] ([DW_EK_EAN_Regningsmodtager] ASC) ON [PRIMARY];
END
GO

-- fct.Vareudlevering
-- Source: ILS dbo.udlev — delivery lines from central warehouse to departments
-- Grain: one row per delivery line (identified by dbo.udlev.id)

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[Vareudlevering](
        [DW_EK_Vareudlevering] INT NOT NULL,
        [CleanCode_Vareudlevering] NVARCHAR(500) NULL,
        [bk_prrowid] NVARCHAR(100) NULL,
        [DW_EK_IlsVare] INT NOT NULL DEFAULT -1,
        [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1,
        [DW_EK_Lager] BIGINT NOT NULL DEFAULT -1,
        [DW_EK_Rekvirent] INT NOT NULL DEFAULT -1,
        [DW_EK_Vare] BIGINT NOT NULL DEFAULT -1,
        [DW_EK_VareudleveringEgenskaber] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Vareudlevering] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Udloeb] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Postering] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Aflevering] INT NOT NULL DEFAULT -1,
        [Vareudlevering_Antal] DECIMAL(18,4) NULL,
        [Vareudlevering_Beloeb] DECIMAL(19,4) NULL,
        [Indkoeb_Beloeb] DECIMAL(19,4) NULL,
        [Rabat_Beloeb] DECIMAL(19,4) NULL,
        [Lager_Antal] DECIMAL(18,4) NULL,
        [Datotid_Vareudlevering] DATETIME2 NULL,
        [Datotid_Udloeb] DATETIME2 NULL,
        [Datotid_Postering] DATETIME2 NULL,
        [Datotid_Aflevering] DATETIME2 NULL,
        [DW_haendelsestidspunkt] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Fct_Vareudlevering] PRIMARY KEY CLUSTERED ([DW_EK_Vareudlevering] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_CleanCode] ON [fct].[Vareudlevering]
    ([CleanCode_Vareudlevering] ASC) WHERE [CleanCode_Vareudlevering] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_IlsVare] ON [fct].[Vareudlevering]
    ([DW_EK_IlsVare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_UNSPSC] ON [fct].[Vareudlevering]
    ([DW_EK_UNSPSC] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_bk_prrowid] ON [fct].[Vareudlevering]
    ([bk_prrowid] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Lager] ON [fct].[Vareudlevering]
    ([DW_EK_Lager] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Rekvirent] ON [fct].[Vareudlevering]
    ([DW_EK_Rekvirent] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Vare] ON [fct].[Vareudlevering]
    ([DW_EK_Vare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_VareudleveringEgenskaber] ON [fct].[Vareudlevering]
    ([DW_EK_VareudleveringEgenskaber] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Vareudlevering] ON [fct].[Vareudlevering]
    ([DW_EK_Dato_Vareudlevering] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Udloeb] ON [fct].[Vareudlevering]
    ([DW_EK_Dato_Udloeb] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Postering] ON [fct].[Vareudlevering]
    ([DW_EK_Dato_Postering] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Aflevering] ON [fct].[Vareudlevering]
    ([DW_EK_Dato_Aflevering] ASC) ON [PRIMARY];
END
GO

-- fct.Lagerbeholdning (Periodic Snapshot — Warehouse Stock)
-- Source: ILS dbo.lgrbeh — daily full-load inventory snapshot
-- Grain: one row per (snapshot date, sgh, lgr, vnr, lgrlok)
-- Load pattern: MERGE via stable DW_EK from map.Lagerbeholdning
-- Soft-delete: DW_IsDeleted=1 for rows that disappear from a snapshot

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[Lagerbeholdning](
        [DW_EK_Lagerbeholdning] BIGINT NOT NULL,
        [DW_EK_Lager] BIGINT NOT NULL DEFAULT -1,
        [DW_EK_IlsVare] INT NOT NULL DEFAULT -1,
        [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1,
        [DW_EK_LagerbeholdningEgenskaber] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Snapshot] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Udloeb] INT NOT NULL DEFAULT -1,
        [Datotid_Snapshot] DATETIME2 NULL,
        [Antal] DECIMAL(18,3) NULL,
        [DW_IsDeleted] BIT NOT NULL DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Fct_Lagerbeholdning] PRIMARY KEY CLUSTERED ([DW_EK_Lagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_Lager] ON [fct].[Lagerbeholdning]
    ([DW_EK_Lager] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_IlsVare] ON [fct].[Lagerbeholdning]
    ([DW_EK_IlsVare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_UNSPSC] ON [fct].[Lagerbeholdning]
    ([DW_EK_UNSPSC] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_Dato_Snapshot] ON [fct].[Lagerbeholdning]
    ([DW_EK_Dato_Snapshot] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_Dato_Udloeb] ON [fct].[Lagerbeholdning]
    ([DW_EK_Dato_Udloeb] ASC) ON [PRIMARY];
END
GO

-- fct.AfdelingLagerbeholdning (Periodic Snapshot — Department Stock)
-- Source: ILS dbo.afdlgrbeh — daily full-load inventory snapshot
-- Grain: one row per (snapshot date, sgh, lgr, vnr, brik, rnr)
-- Load pattern: MERGE via stable DW_EK from map.AfdelingLagerbeholdning
-- Soft-delete: DW_IsDeleted=1 for rows that disappear from a snapshot

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[AfdelingLagerbeholdning](
        [DW_EK_AfdelingLagerbeholdning] BIGINT NOT NULL,
        [DW_EK_Lager] BIGINT NOT NULL DEFAULT -1,
        [DW_EK_Rekvirent] INT NOT NULL DEFAULT -1,
        [DW_EK_IlsVare] INT NOT NULL DEFAULT -1,
        [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1,
        [DW_EK_AfdelingLagerbeholdningEgenskaber] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Snapshot] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Udloeb] INT NOT NULL DEFAULT -1,
        [Datotid_Snapshot] DATETIME2 NULL,
        [Antal] DECIMAL(18,3) NULL,
        [DW_IsDeleted] BIT NOT NULL DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Fct_AfdelingLagerbeholdning] PRIMARY KEY CLUSTERED ([DW_EK_AfdelingLagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Lager] ON [fct].[AfdelingLagerbeholdning]
    ([DW_EK_Lager] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Rekvirent] ON [fct].[AfdelingLagerbeholdning]
    ([DW_EK_Rekvirent] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_IlsVare] ON [fct].[AfdelingLagerbeholdning]
    ([DW_EK_IlsVare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_UNSPSC] ON [fct].[AfdelingLagerbeholdning]
    ([DW_EK_UNSPSC] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Dato_Snapshot] ON [fct].[AfdelingLagerbeholdning]
    ([DW_EK_Dato_Snapshot] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Dato_Udloeb] ON [fct].[AfdelingLagerbeholdning]
    ([DW_EK_Dato_Udloeb] ASC) ON [PRIMARY];

    PRINT 'Created fct.AfdelingLagerbeholdning';
END
GO

-- fct.IndkoebsanalyseLinje
-- Source: KMD fact.SpendAnalysisLines via arc.spend_analysis_lines_kmd_uddata
-- Grain: one row per DW_EK_SpendAnalysisLine (KMD natural key)

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fct].[IndkoebsanalyseLinje]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fct].[IndkoebsanalyseLinje](
        [DW_EK_IndkoebsanalyseLinje] INT NOT NULL,
        [CleanCode_IndkoebsanalyseLinje] NVARCHAR(500) NULL,
        [bk_SpendAnalysisLine] NVARCHAR(20) NULL,
        [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_SpendAnalysis] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Issue] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_OrderIssue] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_ActualDelivery] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_PaymentDue] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Posting] INT NOT NULL DEFAULT -1,
        [IsCreditnote] BIT NOT NULL DEFAULT 0,
        [LineAmount] NUMERIC(38, 6) NULL,
        [InvoicedQuantity] NUMERIC(38, 6) NULL,
        [UnitPrice] NUMERIC(38, 6) NULL,
        [LineTaxAmount] NUMERIC(38, 6) NULL,
        [CO2Emission] NUMERIC(38, 6) NULL,
        [DW_haendelsestidspunkt] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NOT NULL,
        [DW_Timestamp_Update] DATETIME2 NOT NULL,
        CONSTRAINT [PK_Fct_IndkoebsanalyseLinje] PRIMARY KEY CLUSTERED
            ([DW_EK_IndkoebsanalyseLinje] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_IndkoebsanalyseLinje_CleanCode]
        ON [fct].[IndkoebsanalyseLinje] ([CleanCode_IndkoebsanalyseLinje] ASC)
        WHERE [CleanCode_IndkoebsanalyseLinje] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_IndkoebsanalyseLinje_UNSPSC]
        ON [fct].[IndkoebsanalyseLinje] ([DW_EK_UNSPSC] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_IndkoebsanalyseLinje_bk_SpendAnalysisLine]
        ON [fct].[IndkoebsanalyseLinje] ([bk_SpendAnalysisLine] ASC) ON [PRIMARY];

    PRINT 'Created fct.IndkoebsanalyseLinje';
END
GO

-- fct.Finanskladdelinje
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
        AND type = 'U'
)
BEGIN
    CREATE TABLE [fct].[Finanskladdelinje](
        [DW_EK_Finanskladdelinje] INT NOT NULL,
        [bk_ref_LedgertransId] NVARCHAR(100) NULL,
        [DW_EK_Finanskladde] INT NOT NULL DEFAULT -1,
        [DW_EK_Kontoplan] INT NOT NULL DEFAULT -1,
        [DW_EK_Okosted] INT NOT NULL DEFAULT -1,
        [Debet] DECIMAL(30,12) NULL,
        [Kredit] DECIMAL(30,12) NULL,
        [DW_EK_Dato_Linje] INT NOT NULL DEFAULT -1,
        [DW_EK_Dato_Oprettet] INT NOT NULL DEFAULT -1,
        [DW_haendelsestidspunkt] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NOT NULL,
        [DW_Timestamp_Update] DATETIME2 NOT NULL,
        CONSTRAINT [PK_Fct_Finanskladdelinje] PRIMARY KEY CLUSTERED ([DW_EK_Finanskladdelinje] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = 'IX_Fct_Finanskladdelinje_Finanskladde')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Finanskladdelinje_Finanskladde]
    ON [fct].[Finanskladdelinje] ([DW_EK_Finanskladde] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = 'IX_Fct_Finanskladdelinje_Kontoplan')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Finanskladdelinje_Kontoplan]
    ON [fct].[Finanskladdelinje] ([DW_EK_Kontoplan] ASC) ON [PRIMARY];
END
GO

-- fct.Budget
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U'
) BEGIN
CREATE TABLE [fct].[Budget] (
    [DW_EK_Budget] INT NOT NULL,
    [bk_ref_LedgerbudgetId] NVARCHAR(100) NOT NULL,
    [DW_EK_BudgetEgenskaber] INT NOT NULL DEFAULT -1,
    [DW_EK_Kontoplan] INT NOT NULL DEFAULT -1,
    [DW_EK_Okosted] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_Budget] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_BudgetAdopted] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_BudgetOprettet] INT NOT NULL DEFAULT -1,
    [DW_EK_Dato_BudgetAendret] INT NOT NULL DEFAULT -1,
    [Budgetbeloeb] DECIMAL(30, 12) NULL,
    [BudgetbeloebValuta] DECIMAL(30, 12) NULL,
    [DW_haendelsestidspunkt] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Fct_Budget] PRIMARY KEY CLUSTERED ([DW_EK_Budget] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'IX_Fct_Budget_BudgetEgenskaber')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Budget_BudgetEgenskaber]
    ON [fct].[Budget] ([DW_EK_BudgetEgenskaber] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'IX_Fct_Budget_Kontoplan')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Budget_Kontoplan]
    ON [fct].[Budget] ([DW_EK_Kontoplan] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'IX_Fct_Budget_Dato_Budget')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Budget_Dato_Budget]
    ON [fct].[Budget] ([DW_EK_Dato_Budget] ASC) ON [PRIMARY];
END
GO
