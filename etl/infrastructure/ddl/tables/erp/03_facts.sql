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
    [dw_ek_Fakturalinje] INT NOT NULL,
    -- Canonical key + business-key traceability columns
    [clean_code_Fakturalinje] NVARCHAR(500) NULL,
    [bk_faktura_lines] NVARCHAR(500) NULL,
    [bk_faktura_header] NVARCHAR(500) NULL,
    [bk_faktura_metadata] NVARCHAR(500) NULL,
    -- Foreign key lookups
    [dw_ek_Kreditor] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_Kreditor] DEFAULT -1,
    [dw_ek_Leverandoervare] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_Leverandoervare] DEFAULT -1,
    [dw_ek_Faktura] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_Faktura] DEFAULT -1,
    [dw_ek_EAN_regningsmodtager] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_EAN_regningsmodtager] DEFAULT -1,
    [dw_ek_UNSPSC] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_UNSPSC] DEFAULT -1,
    [dw_ek_Dato_faktura] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_Dato_faktura] DEFAULT -1,
    [dw_ek_Dato_ordre] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_Dato_ordre] DEFAULT -1,
    [dw_ek_Dato_betaling] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_Dato_betaling] DEFAULT -1,
    [dw_ek_Dato_modtagelse] INT NOT NULL CONSTRAINT [DF_Fakturalinje_dw_ek_Dato_modtagelse] DEFAULT -1,
    [dato_tid_faktura] DATETIME2 NULL,
    [dato_tid_ordre] DATETIME2 NULL,
    [dato_tid_betaling] DATETIME2 NULL,
    [dato_tid_modtagelse] DATETIME2 NULL,
    [faktura_nr] NVARCHAR(100) NULL,
    [antal_enheder_faktureret] DECIMAL(18, 4) NULL,
    [linje_beloeb] DECIMAL(18, 4) NULL,
    [enheds_pris] DECIMAL(18, 4) NULL,
    [valutakode_linje] NVARCHAR(10) NULL,
    [valutakode_enhedspris] NVARCHAR(10) NULL,
    [valutakode_faktura] NVARCHAR(10) NULL,
    -- Summed allowance/charges per line (detail on dim.Fakturalinje as '#'-concat)
    [gebyr_beloeb] DECIMAL(18, 4) NULL,
    [rabatbeloeb] DECIMAL(18, 4) NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Fct_Fakturalinje] PRIMARY KEY CLUSTERED ([dw_ek_Fakturalinje] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_Kreditor')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_Kreditor]
    ON [fct].[Fakturalinje] ([dw_ek_Kreditor] ASC) INCLUDE ([dw_ek_Leverandoervare], [faktura_nr]) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_clean_code')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_clean_code]
    ON [fct].[Fakturalinje] ([clean_code_Fakturalinje] ASC)
    WHERE [clean_code_Fakturalinje] IS NOT NULL ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_ean')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_ean]
    ON [fct].[Fakturalinje] ([dw_ek_EAN_regningsmodtager] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_UNSPSC')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
         AND name = N'dw_ek_UNSPSC'
   )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_UNSPSC]
    ON [fct].[Fakturalinje] ([dw_ek_UNSPSC] ASC) ON [PRIMARY];
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
    [ref_ledgertrans_id] BIGINT NOT NULL,
    [dw_ek_Postering] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Postering] DEFAULT -1,
    [clean_code_Postering] NVARCHAR(100) NULL,
    [dw_ek_Kreditor] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Kreditor] DEFAULT -1,
    [dw_ek_Kontoplan] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Kontoplan] DEFAULT -1,
    [dw_ek_Faktura] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Faktura] DEFAULT -1,
    [dw_ek_Okosted] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Okosted] DEFAULT -1,
    [dw_ek_Finanskladdelinje] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Finanskladdelinje] DEFAULT -1,
    [dw_ek_Dato_godkendelse] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_godkendelse] DEFAULT -1,
    [dato_tid_godkendelse] DATETIME2 NULL,
    [dw_ek_Dato_lukket] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_lukket] DEFAULT -1,
    [dato_tid_lukket] DATETIME2 NULL,
    [dw_ek_Dato_trans_oprettet] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_trans_oprettet] DEFAULT -1,
    [dato_tid_trans_oprettet] DATETIME2 NULL,
    [dw_ek_Dato_forfald] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_forfald] DEFAULT -1,
    [dato_tid_forfald] DATETIME2 NULL,
    [dw_ek_Dato_journal_faktura] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_journal_faktura] DEFAULT -1,
    [dato_tid_journal_faktura] DATETIME2 NULL,
    [dw_ek_Dato_sidst_udlignet] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_sidst_udlignet] DEFAULT -1,
    [dato_tid_sidst_udlignet] DATETIME2 NULL,
    [dw_ek_Dato_transaktion] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_transaktion] DEFAULT -1,
    [dato_tid_transaktion] DATETIME2 NULL,
    [dw_ek_Dato_bogfoering] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_bogfoering] DEFAULT -1,
    [dato_tid_bogfoering] DATETIME2 NULL,
    [dw_ek_Dato_post_oprettet] INT NOT NULL CONSTRAINT [DF_Postering_dw_ek_Dato_post_oprettet] DEFAULT -1,
    [dato_tid_post_oprettet] DATETIME2 NULL,
    [transaktionsbeloeb_valuta] DECIMAL(30, 12) NULL,
    [transaktionsbeloeb] DECIMAL(30, 12) NULL,
    [udlignet_beloeb_valuta] DECIMAL(30, 12) NULL,
    [udlignet_beloeb] DECIMAL(30, 12) NULL,
    [posteringsbeloeb] DECIMAL(30, 12) NULL,
    [faktura_nr] NVARCHAR(100) NULL,
    [dw_haendelsestidspunkt] DATETIME2 NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Fct_Postering] PRIMARY KEY CLUSTERED ([ref_ledgertrans_id] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Kreditor')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Kreditor] ON [fct].[Postering] (
        [dw_ek_Kreditor] ASC,
        [dw_ek_Kontoplan] ASC
    ) INCLUDE (
        [faktura_nr]
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
    [dw_ek_Faktura] INT NOT NULL,           -- PK from map.Faktura (NOT IDENTITY)
    [dw_ek_Kreditor] INT NOT NULL CONSTRAINT [DF_Faktura_dw_ek_Kreditor] DEFAULT -1,
    [dw_ek_EAN_regningsmodtager] INT NOT NULL CONSTRAINT [DF_Faktura_dw_ek_EAN_regningsmodtager] DEFAULT -1,
    [dw_ek_Dato_faktura] INT NOT NULL CONSTRAINT [DF_Faktura_dw_ek_Dato_faktura] DEFAULT -1,
    [dw_ek_Dato_ordre] INT NOT NULL CONSTRAINT [DF_Faktura_dw_ek_Dato_ordre] DEFAULT -1,
    [dw_ek_Dato_betaling] INT NOT NULL CONSTRAINT [DF_Faktura_dw_ek_Dato_betaling] DEFAULT -1,
    [dato_tid_faktura] DATETIME2 NULL,
    [dato_tid_ordre] DATETIME2 NULL,
    [dato_tid_betaling] DATETIME2 NULL,
    [faktura_nr] NVARCHAR(100) NULL,
    [moms_procent] DECIMAL(9, 4) NULL,
    [moms_grundlag] DECIMAL(18, 4) NULL,
    [momsbeloeb] DECIMAL(18, 4) NULL,
    [linje_total] DECIMAL(18, 4) NULL,
    [faktura_total] DECIMAL(18, 4) NULL,
    [valutakode_faktura] NVARCHAR(10) NULL,
    [vat_afgifts_kategori] NVARCHAR(50) NULL,
    [valutakode_moms_grundlag] NVARCHAR(10) NULL,
    [valutakode_momsbeloeb] NVARCHAR(10) NULL,
    [valutakode_linje_total] NVARCHAR(10) NULL,
    [valutakode_faktura_total] NVARCHAR(10) NULL,
    -- Zero-rated VAT measures (header grain, from ZERO-RATED_TaxAmounts_*)
    [zero_rated_moms_grundlag] DECIMAL(18, 4) NULL,
    [valutakode_zero_rated_moms_grundlag] NVARCHAR(10) NULL,
    [zero_rated_momsbeloeb] DECIMAL(18, 4) NULL,
    [valutakode_zero_rated_momsbeloeb] NVARCHAR(10) NULL,
    -- Summed allowance/charges per header (detail on dim.Faktura as '#'-concat)
    [gebyr_beloeb] DECIMAL(18, 4) NULL,
    [rabatbeloeb] DECIMAL(18, 4) NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Fct_Faktura] PRIMARY KEY CLUSTERED ([dw_ek_Faktura] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'IX_Fct_Faktura_ean')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Faktura_ean]
    ON [fct].[Faktura] ([dw_ek_EAN_regningsmodtager] ASC) ON [PRIMARY];
END
GO

-- fct.Vareudlevering
-- Source: ILS dbo.udlev — delivery lines from central warehouse to departments
-- Grain: one row per delivery line (identified by dbo.udlev.id)

-- Transaction dimension: dw_ek_Vareudlevering is the shared PK with
-- dim.Vareudlevering, resolved from map.Vareudlevering. No separate FK column
-- to dim.Vareudlevering — the shared PK itself is the relationship.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[Vareudlevering](
        [dw_ek_Vareudlevering] INT NOT NULL,
        [clean_code_Vareudlevering] NVARCHAR(500) NULL,
        [bk_prrowid] NVARCHAR(100) NULL,
        [dw_ek_ILSVare] INT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_ILSVare] DEFAULT -1,
        [dw_ek_UNSPSC] INT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_UNSPSC] DEFAULT -1,
        [dw_ek_Lager] BIGINT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_Lager] DEFAULT -1,
        [dw_ek_Rekvirent] INT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_Rekvirent] DEFAULT -1,
        [dw_ek_Leverandoervare] BIGINT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_Leverandoervare] DEFAULT -1,
        [dw_ek_Dato_vareudlevering] INT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_Dato_vareudlevering] DEFAULT -1,
        [dato_tid_vareudlevering] DATETIME2 NULL,
        [dw_ek_Dato_udloeb] INT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_Dato_udloeb] DEFAULT -1,
        [dato_tid_udloeb] DATETIME2 NULL,
        [dw_ek_Dato_postering] INT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_Dato_postering] DEFAULT -1,
        [dato_tid_postering] DATETIME2 NULL,
        [dw_ek_Dato_aflevering] INT NOT NULL CONSTRAINT [DF_Vareudlevering_dw_ek_Dato_aflevering] DEFAULT -1,
        [dato_tid_aflevering] DATETIME2 NULL,
        [vareudlevering_antal] DECIMAL(18,4) NULL,
        [vareudlevering_beloeb] DECIMAL(19,4) NULL,
        [indkoeb_beloeb] DECIMAL(19,4) NULL,
        [rabatbeloeb] DECIMAL(19,4) NULL,
        [lager_antal] DECIMAL(18,4) NULL,
        [dw_haendelsestidspunkt] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Fct_Vareudlevering] PRIMARY KEY CLUSTERED ([dw_ek_Vareudlevering] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_clean_code] ON [fct].[Vareudlevering]
    ([clean_code_Vareudlevering] ASC) WHERE [clean_code_Vareudlevering] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_ILSVare] ON [fct].[Vareudlevering]
    ([dw_ek_ILSVare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_UNSPSC] ON [fct].[Vareudlevering]
    ([dw_ek_UNSPSC] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_bk_prrowid] ON [fct].[Vareudlevering]
    ([bk_prrowid] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Lager] ON [fct].[Vareudlevering]
    ([dw_ek_Lager] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Rekvirent] ON [fct].[Vareudlevering]
    ([dw_ek_Rekvirent] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Leverandoervare] ON [fct].[Vareudlevering]
    ([dw_ek_Leverandoervare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Vareudlevering] ON [fct].[Vareudlevering]
    ([dw_ek_Dato_vareudlevering] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Udloeb] ON [fct].[Vareudlevering]
    ([dw_ek_Dato_udloeb] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Postering] ON [fct].[Vareudlevering]
    ([dw_ek_Dato_postering] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Aflevering] ON [fct].[Vareudlevering]
    ([dw_ek_Dato_aflevering] ASC) ON [PRIMARY];
END
GO

-- fct.Lagerbeholdning (Periodic Snapshot — Warehouse Stock)
-- Source: ILS dbo.lgrbeh — daily full-load inventory snapshot
-- Grain: one row per (snapshot date, sgh, lgr, vnr, lgrlok)
-- Load pattern: MERGE via stable DW_EK from map.Lagerbeholdning
-- Soft-delete: dw_is_deleted=1 for rows that disappear from a snapshot

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[Lagerbeholdning](
        [dw_ek_Lagerbeholdning] BIGINT NOT NULL,
        [dw_ek_Lager] BIGINT NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_ek_Lager] DEFAULT -1,
        [dw_ek_ILSVare] INT NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_ek_ILSVare] DEFAULT -1,
        [dw_ek_UNSPSC] INT NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_ek_UNSPSC] DEFAULT -1,
        [dw_ek_LagerbeholdningEgenskaber] INT NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_ek_LagerbeholdningEgenskaber] DEFAULT -1,
        [dw_ek_Dato_snapshot] INT NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_ek_Dato_snapshot] DEFAULT -1,
        [dw_ek_Dato_udloeb] INT NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_ek_Dato_udloeb] DEFAULT -1,
        [dato_tid_udloeb] DATETIME2 NULL,
        [dato_tid_snapshot] DATETIME2 NULL,
        [antal] DECIMAL(18,3) NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Fct_Lagerbeholdning] PRIMARY KEY CLUSTERED ([dw_ek_Lagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_Lager] ON [fct].[Lagerbeholdning]
    ([dw_ek_Lager] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_ILSVare] ON [fct].[Lagerbeholdning]
    ([dw_ek_ILSVare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_UNSPSC] ON [fct].[Lagerbeholdning]
    ([dw_ek_UNSPSC] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_Dato_Snapshot] ON [fct].[Lagerbeholdning]
    ([dw_ek_Dato_snapshot] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Lagerbeholdning_Dato_Udloeb] ON [fct].[Lagerbeholdning]
    ([dw_ek_Dato_udloeb] ASC) ON [PRIMARY];
END
GO

-- fct.AfdelingLagerbeholdning (Periodic Snapshot — Department Stock)
-- Source: ILS dbo.afdlgrbeh — daily full-load inventory snapshot
-- Grain: one row per (snapshot date, sgh, lgr, vnr, brik, rnr)
-- Load pattern: MERGE via stable DW_EK from map.AfdelingLagerbeholdning
-- Soft-delete: dw_is_deleted=1 for rows that disappear from a snapshot

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[AfdelingLagerbeholdning](
        [dw_ek_AfdelingLagerbeholdning] BIGINT NOT NULL,
        [dw_ek_Lager] BIGINT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_ek_Lager] DEFAULT -1,
        [dw_ek_Rekvirent] INT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_ek_Rekvirent] DEFAULT -1,
        [dw_ek_ILSVare] INT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_ek_ILSVare] DEFAULT -1,
        [dw_ek_UNSPSC] INT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_ek_UNSPSC] DEFAULT -1,
        [dw_ek_AfdelingLagerbeholdningEgenskaber] INT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_ek_AfdelingLagerbeholdningEgenskaber] DEFAULT -1,
        [dw_ek_Dato_snapshot] INT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_ek_Dato_snapshot] DEFAULT -1,
        [dw_ek_Dato_udloeb] INT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_ek_Dato_udloeb] DEFAULT -1,
        [dato_tid_udloeb] DATETIME2 NULL,
        [dato_tid_snapshot] DATETIME2 NULL,
        [antal] DECIMAL(18,3) NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Fct_AfdelingLagerbeholdning] PRIMARY KEY CLUSTERED ([dw_ek_AfdelingLagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Lager] ON [fct].[AfdelingLagerbeholdning]
    ([dw_ek_Lager] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Rekvirent] ON [fct].[AfdelingLagerbeholdning]
    ([dw_ek_Rekvirent] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_ILSVare] ON [fct].[AfdelingLagerbeholdning]
    ([dw_ek_ILSVare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_UNSPSC] ON [fct].[AfdelingLagerbeholdning]
    ([dw_ek_UNSPSC] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Dato_Snapshot] ON [fct].[AfdelingLagerbeholdning]
    ([dw_ek_Dato_snapshot] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_Dato_Udloeb] ON [fct].[AfdelingLagerbeholdning]
    ([dw_ek_Dato_udloeb] ASC) ON [PRIMARY];

    PRINT 'Created fct.AfdelingLagerbeholdning';
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
        [dw_ek_Finanskladdelinje] INT NOT NULL,
        [bk_ref_ledgertrans_id] NVARCHAR(100) NULL,
        [dw_ek_Finanskladde] INT NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_ek_Finanskladde] DEFAULT -1,
        [dw_ek_Kontoplan] INT NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_ek_Kontoplan] DEFAULT -1,
        [dw_ek_Okosted] INT NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_ek_Okosted] DEFAULT -1,
        [debet] DECIMAL(30,12) NULL,
        [kredit] DECIMAL(30,12) NULL,
        [dw_ek_Dato_linje] INT NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_ek_Dato_linje] DEFAULT -1,
        [dato_tid_linje] DATETIME2 NULL,
        [dw_ek_Dato_oprettet] INT NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_ek_Dato_oprettet] DEFAULT -1,
        [dato_tid_oprettet] DATETIME2 NULL,
        [dw_haendelsestidspunkt] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Fct_Finanskladdelinje] PRIMARY KEY CLUSTERED ([dw_ek_Finanskladdelinje] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = 'IX_Fct_Finanskladdelinje_Finanskladde')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Finanskladdelinje_Finanskladde]
    ON [fct].[Finanskladdelinje] ([dw_ek_Finanskladde] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = 'IX_Fct_Finanskladdelinje_Kontoplan')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Finanskladdelinje_Kontoplan]
    ON [fct].[Finanskladdelinje] ([dw_ek_Kontoplan] ASC) ON [PRIMARY];
END
GO

-- fct.Budget
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U'
) BEGIN
CREATE TABLE [fct].[Budget] (
    [dw_ek_Budget] INT NOT NULL,
    [bk_ref_ledgerbudget_id] NVARCHAR(100) NOT NULL,
    [dw_ek_BudgetEgenskaber] INT NOT NULL CONSTRAINT [DF_Budget_dw_ek_BudgetEgenskaber] DEFAULT -1,
    [dw_ek_Kontoplan] INT NOT NULL CONSTRAINT [DF_Budget_dw_ek_Kontoplan] DEFAULT -1,
    [dw_ek_Okosted] INT NOT NULL CONSTRAINT [DF_Budget_dw_ek_Okosted] DEFAULT -1,
    [dw_ek_Dato_budget] INT NOT NULL CONSTRAINT [DF_Budget_dw_ek_Dato_budget] DEFAULT -1,
    [dato_tid_budget] DATETIME2 NULL,
    [dw_ek_Dato_budget_adopted] INT NOT NULL CONSTRAINT [DF_Budget_dw_ek_Dato_budget_adopted] DEFAULT -1,
    [dato_tid_budget_adopted] DATETIME2 NULL,
    [dw_ek_Dato_budget_oprettet] INT NOT NULL CONSTRAINT [DF_Budget_dw_ek_Dato_budget_oprettet] DEFAULT -1,
    [dato_tid_budget_oprettet] DATETIME2 NULL,
    [dw_ek_Dato_budget_aendret] INT NOT NULL CONSTRAINT [DF_Budget_dw_ek_Dato_budget_aendret] DEFAULT -1,
    [dato_tid_budget_aendret] DATETIME2 NULL,
    [budgetbeloeb] DECIMAL(30, 12) NULL,
    [budgetbeloeb_valuta] DECIMAL(30, 12) NULL,
    [dw_haendelsestidspunkt] DATETIME2 NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Budget_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Budget_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Fct_Budget] PRIMARY KEY CLUSTERED ([dw_ek_Budget] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'IX_Fct_Budget_BudgetEgenskaber')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Budget_BudgetEgenskaber]
    ON [fct].[Budget] ([dw_ek_BudgetEgenskaber] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'IX_Fct_Budget_Kontoplan')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Budget_Kontoplan]
    ON [fct].[Budget] ([dw_ek_Kontoplan] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'IX_Fct_Budget_Dato_Budget')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Budget_Dato_Budget]
    ON [fct].[Budget] ([dw_ek_Dato_budget] ASC) ON [PRIMARY];
END
GO

-- fct.AftaleVarePris
-- Source: ILS dbo.laftd (backbone, spec 2026-08-31-prisloese-linjer-og-aftalelager)
--   LEFT JOIN dbo.laftdpris + dbo.lafth + dbo.laftdstam.
-- Grain: one row per price version (OR synthetic line-coverage row for price-less
--   lines) per agreement line per lvnr (bk_prrowid x clean_code_AftaleVarePris).
-- bk_prrowid carries a source prefix so the two row types never collide:
--   'LAFTDPRIS_<prrowid>' (real price version, dbo.laftdpris.prrowid) /
--   'LAFTD_<prrowid>' (synthetic row, dbo.laftd.prrowid) — both varchar(36).
-- har_prisversion: 1 = real price version, 0 = synthetic line-coverage row
--   (agreement line has items/alternates but zero laftdpris rows).
-- dw_valid_from = source start_/startklk (real) / linje_start (synthetic);
--   dw_valid_to = LEAD-chained per line (source slut only as last-version
--   fallback) / '9999-12-31' (synthetic, always dw_is_current = 1).
-- clean_code_AftaleVarePris format: AFTALE_{lnr}_{aftale}_VARE_{vnr}[_LEVVARE_{lvnr}] — basisform uden lvnr paa no-lvnr-raekken.

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[AftaleVarePris](
        [dw_sk_AftaleVarePris] BIGINT IDENTITY(1,1) NOT NULL,
        [dw_ek_AftaleVarePris] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_AftaleVarePris] DEFAULT -1,
        [dw_ek_Aftale] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_Aftale] DEFAULT -1,
        [dw_ek_Leverandoervare] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_Leverandoervare] DEFAULT -1,
        [dw_ek_ILSVare] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_ILSVare] DEFAULT -1,
        [dw_ek_Kreditor] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_Kreditor] DEFAULT -1,
        [clean_code_AftaleVarePris] NVARCHAR(500) NULL,
        [bk_prrowid] NVARCHAR(100) NOT NULL,
        [bk_leverandoernummer] NVARCHAR(100) NULL,
        [bk_aftale_nummer] NVARCHAR(100) NULL,
        [bk_ils_varenummer] NVARCHAR(100) NULL,
        [pris1] DECIMAL(18,4) NULL,
        [pris2] DECIMAL(18,4) NULL,
        [pris3] DECIMAL(18,4) NULL,
        [pris4] DECIMAL(18,4) NULL,
        [pakkestoerrelse1] DECIMAL(18,4) NULL,
        [pakkestoerrelse2] DECIMAL(18,4) NULL,
        [pakkestoerrelse3] DECIMAL(18,4) NULL,
        [pakkestoerrelse4] DECIMAL(18,4) NULL,
        [momskode] NVARCHAR(50) NULL,
        [kilder] NVARCHAR(100) NULL,
        [har_prisversion] BIT NOT NULL CONSTRAINT [DF_AftaleVarePris_har_prisversion] DEFAULT 1,
        [maerke] NVARCHAR(255) NULL,
        [linje_start] DATETIME2 NULL,
        [linje_slut] DATETIME2 NULL,
        [dw_valid_from] DATETIME2(0) NOT NULL,
        [dw_valid_to] DATETIME2(0) NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_valid_to] DEFAULT '9999-12-31',
        [dw_is_current] BIT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_is_current] DEFAULT 0,
        [source_aktiv] BIT NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Fct_AftaleVarePris] PRIMARY KEY CLUSTERED ([dw_sk_AftaleVarePris] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_grain] ON [fct].[AftaleVarePris]
    ([bk_prrowid] ASC, [clean_code_AftaleVarePris] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_AftaleVarePris] ON [fct].[AftaleVarePris]
    ([dw_ek_AftaleVarePris] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_Aftale] ON [fct].[AftaleVarePris]
    ([dw_ek_Aftale] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_Leverandoervare] ON [fct].[AftaleVarePris]
    ([dw_ek_Leverandoervare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_IlsVare] ON [fct].[AftaleVarePris]
    ([dw_ek_ILSVare] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_dw_is_current] ON [fct].[AftaleVarePris]
    ([dw_ek_AftaleVarePris] ASC) WHERE [dw_is_current] = 1 ON [PRIMARY];

    -- Understoetter pre-delete-sweepen (spec 2026-08-31): sweepen skopes paa
    -- linjenoeglen (bk_leverandoernummer, bk_aftale_nummer, bk_ils_varenummer),
    -- ikke bk_prrowid — uden dette indeks scanner sweepen hele tabellen hver
    -- koersel. (Review-ruling: spec's "No index changes" veg for dette.)
    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_linjenoegle] ON [fct].[AftaleVarePris]
    ([bk_leverandoernummer] ASC, [bk_aftale_nummer] ASC, [bk_ils_varenummer] ASC) ON [PRIMARY];
END
GO
