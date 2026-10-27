-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Map Tables
-- Source of truth for DW_EK assignment via IDENTITY
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/NON_FHIR_ELT_Implementeation_Guide.md § DDL Migration Convention
-- ============================================================================

-- map.Kontoplan
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[map].[Kontoplan]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [map].[Kontoplan] (
    [dw_ek_Kontoplan] INT IDENTITY(1, 1) NOT NULL,
    [clean_code_Kontoplan] NVARCHAR(255) NOT NULL,
    [bk_finanskonto_id] BIGINT NOT NULL,
    [bk_kontonummer] NVARCHAR(255) NOT NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Kontoplan_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Kontoplan_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_Kontoplan] PRIMARY KEY CLUSTERED ([clean_code_Kontoplan] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'IX_Map_Kontoplan_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Kontoplan_DW_EK]
        ON [map].[Kontoplan] ([dw_ek_Kontoplan] ASC) INCLUDE ([clean_code_Kontoplan]) ON [PRIMARY]
    ');
END
GO

-- map.Kreditor
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Kreditor](
        [dw_ek_Kreditor] INT IDENTITY(1,1) NOT NULL,
        [clean_code_Kreditor] NVARCHAR(255) NOT NULL,
        [dw_source_sydax] BIT NOT NULL CONSTRAINT [DF_Kreditor_dw_source_sydax] DEFAULT 0,
        [dw_source_ils] BIT NOT NULL CONSTRAINT [DF_Kreditor_dw_source_ils] DEFAULT 0,
        [dw_source_mysupply] BIT NOT NULL CONSTRAINT [DF_Kreditor_dw_source_mysupply] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Kreditor_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Kreditor_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Kreditor] PRIMARY KEY CLUSTERED ([clean_code_Kreditor] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'IX_Map_Kreditor_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Kreditor_DW_EK] ON [map].[Kreditor]
        ([dw_ek_Kreditor] ASC) INCLUDE ([clean_code_Kreditor]) ON [PRIMARY]
    ');
END
GO

-- map.Leverandoervare
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[map].[Leverandoervare]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [map].[Leverandoervare] (
    [dw_ek_Leverandoervare] INT IDENTITY(1, 1) NOT NULL,
    [bk_kreditor] NVARCHAR(255) NOT NULL,
    [bk_leverandoer_varenummer] NVARCHAR(1023) NOT NULL,
    [clean_code_Leverandoervare] NVARCHAR(550) NOT NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Leverandoervare_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Leverandoervare_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_Leverandoervare] PRIMARY KEY CLUSTERED ([clean_code_Leverandoervare] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Leverandoervare]') AND name = 'IX_Map_Leverandoervare_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Leverandoervare]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Leverandoervare_DW_EK]
        ON [map].[Leverandoervare] ([dw_ek_Leverandoervare] ASC) INCLUDE (
            [clean_code_Leverandoervare],
            [bk_kreditor],
            [bk_leverandoer_varenummer]
        ) ON [PRIMARY]
    ');
END
GO

-- map.Faktura (NEW) - grain: metadata.id + statistic_invoice_header.id
-- CleanCode format: CleanCode_{metadata_id}_{header_id}
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Faktura] (
        [dw_ek_Faktura] INT IDENTITY(1, 1) NOT NULL,
        [bk_faktura_metadata] NVARCHAR(100) NOT NULL,
        [bk_faktura_header] NVARCHAR(100) NOT NULL,
        [clean_code_Faktura] NVARCHAR(255) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Faktura] PRIMARY KEY CLUSTERED ([clean_code_Faktura] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND name = 'IX_Map_Faktura_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Faktura_DW_EK]
        ON [map].[Faktura] ([dw_ek_Faktura] ASC) INCLUDE (
            [bk_faktura_metadata],
            [bk_faktura_header],
            [clean_code_Faktura]
        ) ON [PRIMARY]
    ');
END
GO

-- map.Aftale
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Aftale](
        [dw_ek_Aftale] INT IDENTITY(1,1) NOT NULL,
        [clean_code_Aftale] NVARCHAR(255) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Aftale_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Aftale_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Aftale] PRIMARY KEY CLUSTERED ([clean_code_Aftale] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'IX_Map_Aftale_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Aftale_DW_EK] ON [map].[Aftale]
        ([dw_ek_Aftale] ASC) INCLUDE ([clean_code_Aftale]) ON [PRIMARY]
    ');
END
GO

-- map.ILSLeverandoer - grain: ILS supplier number (dbo.lev.lnr)
-- CleanCode format: ILSLEV_{lnr}
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[ILSLeverandoer]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[ILSLeverandoer](
        [dw_ek_ILSLeverandoer] INT IDENTITY(1,1) NOT NULL,
        [clean_code_ILSLeverandoer] NVARCHAR(255) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_ILSLeverandoer_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_ILSLeverandoer_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_ILSLeverandoer] PRIMARY KEY CLUSTERED ([clean_code_ILSLeverandoer] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[ILSLeverandoer]') AND name = 'IX_Map_ILSLeverandoer_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[ILSLeverandoer]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_ILSLeverandoer_DW_EK] ON [map].[ILSLeverandoer]
        ([dw_ek_ILSLeverandoer] ASC) INCLUDE ([clean_code_ILSLeverandoer]) ON [PRIMARY]
    ');
END
GO

-- map.AftaleVarePris - grain: aftalelinje (lnr, aftale, vnr)
-- CleanCode format: AFTALE_{lnr}_{aftale}_VARE_{vnr}
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[AftaleVarePris](
        [dw_ek_AftaleVarePris] INT IDENTITY(1,1) NOT NULL,
        [clean_code_AftaleVarePris] NVARCHAR(500) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_AftaleVarePris] PRIMARY KEY CLUSTERED ([clean_code_AftaleVarePris] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND name = 'IX_Map_AftaleVarePris_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_AftaleVarePris_DW_EK] ON [map].[AftaleVarePris]
        ([dw_ek_AftaleVarePris] ASC) INCLUDE ([clean_code_AftaleVarePris]) ON [PRIMARY]
    ');
END
GO

-- map.Fakturalinje
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[map].[Fakturalinje]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [map].[Fakturalinje] (
    [dw_ek_Fakturalinje] INT IDENTITY(1, 1) NOT NULL,
    [bk_faktura_lines] NVARCHAR(100) NULL,
    [bk_faktura_header] NVARCHAR(100) NOT NULL,
    [bk_faktura_metadata] NVARCHAR(100) NOT NULL,
    [clean_code_Fakturalinje] NVARCHAR(255) NOT NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_Fakturalinje] PRIMARY KEY CLUSTERED ([clean_code_Fakturalinje] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'IX_Map_Fakturalinje_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Fakturalinje_DW_EK]
        ON [map].[Fakturalinje] ([dw_ek_Fakturalinje] ASC) INCLUDE (
            [bk_faktura_lines],
            [bk_faktura_header],
            [bk_faktura_metadata],
            [clean_code_Fakturalinje]
        ) ON [PRIMARY]
    ');
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'IX_Map_Fakturalinje_clean_code_FakturaLines')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'bk_faktura_lines')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Fakturalinje_clean_code_FakturaLines]
        ON [map].[Fakturalinje] ([bk_faktura_lines] ASC)
        WHERE [bk_faktura_lines] IS NOT NULL ON [PRIMARY]
    ');
END
GO

-- map.Postering
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Postering](
        [dw_ek_Postering] INT IDENTITY(1,1) NOT NULL,
        [clean_code_Postering] NVARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Postering] PRIMARY KEY CLUSTERED ([dw_ek_Postering] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'IX_Map_Postering_clean_code')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'clean_code_Postering')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Postering_clean_code]
        ON [map].[Postering] ([clean_code_Postering] ASC) ON [PRIMARY]
    ');
END
GO

-- map.Vareudlevering
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Vareudlevering](
        [dw_ek_Vareudlevering] INT IDENTITY(1,1) NOT NULL,
        [clean_code_Vareudlevering] NVARCHAR(500) NOT NULL,
        [bk_prrowid] NVARCHAR(100) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Vareudlevering] PRIMARY KEY CLUSTERED ([clean_code_Vareudlevering] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND name = 'IX_Map_Vareudlevering_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Vareudlevering_DW_EK] ON [map].[Vareudlevering]
        ([dw_ek_Vareudlevering] ASC) INCLUDE ([clean_code_Vareudlevering]) ON [PRIMARY]
    ');
END
GO

-- map.Lager
-- CleanCode -> DW_EK resolution for Lager dimension
-- Supports the ILS Lager codesystem hierarchy (REGION_SYD -> sgh -> lgr)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Lager](
        [dw_ek_Lager] [int] IDENTITY(1,1) NOT NULL,
        [clean_code_Lager] [varchar](100) UNIQUE NOT NULL,
        [bk_code] [varchar](100) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Lager_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Lager_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Lager] PRIMARY KEY CLUSTERED
            ([dw_ek_Lager] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'IX_map_Lager_clean_code')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND type = 'U')
BEGIN
    CREATE INDEX IX_map_Lager_clean_code ON [map].[Lager]([clean_code_Lager]);
END
GO

-- map.Rekvirent
-- CleanCode -> DW_EK resolution for Rekvirent dimension
-- Supports the ILS Rekvirent codesystem hierarchy (REGION_SYD -> sgh -> rgrp -> rtyp -> rnr)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Rekvirent](
        [dw_ek_Rekvirent] [int] IDENTITY(1,1) NOT NULL,
        [clean_code_Rekvirent] [varchar](100) UNIQUE NOT NULL,
        [bk_code] [varchar](100) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Rekvirent_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Rekvirent_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Rekvirent] PRIMARY KEY CLUSTERED
            ([dw_ek_Rekvirent] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'IX_map_Rekvirent_clean_code')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND type = 'U')
BEGIN
    CREATE INDEX IX_map_Rekvirent_clean_code ON [map].[Rekvirent]([clean_code_Rekvirent]);
END
GO
-- ============================================================================
-- MIGRATION: Rename legacy CleanCode columns to canonical CleanCode_* naming
-- Safe: sp_rename preserves data; constraints follow the rename automatically.
-- Idempotent: each block checks for old name before renaming.
-- ============================================================================

-- NOTE: All column renames (legacy -> canonical CleanCode_* naming) are now handled
-- inline immediately after each table's CREATE TABLE block, before any index creation.
-- This ensures column names are canonical when SQL Server compiles index DDL batches.
-- The renames are idempotent: each checks for the old name before renaming.

-- map.Postering: add unique index on clean_code_Postering if missing
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'IX_Map_Postering_clean_code')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'clean_code_Postering')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Postering_clean_code]
        ON [map].[Postering] ([clean_code_Postering] ASC) ON [PRIMARY]
    ');
    PRINT 'Created IX_Map_Postering_clean_code unique index';
END
GO

-- map.LagerbeholdningEgenskaber (warehouse stock attribute combinations)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[LagerbeholdningEgenskaber](
        [dw_ek_LagerbeholdningEgenskaber] [int] IDENTITY(1,1) NOT NULL,
        [clean_code_LagerbeholdningEgenskaber] [nvarchar](500) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_LagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([dw_ek_LagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_LagerbeholdningEgenskaber_clean_code]
    ON [map].[LagerbeholdningEgenskaber] ([clean_code_LagerbeholdningEgenskaber] ASC) ON [PRIMARY];

    PRINT 'Created map.LagerbeholdningEgenskaber';
END
GO

-- map.AfdelingLagerbeholdningEgenskaber (department stock attribute combinations)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[AfdelingLagerbeholdningEgenskaber](
        [dw_ek_AfdelingLagerbeholdningEgenskaber] [int] IDENTITY(1,1) NOT NULL,
        [clean_code_AfdelingLagerbeholdningEgenskaber] [nvarchar](500) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_AfdelingLagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([dw_ek_AfdelingLagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_AfdelingLagerbeholdningEgenskaber_clean_code]
    ON [map].[AfdelingLagerbeholdningEgenskaber] ([clean_code_AfdelingLagerbeholdningEgenskaber] ASC) ON [PRIMARY];

    PRINT 'Created map.AfdelingLagerbeholdningEgenskaber';
END
GO

-- map.Lagerbeholdning (stable CleanCode -> DW_EK for periodic snapshot fact)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Lagerbeholdning](
        [dw_ek_Lagerbeholdning] BIGINT IDENTITY(1,1) NOT NULL,
        [clean_code_Lagerbeholdning] NVARCHAR(500) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_Lagerbeholdning] PRIMARY KEY CLUSTERED
            ([dw_ek_Lagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Lagerbeholdning_clean_code]
    ON [map].[Lagerbeholdning] ([clean_code_Lagerbeholdning] ASC) ON [PRIMARY];

    PRINT 'Created map.Lagerbeholdning';
END
GO

-- map.AfdelingLagerbeholdning (stable CleanCode -> DW_EK for periodic snapshot fact)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[AfdelingLagerbeholdning](
        [dw_ek_AfdelingLagerbeholdning] BIGINT IDENTITY(1,1) NOT NULL,
        [clean_code_AfdelingLagerbeholdning] NVARCHAR(500) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_AfdelingLagerbeholdning] PRIMARY KEY CLUSTERED
            ([dw_ek_AfdelingLagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_AfdelingLagerbeholdning_clean_code]
    ON [map].[AfdelingLagerbeholdning] ([clean_code_AfdelingLagerbeholdning] ASC) ON [PRIMARY];

    PRINT 'Created map.AfdelingLagerbeholdning';
END
GO

-- map.ean
-- CleanCode -> DW_EK resolution for ean dimension
-- Flat structure (no hierarchy) - maps ean codes from MySupply invoice data
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[ean]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[ean](
        [dw_ek_EAN] [int] IDENTITY(1,1) NOT NULL,
        [clean_code_EAN] [nvarchar](50) UNIQUE NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_ean_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_ean_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_ean] PRIMARY KEY CLUSTERED
            ([dw_ek_EAN] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[ean]') AND name = 'IX_map_ean_clean_code')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[ean]') AND type = 'U')
BEGIN
    CREATE INDEX IX_map_ean_clean_code ON [map].[ean]([clean_code_EAN]);
END
GO

-- ============================================================================
-- MIGRATION: Trim whitespace from map.Kreditor clean_code_Kreditor
-- Ensures the clustered PK index is seekable without LTRIM/RTRIM wrappers
-- in downstream DimUpd_kreditor JOINs.
-- ============================================================================
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND type = 'U')
BEGIN
    UPDATE [map].[Kreditor]
    SET clean_code_Kreditor = LTRIM(RTRIM(clean_code_Kreditor))
    WHERE clean_code_Kreditor <> LTRIM(RTRIM(clean_code_Kreditor));
    PRINT 'Trimmed whitespace from map.Kreditor.clean_code_Kreditor';
END
GO

-- ============================================================================
-- NOTE: DW_EK IDENTITY columns in map tables were changed from BIGINT to INT
-- in the CREATE TABLE statements above. MSSQL does not allow ALTER COLUMN on
-- IDENTITY columns, so existing databases retain BIGINT IDENTITY until a full
-- table recreation. This is harmless: INT values fit in BIGINT, and the ERP
-- domain will never exceed INT range (2.1 billion).
-- ============================================================================

-- map.BudgetEgenskaber (composite grain: one row per distinct combination of descriptive columns)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND type = 'U'
) BEGIN
CREATE TABLE [map].[BudgetEgenskaber] (
    [dw_ek_BudgetEgenskaber]       INT IDENTITY(1, 1) NOT NULL,
    [clean_code_BudgetEgenskaber]   NVARCHAR(500)      NOT NULL,
    [bk_ref_budgetmodel_id]         NVARCHAR(100)      NULL,
    [dw_timestamp_insert]          DATETIME2          NOT NULL CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]          DATETIME2          NOT NULL CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_BudgetEgenskaber] PRIMARY KEY CLUSTERED ([clean_code_BudgetEgenskaber] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND name = 'IX_Map_BudgetEgenskaber_DW_EK'
) AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND type = 'U'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_BudgetEgenskaber_DW_EK]
    ON [map].[BudgetEgenskaber] ([dw_ek_BudgetEgenskaber] ASC)
    INCLUDE ([clean_code_BudgetEgenskaber])
    ON [PRIMARY];
END
GO

-- map.Budget (fact-level grain: one row per ref_LedgerbudgetId)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Budget]') AND type = 'U'
) BEGIN
CREATE TABLE [map].[Budget] (
    [dw_ek_Budget]            INT IDENTITY(1, 1) NOT NULL,
    [clean_code_Budget]        NVARCHAR(255)      NOT NULL,
    [bk_ref_ledgerbudget_id]   NVARCHAR(100)      NOT NULL,
    [dw_timestamp_insert]     DATETIME2          NOT NULL CONSTRAINT [DF_Budget_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]     DATETIME2          NOT NULL CONSTRAINT [DF_Budget_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_Budget] PRIMARY KEY CLUSTERED ([clean_code_Budget] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[map].[Budget]') AND name = 'IX_Map_Budget_DW_EK'
) AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Budget]') AND type = 'U'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Budget_DW_EK]
    ON [map].[Budget] ([dw_ek_Budget] ASC)
    INCLUDE ([clean_code_Budget])
    ON [PRIMARY];
END
GO

-- map.ILSVare
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[ILSVare]') AND type = 'U'
) BEGIN
CREATE TABLE [map].[ILSVare] (
    [dw_ek_ILSVare]        INT IDENTITY(1, 1) NOT NULL,
    [clean_code_ILSVare]    NVARCHAR(100)      NOT NULL,
    [dw_timestamp_insert]   DATETIME2          NOT NULL CONSTRAINT [DF_ILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]   DATETIME2          NOT NULL CONSTRAINT [DF_ILSVare_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_ILSVare] PRIMARY KEY CLUSTERED ([clean_code_ILSVare] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[map].[ILSVare]') AND name = 'IX_Map_ILSVare_DW_EK'
) AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[ILSVare]') AND type = 'U'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_ILSVare_DW_EK]
    ON [map].[ILSVare] ([dw_ek_ILSVare] ASC)
    INCLUDE ([clean_code_ILSVare])
    ON [PRIMARY];
END
GO

-- map.ILSBruger
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[ILSBruger]') AND type = 'U'
) BEGIN
CREATE TABLE [map].[ILSBruger] (
    [dw_ek_ILSBruger]       INT IDENTITY(1, 1) NOT NULL,
    [clean_code_ILSBruger]  NVARCHAR(100)      NOT NULL,
    [dw_timestamp_insert]   DATETIME2          NOT NULL CONSTRAINT [DF_ILSBruger_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]   DATETIME2          NOT NULL CONSTRAINT [DF_ILSBruger_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_ILSBruger] PRIMARY KEY CLUSTERED ([clean_code_ILSBruger] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[map].[ILSBruger]') AND name = 'IX_Map_ILSBruger_DW_EK'
) AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[ILSBruger]') AND type = 'U'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_ILSBruger_DW_EK]
    ON [map].[ILSBruger] ([dw_ek_ILSBruger] ASC)
    INCLUDE ([clean_code_ILSBruger])
    ON [PRIMARY];
END
GO

-- map.Finanskladde
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]') AND type = 'U')
CREATE TABLE [map].[Finanskladde] (
    [dw_ek_Finanskladde]      INT IDENTITY(1,1) NOT NULL,
    [clean_code_Finanskladde]  NVARCHAR(100) NOT NULL,
    [dw_timestamp_insert]     DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladde_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]     DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladde_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_Finanskladde] PRIMARY KEY CLUSTERED ([clean_code_Finanskladde] ASC)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Map_Finanskladde_DW_EK' AND object_id = OBJECT_ID('[map].[Finanskladde]'))
CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Finanskladde_DW_EK] ON [map].[Finanskladde]([dw_ek_Finanskladde] ASC);
GO

-- map.Finanskladdelinje
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND type = 'U')
CREATE TABLE [map].[Finanskladdelinje] (
    [dw_ek_Finanskladdelinje]      INT IDENTITY(1,1) NOT NULL,
    [clean_code_Finanskladdelinje]  NVARCHAR(100) NOT NULL,
    [dw_timestamp_insert]          DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]          DATETIME2 NOT NULL CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Map_Finanskladdelinje] PRIMARY KEY CLUSTERED ([clean_code_Finanskladdelinje] ASC)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Map_Finanskladdelinje_DW_EK' AND object_id = OBJECT_ID('[map].[Finanskladdelinje]'))
CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Finanskladdelinje_DW_EK] ON [map].[Finanskladdelinje]([dw_ek_Finanskladdelinje] ASC);
-- map.UNSPSC
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[UNSPSC] (
        [dw_ek_UNSPSC]        INT IDENTITY(1,1) NOT NULL,
        [clean_code_UNSPSC]    NVARCHAR(50) NOT NULL,
        [dw_source_ils]       BIT NOT NULL CONSTRAINT [DF_UNSPSC_dw_source_ils] DEFAULT 0,
        [dw_source_kmd]       BIT NOT NULL CONSTRAINT [DF_UNSPSC_dw_source_kmd] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_UNSPSC_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_UNSPSC_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_UNSPSC] PRIMARY KEY CLUSTERED ([clean_code_UNSPSC] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]') AND name = 'IX_Map_UNSPSC_DW_EK')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Map_UNSPSC_DW_EK]
        ON [map].[UNSPSC] ([dw_ek_UNSPSC]);
END
GO

-- map.Okosted
-- clean_code_Okosted -> DW_EK resolution for Omkostningssted codes
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Okosted] (
        [dw_ek_Okosted] [int] IDENTITY(1, 1) NOT NULL,
        [clean_code_Okosted] [varchar](120) UNIQUE NOT NULL,
        [bk_code] [varchar](120) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Okosted_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Okosted_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_Okosted] PRIMARY KEY CLUSTERED ([dw_ek_Okosted] ASC)
    ) ON [PRIMARY];

    CREATE INDEX IX_map_Okosted_clean_code_Okosted
        ON [map].[Okosted] ([clean_code_Okosted]);
END
GO

-- map.SHAK
-- clean_code_SHAK -> DW_EK resolution for SHAK (Sygehus-Afdelingsklassifikation) codes
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[SHAK]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[SHAK] (
        [dw_ek_SHAK] [int] IDENTITY(1, 1) NOT NULL,
        [clean_code_SHAK] [varchar](100) UNIQUE NOT NULL,
        [bk_code] [varchar](50) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAK_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAK_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_SHAK] PRIMARY KEY CLUSTERED ([dw_ek_SHAK] ASC)
    ) ON [PRIMARY];

    CREATE INDEX IX_map_SHAK_clean_code_SHAK
        ON [map].[SHAK] ([clean_code_SHAK]);
END
GO

-- map.SHAKAlike
-- clean_code_SHAKAlike -> DW_EK resolution for EPJI SHAK-a-like codes
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[SHAKAlike]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[SHAKAlike] (
        [dw_ek_SHAKAlike] [int] IDENTITY(1, 1) NOT NULL,
        [clean_code_SHAKAlike] [varchar](100) UNIQUE NOT NULL,
        [bk_code] [varchar](50) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAKAlike_map_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAKAlike_map_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_SHAKAlike] PRIMARY KEY CLUSTERED ([dw_ek_SHAKAlike] ASC)
    ) ON [PRIMARY];

    CREATE INDEX IX_map_SHAKAlike_clean_code_SHAKAlike
        ON [map].[SHAKAlike] ([clean_code_SHAKAlike]);
END
GO
