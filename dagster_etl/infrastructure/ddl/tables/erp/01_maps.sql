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
    [DW_EK_Kontoplan] INT IDENTITY(1, 1) NOT NULL,
    [CleanCode_Kontoplan] NVARCHAR(255) NOT NULL,
    [bk_FinanskontoId] BIGINT NOT NULL,
    [bk_Kontonummer] NVARCHAR(255) NOT NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Map_Kontoplan] PRIMARY KEY CLUSTERED ([CleanCode_Kontoplan] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'IX_Map_Kontoplan_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Kontoplan_DW_EK]
        ON [map].[Kontoplan] ([DW_EK_Kontoplan] ASC) INCLUDE ([CleanCode_Kontoplan]) ON [PRIMARY]
    ');
END
GO

-- map.Kreditor
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Kreditor](
        [DW_EK_Kreditor] INT IDENTITY(1,1) NOT NULL,
        [CleanCode_Kreditor] NVARCHAR(255) NOT NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Map_Kreditor] PRIMARY KEY CLUSTERED ([CleanCode_Kreditor] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'IX_Map_Kreditor_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Kreditor_DW_EK] ON [map].[Kreditor]
        ([DW_EK_Kreditor] ASC) INCLUDE ([CleanCode_Kreditor]) ON [PRIMARY]
    ');
END
GO

-- map.Vare
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[map].[Vare]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [map].[Vare] (
    [DW_EK_Vare] INT IDENTITY(1, 1) NOT NULL,
    [bk_Kreditor] NVARCHAR(255) NOT NULL,
    [bk_LeverandoerVarenummer] NVARCHAR(1023) NOT NULL,
    [CleanCode_Vare] NVARCHAR(550) NOT NULL,
    [vnr] NVARCHAR(100) NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Map_Vare] PRIMARY KEY CLUSTERED ([CleanCode_Vare] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'IX_Map_Vare_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Vare_DW_EK]
        ON [map].[Vare] ([DW_EK_Vare] ASC) INCLUDE (
            [CleanCode_Vare],
            [bk_Kreditor],
            [bk_LeverandoerVarenummer]
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
        [DW_EK_Faktura] INT IDENTITY(1, 1) NOT NULL,
        [bk_FakturaMetadata] NVARCHAR(100) NOT NULL,
        [bk_FakturaHeader] NVARCHAR(100) NOT NULL,
        [CleanCode_Faktura] NVARCHAR(255) NOT NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Map_Faktura] PRIMARY KEY CLUSTERED ([CleanCode_Faktura] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND name = 'IX_Map_Faktura_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Faktura_DW_EK]
        ON [map].[Faktura] ([DW_EK_Faktura] ASC) INCLUDE (
            [bk_FakturaMetadata],
            [bk_FakturaHeader],
            [CleanCode_Faktura]
        ) ON [PRIMARY]
    ');
END
GO

-- map.Aftale
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Aftale](
        [DW_EK_Aftale] INT IDENTITY(1,1) NOT NULL,
        [CleanCode_Aftale] NVARCHAR(255) NOT NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Map_Aftale] PRIMARY KEY CLUSTERED ([CleanCode_Aftale] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'IX_Map_Aftale_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Aftale_DW_EK] ON [map].[Aftale]
        ([DW_EK_Aftale] ASC) INCLUDE ([CleanCode_Aftale]) ON [PRIMARY]
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
    [DW_EK_Fakturalinje] INT IDENTITY(1, 1) NOT NULL,
    [bk_FakturaLines] NVARCHAR(100) NULL,
    [bk_FakturaHeader] NVARCHAR(100) NOT NULL,
    [bk_FakturaMetadata] NVARCHAR(100) NOT NULL,
    [CleanCode_Fakturalinje] NVARCHAR(255) NOT NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Map_Fakturalinje] PRIMARY KEY CLUSTERED ([CleanCode_Fakturalinje] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'IX_Map_Fakturalinje_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Fakturalinje_DW_EK]
        ON [map].[Fakturalinje] ([DW_EK_Fakturalinje] ASC) INCLUDE (
            [bk_FakturaLines],
            [bk_FakturaHeader],
            [bk_FakturaMetadata],
            [CleanCode_Fakturalinje]
        ) ON [PRIMARY]
    ');
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'IX_Map_Fakturalinje_CleanCode_FakturaLines')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND type = 'U')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'bk_FakturaLines')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Fakturalinje_CleanCode_FakturaLines]
        ON [map].[Fakturalinje] ([bk_FakturaLines] ASC)
        WHERE [bk_FakturaLines] IS NOT NULL ON [PRIMARY]
    ');
END
GO

-- map.Postering
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Postering](
        [DW_EK_Postering] INT IDENTITY(1,1) NOT NULL,
        [CleanCode_Postering] NVARCHAR(100) NOT NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Map_Postering] PRIMARY KEY CLUSTERED ([DW_EK_Postering] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'IX_Map_Postering_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Postering_DW_EK] ON [map].[Postering]
    ([DW_EK_Postering] ASC) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'IX_Map_Postering_CleanCode')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'CleanCode_Postering')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Postering_CleanCode]
        ON [map].[Postering] ([CleanCode_Postering] ASC) ON [PRIMARY]
    ');
END
GO

-- map.Vareudlevering
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Vareudlevering](
        [DW_EK_Vareudlevering] INT IDENTITY(1,1) NOT NULL,
        [CleanCode_Vareudlevering] NVARCHAR(500) NOT NULL,
        [bk_prrowid] NVARCHAR(100) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Map_Vareudlevering] PRIMARY KEY CLUSTERED ([CleanCode_Vareudlevering] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND name = 'IX_Map_Vareudlevering_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Vareudlevering_DW_EK] ON [map].[Vareudlevering]
        ([DW_EK_Vareudlevering] ASC) INCLUDE ([CleanCode_Vareudlevering]) ON [PRIMARY]
    ');
END
GO

-- map.Lager
-- CleanCode -> DW_EK resolution for Lager dimension
-- Supports the ILS Lager codesystem hierarchy (REGION_SYD -> SGH -> LGR)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Lager](
        [DW_EK_Lager] [int] IDENTITY(1,1) NOT NULL,
        [CleanCode_Lager] [varchar](100) UNIQUE NOT NULL,
        [bk_Code] [varchar](100) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Lager] PRIMARY KEY CLUSTERED
            ([DW_EK_Lager] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'IX_map_Lager_CleanCode')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND type = 'U')
BEGIN
    CREATE INDEX IX_map_Lager_CleanCode ON [map].[Lager]([CleanCode_Lager]);
END
GO

-- map.Rekvirent
-- CleanCode -> DW_EK resolution for Rekvirent dimension
-- Supports the ILS Rekvirent codesystem hierarchy (REGION_SYD -> SGH -> RGRP -> RTYP -> RNR)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Rekvirent](
        [DW_EK_Rekvirent] [int] IDENTITY(1,1) NOT NULL,
        [CleanCode_Rekvirent] [varchar](100) UNIQUE NOT NULL,
        [bk_Code] [varchar](100) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_Rekvirent] PRIMARY KEY CLUSTERED
            ([DW_EK_Rekvirent] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'IX_map_Rekvirent_CleanCode')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND type = 'U')
BEGIN
    CREATE INDEX IX_map_Rekvirent_CleanCode ON [map].[Rekvirent]([CleanCode_Rekvirent]);
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

-- map.Postering: add unique index on CleanCode_Postering if missing
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'IX_Map_Postering_CleanCode')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'CleanCode_Postering')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Postering_CleanCode]
        ON [map].[Postering] ([CleanCode_Postering] ASC) ON [PRIMARY]
    ');
    PRINT 'Created IX_Map_Postering_CleanCode unique index';
END
GO

-- map.LagerbeholdningEgenskaber (warehouse stock attribute combinations)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[LagerbeholdningEgenskaber](
        [DW_EK_LagerbeholdningEgenskaber] [int] IDENTITY(1,1) NOT NULL,
        [CleanCode_LagerbeholdningEgenskaber] [nvarchar](500) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_LagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([DW_EK_LagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_LagerbeholdningEgenskaber_CleanCode]
    ON [map].[LagerbeholdningEgenskaber] ([CleanCode_LagerbeholdningEgenskaber] ASC) ON [PRIMARY];

    PRINT 'Created map.LagerbeholdningEgenskaber';
END
GO

-- map.AfdelingLagerbeholdningEgenskaber (department stock attribute combinations)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[AfdelingLagerbeholdningEgenskaber](
        [DW_EK_AfdelingLagerbeholdningEgenskaber] [int] IDENTITY(1,1) NOT NULL,
        [CleanCode_AfdelingLagerbeholdningEgenskaber] [nvarchar](500) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_AfdelingLagerbeholdningEgenskaber] PRIMARY KEY CLUSTERED
            ([DW_EK_AfdelingLagerbeholdningEgenskaber] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_AfdelingLagerbeholdningEgenskaber_CleanCode]
    ON [map].[AfdelingLagerbeholdningEgenskaber] ([CleanCode_AfdelingLagerbeholdningEgenskaber] ASC) ON [PRIMARY];

    PRINT 'Created map.AfdelingLagerbeholdningEgenskaber';
END
GO

-- map.Lagerbeholdning (stable CleanCode -> DW_EK for periodic snapshot fact)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Lagerbeholdning](
        [DW_EK_Lagerbeholdning] BIGINT IDENTITY(1,1) NOT NULL,
        [CleanCode_Lagerbeholdning] NVARCHAR(500) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_Lagerbeholdning] PRIMARY KEY CLUSTERED
            ([DW_EK_Lagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Lagerbeholdning_CleanCode]
    ON [map].[Lagerbeholdning] ([CleanCode_Lagerbeholdning] ASC) ON [PRIMARY];

    PRINT 'Created map.Lagerbeholdning';
END
GO

-- map.AfdelingLagerbeholdning (stable CleanCode -> DW_EK for periodic snapshot fact)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[AfdelingLagerbeholdning](
        [DW_EK_AfdelingLagerbeholdning] BIGINT IDENTITY(1,1) NOT NULL,
        [CleanCode_AfdelingLagerbeholdning] NVARCHAR(500) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_AfdelingLagerbeholdning] PRIMARY KEY CLUSTERED
            ([DW_EK_AfdelingLagerbeholdning] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_AfdelingLagerbeholdning_CleanCode]
    ON [map].[AfdelingLagerbeholdning] ([CleanCode_AfdelingLagerbeholdning] ASC) ON [PRIMARY];

    PRINT 'Created map.AfdelingLagerbeholdning';
END
GO

-- map.IndkoebsanalyseLinje (stable CleanCode -> DW_EK for KMD SpendAnalysisLines fact)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[IndkoebsanalyseLinje]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[IndkoebsanalyseLinje](
        [DW_EK_IndkoebsanalyseLinje] INT IDENTITY(1,1) NOT NULL,
        [CleanCode_IndkoebsanalyseLinje] NVARCHAR(500) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) NOT NULL DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) NOT NULL DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_IndkoebsanalyseLinje] PRIMARY KEY CLUSTERED
            ([DW_EK_IndkoebsanalyseLinje] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_IndkoebsanalyseLinje_CleanCode]
        ON [map].[IndkoebsanalyseLinje] ([CleanCode_IndkoebsanalyseLinje] ASC) ON [PRIMARY];

    PRINT 'Created map.IndkoebsanalyseLinje';
END
GO

-- map.EAN
-- CleanCode -> DW_EK resolution for EAN dimension
-- Flat structure (no hierarchy) - maps EAN codes from MySupply invoice data
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[EAN]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[EAN](
        [DW_EK_EAN] [int] IDENTITY(1,1) NOT NULL,
        [CleanCode_EAN] [nvarchar](50) UNIQUE NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_EAN] PRIMARY KEY CLUSTERED
            ([DW_EK_EAN] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[EAN]') AND name = 'IX_map_EAN_CleanCode')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[EAN]') AND type = 'U')
BEGIN
    CREATE INDEX IX_map_EAN_CleanCode ON [map].[EAN]([CleanCode_EAN]);
END
GO

-- ============================================================================
-- MIGRATION: Trim whitespace from map.Kreditor CleanCode_Kreditor
-- Ensures the clustered PK index is seekable without LTRIM/RTRIM wrappers
-- in downstream DimUpd_kreditor JOINs.
-- ============================================================================
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND type = 'U')
BEGIN
    UPDATE [map].[Kreditor]
    SET CleanCode_Kreditor = LTRIM(RTRIM(CleanCode_Kreditor))
    WHERE CleanCode_Kreditor <> LTRIM(RTRIM(CleanCode_Kreditor));
    PRINT 'Trimmed whitespace from map.Kreditor.CleanCode_Kreditor';
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
    [DW_EK_BudgetEgenskaber]       INT IDENTITY(1, 1) NOT NULL,
    [CleanCode_BudgetEgenskaber]   NVARCHAR(500)      NOT NULL,
    [bk_ref_BudgetmodelId]         NVARCHAR(100)      NULL,
    [DW_Timestamp_Insert]          DATETIME2          NULL,
    [DW_Timestamp_Update]          DATETIME2          NULL,
    CONSTRAINT [PK_Map_BudgetEgenskaber] PRIMARY KEY CLUSTERED ([CleanCode_BudgetEgenskaber] ASC)
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
    ON [map].[BudgetEgenskaber] ([DW_EK_BudgetEgenskaber] ASC)
    INCLUDE ([CleanCode_BudgetEgenskaber])
    ON [PRIMARY];
END
GO

-- map.Budget (fact-level grain: one row per ref_LedgerbudgetId)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Budget]') AND type = 'U'
) BEGIN
CREATE TABLE [map].[Budget] (
    [DW_EK_Budget]            INT IDENTITY(1, 1) NOT NULL,
    [CleanCode_Budget]        NVARCHAR(255)      NOT NULL,
    [bk_ref_LedgerbudgetId]   NVARCHAR(100)      NOT NULL,
    [DW_Timestamp_Insert]     DATETIME2          NULL,
    [DW_Timestamp_Update]     DATETIME2          NULL,
    CONSTRAINT [PK_Map_Budget] PRIMARY KEY CLUSTERED ([CleanCode_Budget] ASC)
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
    ON [map].[Budget] ([DW_EK_Budget] ASC)
    INCLUDE ([CleanCode_Budget])
    ON [PRIMARY];
END
GO

-- map.IlsVare
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[IlsVare]') AND type = 'U'
) BEGIN
CREATE TABLE [map].[IlsVare] (
    [DW_EK_IlsVare]        INT IDENTITY(1, 1) NOT NULL,
    [CleanCode_IlsVare]    NVARCHAR(100)      NOT NULL,
    [DW_Timestamp_Insert]   DATETIME2          NULL,
    [DW_Timestamp_Update]   DATETIME2          NULL,
    CONSTRAINT [PK_Map_IlsVare] PRIMARY KEY CLUSTERED ([CleanCode_IlsVare] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[map].[IlsVare]') AND name = 'IX_Map_IlsVare_DW_EK'
) AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[IlsVare]') AND type = 'U'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_IlsVare_DW_EK]
    ON [map].[IlsVare] ([DW_EK_IlsVare] ASC)
    INCLUDE ([CleanCode_IlsVare])
    ON [PRIMARY];
END
GO

-- map.Finanskladde
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]') AND type = 'U')
CREATE TABLE [map].[Finanskladde] (
    [DW_EK_Finanskladde]      INT IDENTITY(1,1) NOT NULL,
    [CleanCode_Finanskladde]  NVARCHAR(100) NOT NULL,
    [DW_Timestamp_Insert]     DATETIME2 NOT NULL,
    [DW_Timestamp_Update]     DATETIME2 NOT NULL,
    CONSTRAINT [PK_Map_Finanskladde] PRIMARY KEY CLUSTERED ([CleanCode_Finanskladde] ASC)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Map_Finanskladde_DW_EK' AND object_id = OBJECT_ID('[map].[Finanskladde]'))
CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Finanskladde_DW_EK] ON [map].[Finanskladde]([DW_EK_Finanskladde] ASC);
GO

-- map.Finanskladdelinje
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND type = 'U')
CREATE TABLE [map].[Finanskladdelinje] (
    [DW_EK_Finanskladdelinje]      INT IDENTITY(1,1) NOT NULL,
    [CleanCode_Finanskladdelinje]  NVARCHAR(100) NOT NULL,
    [DW_Timestamp_Insert]          DATETIME2 NOT NULL,
    [DW_Timestamp_Update]          DATETIME2 NOT NULL,
    CONSTRAINT [PK_Map_Finanskladdelinje] PRIMARY KEY CLUSTERED ([CleanCode_Finanskladdelinje] ASC)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Map_Finanskladdelinje_DW_EK' AND object_id = OBJECT_ID('[map].[Finanskladdelinje]'))
CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Finanskladdelinje_DW_EK] ON [map].[Finanskladdelinje]([DW_EK_Finanskladdelinje] ASC);
-- map.UNSPSC
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[UNSPSC] (
        [DW_EK_UNSPSC]        INT IDENTITY(1,1) NOT NULL,
        [CleanCode_UNSPSC]    NVARCHAR(50) NOT NULL,
        [DW_Source_ILS]       BIT NOT NULL DEFAULT 0,
        [DW_Source_KMD]       BIT NOT NULL DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        [DW_Timestamp_Update] DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_UNSPSC] PRIMARY KEY CLUSTERED ([CleanCode_UNSPSC] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]') AND name = 'IX_Map_UNSPSC_DW_EK')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Map_UNSPSC_DW_EK]
        ON [map].[UNSPSC] ([DW_EK_UNSPSC]);
END
GO

-- map.Okosted
-- CleanCode_Okosted -> DW_EK resolution for Omkostningssted codes
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Okosted] (
        [DW_EK_Okosted] [int] IDENTITY(1, 1) NOT NULL,
        [CleanCode_Okosted] [varchar](100) UNIQUE NOT NULL,
        [bk_Code] [varchar](50) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_Okosted] PRIMARY KEY CLUSTERED ([DW_EK_Okosted] ASC)
    ) ON [PRIMARY];

    CREATE INDEX IX_map_Okosted_CleanCode_Okosted
        ON [map].[Okosted] ([CleanCode_Okosted]);
END
GO
