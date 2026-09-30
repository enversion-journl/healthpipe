-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Map Table Migrations
--
-- All map.* schema migrations in dependency order.
-- Every statement is idempotent. Safe to delete once applied to all environments.
-- ============================================================================

-- ============================================================================
-- 1. Rename legacy CleanCode columns to canonical CleanCode_* naming
-- ============================================================================

-- ---- map.Kontoplan ----

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'IX_Map_Kontoplan_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'Kontoplan_CleanCode')
BEGIN
    DROP INDEX [IX_Map_Kontoplan_DW_EK] ON [map].[Kontoplan];
    PRINT 'Dropped IX_Map_Kontoplan_DW_EK (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'PK_Map_Kontoplan')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'Kontoplan_CleanCode')
BEGIN
    ALTER TABLE [map].[Kontoplan] DROP CONSTRAINT [PK_Map_Kontoplan];
    PRINT 'Dropped PK_Map_Kontoplan (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'Kontoplan_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Kontoplan].[Kontoplan_CleanCode]', 'CleanCode_Kontoplan', 'COLUMN';
    PRINT 'Renamed map.Kontoplan.Kontoplan_CleanCode -> CleanCode_Kontoplan';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'PK_Map_Kontoplan')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND type = 'U')
BEGIN
    ALTER TABLE [map].[Kontoplan] ADD CONSTRAINT [PK_Map_Kontoplan] PRIMARY KEY CLUSTERED ([CleanCode_Kontoplan] ASC);
    PRINT 'Recreated PK_Map_Kontoplan on CleanCode_Kontoplan';
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

-- ---- map.Kreditor ----

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'IX_Map_Kreditor_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'CVR_CleanCode')
BEGIN
    DROP INDEX [IX_Map_Kreditor_DW_EK] ON [map].[Kreditor];
    PRINT 'Dropped IX_Map_Kreditor_DW_EK (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'PK_Map_Kreditor')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'CVR_CleanCode')
BEGIN
    ALTER TABLE [map].[Kreditor] DROP CONSTRAINT [PK_Map_Kreditor];
    PRINT 'Dropped PK_Map_Kreditor (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'CVR_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Kreditor].[CVR_CleanCode]', 'CleanCode_Kreditor', 'COLUMN';
    PRINT 'Renamed map.Kreditor.CVR_CleanCode -> CleanCode_Kreditor';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'PK_Map_Kreditor')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND type = 'U')
BEGIN
    ALTER TABLE [map].[Kreditor] ADD CONSTRAINT [PK_Map_Kreditor] PRIMARY KEY CLUSTERED ([CleanCode_Kreditor] ASC);
    PRINT 'Recreated PK_Map_Kreditor on CleanCode_Kreditor';
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

-- ---- map.Vare ----

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'IX_Map_Vare_DW_EK')
BEGIN
    DROP INDEX [IX_Map_Vare_DW_EK] ON [map].[Vare];
    PRINT 'Dropped IX_Map_Vare_DW_EK (will be recreated after migration)';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'PK_Map_Vare')
   AND (
       EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'CVR_CleanCode')
       OR EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'CleanCode_Kreditor')
       OR EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'CleanCode_LeverandoerVarenummer')
   )
BEGIN
    ALTER TABLE [map].[Vare] DROP CONSTRAINT [PK_Map_Vare];
    PRINT 'Dropped PK_Map_Vare (will be recreated after migration)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'CVR_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Vare].[CVR_CleanCode]', 'bk_Kreditor', 'COLUMN';
    PRINT 'Renamed map.Vare.CVR_CleanCode -> bk_Kreditor';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'Vare_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Vare].[Vare_CleanCode]', 'CleanCode_Vare', 'COLUMN';
    PRINT 'Renamed map.Vare.Vare_CleanCode -> CleanCode_Vare';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'Leverandoer_Varenummer_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Vare].[Leverandoer_Varenummer_CleanCode]', 'bk_LeverandoerVarenummer', 'COLUMN';
    PRINT 'Renamed map.Vare.Leverandoer_Varenummer_CleanCode -> bk_LeverandoerVarenummer';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'CleanCode_Kreditor')
BEGIN
    EXEC sp_rename '[map].[Vare].[CleanCode_Kreditor]', 'bk_Kreditor', 'COLUMN';
    PRINT 'Renamed map.Vare.CleanCode_Kreditor -> bk_Kreditor';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'CleanCode_LeverandoerVarenummer')
BEGIN
    EXEC sp_rename '[map].[Vare].[CleanCode_LeverandoerVarenummer]', 'bk_LeverandoerVarenummer', 'COLUMN';
    PRINT 'Renamed map.Vare.CleanCode_LeverandoerVarenummer -> bk_LeverandoerVarenummer';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'PK_Map_Vare')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND type = 'U')
BEGIN
    ALTER TABLE [map].[Vare] ADD CONSTRAINT [PK_Map_Vare] PRIMARY KEY CLUSTERED ([CleanCode_Vare] ASC);
    PRINT 'Recreated PK_Map_Vare on CleanCode_Vare';
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

-- ---- map.Aftale ----

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'IX_Map_Aftale_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'Aftale_CleanCode')
BEGIN
    DROP INDEX [IX_Map_Aftale_DW_EK] ON [map].[Aftale];
    PRINT 'Dropped IX_Map_Aftale_DW_EK (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'PK_Map_Aftale')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'Aftale_CleanCode')
BEGIN
    ALTER TABLE [map].[Aftale] DROP CONSTRAINT [PK_Map_Aftale];
    PRINT 'Dropped PK_Map_Aftale (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'Aftale_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Aftale].[Aftale_CleanCode]', 'CleanCode_Aftale', 'COLUMN';
    PRINT 'Renamed map.Aftale.Aftale_CleanCode -> CleanCode_Aftale';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Aftale]') AND name = 'PK_Map_Aftale')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND type = 'U')
BEGIN
    ALTER TABLE [map].[Aftale] ADD CONSTRAINT [PK_Map_Aftale] PRIMARY KEY CLUSTERED ([CleanCode_Aftale] ASC);
    PRINT 'Recreated PK_Map_Aftale on CleanCode_Aftale';
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

-- ---- map.Postering ----

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'IX_Map_Postering_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'Postering_CleanCode')
BEGIN
    DROP INDEX [IX_Map_Postering_DW_EK] ON [map].[Postering];
    PRINT 'Dropped IX_Map_Postering_DW_EK (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = 'Postering_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Postering].[Postering_CleanCode]', 'CleanCode_Postering', 'COLUMN';
    PRINT 'Renamed map.Postering.Postering_CleanCode -> CleanCode_Postering';
END
GO

-- ---- map.Udlevering ----

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'IX_Map_Udlevering_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'Udlevering_CleanCode')
BEGIN
    DROP INDEX [IX_Map_Udlevering_DW_EK] ON [map].[Udlevering];
    PRINT 'Dropped IX_Map_Udlevering_DW_EK (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'PK_Map_Udlevering')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'Udlevering_CleanCode')
BEGIN
    ALTER TABLE [map].[Udlevering] DROP CONSTRAINT [PK_Map_Udlevering];
    PRINT 'Dropped PK_Map_Udlevering (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'Udlevering_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Udlevering].[Udlevering_CleanCode]', 'CleanCode_Udlevering', 'COLUMN';
    PRINT 'Renamed map.Udlevering.Udlevering_CleanCode -> CleanCode_Udlevering';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'PK_Map_Udlevering')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND type = 'U')
BEGIN
    ALTER TABLE [map].[Udlevering] ADD CONSTRAINT [PK_Map_Udlevering] PRIMARY KEY CLUSTERED ([CleanCode_Udlevering] ASC);
    PRINT 'Recreated PK_Map_Udlevering on CleanCode_Udlevering';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'IX_Map_Udlevering_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Udlevering_DW_EK] ON [map].[Udlevering]
        ([DW_EK_Udlevering] ASC) INCLUDE ([CleanCode_Udlevering]) ON [PRIMARY]
    ');
END
GO

-- ============================================================================
-- 2. Rename map.Faktura -> map.FakturaEgenskaber (via map.Faktura_egenskab)
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Faktura_egenskab]') AND type = 'U')
BEGIN
    EXEC sp_rename '[map].[Faktura]', 'FakturaEgenskaber';
    PRINT 'Renamed map.Faktura -> map.FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Faktura_egenskab]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND type = 'U')
BEGIN
    EXEC sp_rename '[map].[Faktura_egenskab]', 'FakturaEgenskaber';
    PRINT 'Renamed map.Faktura_egenskab -> map.FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'PK_Map_Faktura')
BEGIN
    ALTER TABLE [map].[FakturaEgenskaber] DROP CONSTRAINT [PK_Map_Faktura];
    ALTER TABLE [map].[FakturaEgenskaber] ADD CONSTRAINT [PK_Map_FakturaEgenskaber] PRIMARY KEY CLUSTERED ([CleanCode_Faktura] ASC);
    PRINT 'Renamed PK_Map_Faktura -> PK_Map_FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'PK_Map_Faktura_egenskab')
BEGIN
    ALTER TABLE [map].[FakturaEgenskaber] DROP CONSTRAINT [PK_Map_Faktura_egenskab];
    ALTER TABLE [map].[FakturaEgenskaber] ADD CONSTRAINT [PK_Map_FakturaEgenskaber] PRIMARY KEY CLUSTERED ([CleanCode_Faktura] ASC);
    PRINT 'Renamed PK_Map_Faktura_egenskab -> PK_Map_FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'IX_Map_Faktura_DW_EK')
BEGIN
    DROP INDEX [IX_Map_Faktura_DW_EK] ON [map].[FakturaEgenskaber];
    PRINT 'Dropped IX_Map_Faktura_DW_EK from FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'IX_Map_Faktura_egenskab_DW_EK')
BEGIN
    DROP INDEX [IX_Map_Faktura_egenskab_DW_EK] ON [map].[FakturaEgenskaber];
    PRINT 'Dropped IX_Map_Faktura_egenskab_DW_EK from FakturaEgenskaber';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'IX_Map_FakturaEgenskaber_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_FakturaEgenskaber_DW_EK]
        ON [map].[FakturaEgenskaber] ([DW_EK_Faktura] ASC) INCLUDE (
            [bk_Faktura],
            [bk_Kreditor]
        ) ON [PRIMARY]
    ');
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'FakturaNr_CleanCode')
BEGIN
    EXEC sp_rename '[map].[FakturaEgenskaber].[FakturaNr_CleanCode]', 'bk_Faktura', 'COLUMN';
    PRINT 'Renamed map.FakturaEgenskaber.FakturaNr_CleanCode -> bk_Faktura';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'CVR_CleanCode')
BEGIN
    EXEC sp_rename '[map].[FakturaEgenskaber].[CVR_CleanCode]', 'bk_Kreditor', 'COLUMN';
    PRINT 'Renamed map.FakturaEgenskaber.CVR_CleanCode -> bk_Kreditor';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'Faktura_CVR_CleanCode')
BEGIN
    EXEC sp_rename '[map].[FakturaEgenskaber].[Faktura_CVR_CleanCode]', 'CleanCode_Faktura', 'COLUMN';
    PRINT 'Renamed map.FakturaEgenskaber.Faktura_CVR_CleanCode -> CleanCode_Faktura';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'CleanCode_Faktura_Kreditor')
BEGIN
    EXEC sp_rename '[map].[FakturaEgenskaber].[CleanCode_Faktura_Kreditor]', 'CleanCode_Faktura', 'COLUMN';
    PRINT 'Renamed map.FakturaEgenskaber.CleanCode_Faktura_Kreditor -> CleanCode_Faktura';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'CleanCode_Kreditor_Faktura')
BEGIN
    EXEC sp_rename '[map].[FakturaEgenskaber].[CleanCode_Kreditor_Faktura]', 'CleanCode_Faktura', 'COLUMN';
    PRINT 'Renamed map.FakturaEgenskaber.CleanCode_Kreditor_Faktura -> CleanCode_Faktura';
END
GO

-- ============================================================================
-- 3. Widen map.Vare.bk_LeverandoerVarenummer to NVARCHAR(1023)
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND type = 'U')
BEGIN
    IF EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'map' AND TABLE_NAME = 'Vare'
          AND COLUMN_NAME = 'Leverandoer_Varenummer_CleanCode'
          AND CHARACTER_MAXIMUM_LENGTH < 1023
    )
    BEGIN
        ALTER TABLE [map].[Vare] ALTER COLUMN [Leverandoer_Varenummer_CleanCode] NVARCHAR(1023) NOT NULL;
        PRINT 'Widened map.Vare.Leverandoer_Varenummer_CleanCode to NVARCHAR(1023)';
    END

    IF EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'map' AND TABLE_NAME = 'Vare'
          AND COLUMN_NAME = 'bk_LeverandoerVarenummer'
          AND CHARACTER_MAXIMUM_LENGTH < 1023
    )
    BEGIN
        ALTER TABLE [map].[Vare] ALTER COLUMN [bk_LeverandoerVarenummer] NVARCHAR(1023) NOT NULL;
        PRINT 'Widened map.Vare.bk_LeverandoerVarenummer to NVARCHAR(1023)';
    END
END
GO

-- ============================================================================
-- 4. Add vnr column to map.Vare
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vare]') AND name = 'vnr')
BEGIN
    ALTER TABLE [map].[Vare] ADD [vnr] NVARCHAR(100) NULL;
    PRINT 'Added vnr column to map.Vare';
END
GO

-- ============================================================================
-- 5. Create NEW map.Faktura table (metadata+header grain) and PK rename safety
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'PK_Map_Faktura')
BEGIN
    ALTER TABLE [map].[FakturaEgenskaber] DROP CONSTRAINT [PK_Map_Faktura];
    ALTER TABLE [map].[FakturaEgenskaber] ADD CONSTRAINT [PK_Map_FakturaEgenskaber] PRIMARY KEY CLUSTERED ([CleanCode_Faktura] ASC);
    PRINT 'Freed PK name PK_Map_Faktura from FakturaEgenskaber';
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND type = 'U')
   AND EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND type = 'U')
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
    PRINT 'Created NEW map.Faktura (metadata+header grain)';
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

-- ============================================================================
-- 6. Drop FinansposteringId from map.FakturaEgenskaber
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'IX_Map_Faktura_FinansposteringId')
BEGIN
    DROP INDEX [IX_Map_Faktura_FinansposteringId] ON [map].[FakturaEgenskaber];
    PRINT 'Dropped IX_Map_Faktura_FinansposteringId from FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]') AND name = 'FinansposteringId')
BEGIN
    ALTER TABLE [map].[FakturaEgenskaber] DROP COLUMN [FinansposteringId];
    PRINT 'Dropped FakturaEgenskaber.FinansposteringId column';
END
GO

-- ============================================================================
-- 7. Drop obsolete map.Vareaftaler
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vareaftaler]') AND type = 'U')
BEGIN
    DROP TABLE [map].[Vareaftaler];
    PRINT 'Dropped obsolete map.Vareaftaler table';
END
GO

-- ============================================================================
-- 8. Add unique index on CleanCode_Postering
-- ============================================================================

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

-- ============================================================================
-- 9. Rename audit columns in map.Lager, map.Rekvirent, and map.EAN
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename 'map.Lager.DW_Insert_DateTime', 'DW_Timestamp_Insert', 'COLUMN';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename 'map.Lager.DW_Update_DateTime', 'DW_Timestamp_Update', 'COLUMN';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename 'map.Rekvirent.DW_Insert_DateTime', 'DW_Timestamp_Insert', 'COLUMN';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename 'map.Rekvirent.DW_Update_DateTime', 'DW_Timestamp_Update', 'COLUMN';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[EAN]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename 'map.EAN.DW_Insert_DateTime', 'DW_Timestamp_Insert', 'COLUMN';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[EAN]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename 'map.EAN.DW_Update_DateTime', 'DW_Timestamp_Update', 'COLUMN';
END
GO

-- ============================================================================
-- 10. Rename legacy Fakturalinje columns to canonical naming
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Map_Fakturalinje_FakturaLines_CleanCode' AND object_id = OBJECT_ID(N'[map].[Fakturalinje]'))
BEGIN
    DROP INDEX [IX_Map_Fakturalinje_FakturaLines_CleanCode] ON [map].[Fakturalinje];
    PRINT 'Dropped legacy index IX_Map_Fakturalinje_FakturaLines_CleanCode';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Map_Fakturalinje_DW_EK' AND object_id = OBJECT_ID(N'[map].[Fakturalinje]'))
BEGIN
    DROP INDEX [IX_Map_Fakturalinje_DW_EK] ON [map].[Fakturalinje];
    PRINT 'Dropped IX_Map_Fakturalinje_DW_EK (will be recreated after migration)';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Map_Fakturalinje_CleanCode_FakturaLines' AND object_id = OBJECT_ID(N'[map].[Fakturalinje]'))
BEGIN
    DROP INDEX [IX_Map_Fakturalinje_CleanCode_FakturaLines] ON [map].[Fakturalinje];
    PRINT 'Dropped IX_Map_Fakturalinje_CleanCode_FakturaLines (will be recreated after migration)';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'PK_Map_Fakturalinje')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'Fakturalinje_CleanCode')
BEGIN
    ALTER TABLE [map].[Fakturalinje] DROP CONSTRAINT [PK_Map_Fakturalinje];
    PRINT 'Dropped PK_Map_Fakturalinje (depends on legacy column, will be recreated)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'FakturaLines_CleanCode')
    EXEC sp_rename '[map].[Fakturalinje].[FakturaLines_CleanCode]', 'bk_FakturaLines', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'FakturaHeader_CleanCode')
    EXEC sp_rename '[map].[Fakturalinje].[FakturaHeader_CleanCode]', 'bk_FakturaHeader', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'FakturaMetadata_CleanCode')
    EXEC sp_rename '[map].[Fakturalinje].[FakturaMetadata_CleanCode]', 'bk_FakturaMetadata', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'Fakturalinje_CleanCode')
    EXEC sp_rename '[map].[Fakturalinje].[Fakturalinje_CleanCode]', 'CleanCode_Fakturalinje', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'CleanCode_FakturaLines')
    EXEC sp_rename '[map].[Fakturalinje].[CleanCode_FakturaLines]', 'bk_FakturaLines', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'CleanCode_FakturaHeader')
    EXEC sp_rename '[map].[Fakturalinje].[CleanCode_FakturaHeader]', 'bk_FakturaHeader', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'CleanCode_FakturaMetadata')
    EXEC sp_rename '[map].[Fakturalinje].[CleanCode_FakturaMetadata]', 'bk_FakturaMetadata', 'COLUMN';
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = 'PK_Map_Fakturalinje')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND type = 'U')
BEGIN
    ALTER TABLE [map].[Fakturalinje] ADD CONSTRAINT [PK_Map_Fakturalinje] PRIMARY KEY CLUSTERED ([CleanCode_Fakturalinje] ASC);
    PRINT 'Recreated PK_Map_Fakturalinje on CleanCode_Fakturalinje';
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

-- ============================================================================
-- 11. Add bk_* transparency columns to map tables
--     Keep original column names for backward compatibility; add bk_ prefixed
--     copies populated with the same value for naming convention compliance.
-- ============================================================================

-- ---- map.Kontoplan: add bk_FinanskontoId, bk_Kontonummer ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'bk_FinanskontoId')
BEGIN
    ALTER TABLE [map].[Kontoplan] ADD [bk_FinanskontoId] BIGINT NULL;
    PRINT 'Added bk_FinanskontoId to map.Kontoplan';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'bk_Kontonummer')
BEGIN
    ALTER TABLE [map].[Kontoplan] ADD [bk_Kontonummer] NVARCHAR(255) NULL;
    PRINT 'Added bk_Kontonummer to map.Kontoplan';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'bk_FinanskontoId')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'FinanskontoId')
BEGIN
    EXEC('UPDATE [map].[Kontoplan] SET [bk_FinanskontoId] = [FinanskontoId] WHERE [bk_FinanskontoId] IS NULL');
    EXEC('UPDATE [map].[Kontoplan] SET [bk_Kontonummer] = [Kontonummer] WHERE [bk_Kontonummer] IS NULL');
    PRINT 'Backfilled bk_FinanskontoId and bk_Kontonummer from existing columns';
END
GO

-- ---- map.Udlevering: add bk_prrowid ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'bk_prrowid')
BEGIN
    ALTER TABLE [map].[Udlevering] ADD [bk_prrowid] NVARCHAR(100) NULL;
    PRINT 'Added bk_prrowid to map.Udlevering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'bk_prrowid')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'prrowid')
BEGIN
    EXEC('UPDATE [map].[Udlevering] SET [bk_prrowid] = [prrowid] WHERE [bk_prrowid] IS NULL AND [prrowid] IS NOT NULL');
    PRINT 'Backfilled bk_prrowid from prrowid';
END
GO

-- ---- map.Lager: add bk_Code ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'bk_Code')
BEGIN
    ALTER TABLE [map].[Lager] ADD [bk_Code] [varchar](100) NULL;
    PRINT 'Added bk_Code to map.Lager';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'bk_Code')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'Code')
BEGIN
    EXEC('UPDATE [map].[Lager] SET [bk_Code] = [Code] WHERE [bk_Code] IS NULL');
    PRINT 'Backfilled bk_Code from Code';
END
GO

-- ---- map.Rekvirent: add bk_Code ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'bk_Code')
BEGIN
    ALTER TABLE [map].[Rekvirent] ADD [bk_Code] [varchar](100) NULL;
    PRINT 'Added bk_Code to map.Rekvirent';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'bk_Code')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'Code')
BEGIN
    EXEC('UPDATE [map].[Rekvirent] SET [bk_Code] = [Code] WHERE [bk_Code] IS NULL');
    PRINT 'Backfilled bk_Code from Code';
END
GO
