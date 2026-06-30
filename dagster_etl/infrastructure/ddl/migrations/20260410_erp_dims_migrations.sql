-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Dimension Table Migrations
--
-- All dim.* schema migrations in dependency order.
-- Every statement is idempotent. Safe to delete once applied to all environments.
-- ============================================================================

-- ============================================================================
-- 1. Rename legacy CleanCode columns to canonical naming
-- ============================================================================

-- dim.Kontoplan: Kontoplan_CleanCode -> CleanCode_Kontoplan
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND name = 'Kontoplan_CleanCode')
BEGIN
    EXEC sp_rename '[dim].[Kontoplan].[Kontoplan_CleanCode]', 'CleanCode_Kontoplan', 'COLUMN';
    PRINT 'Renamed dim.Kontoplan.Kontoplan_CleanCode -> CleanCode_Kontoplan';
END
GO

-- dim.Kreditor: CVR_CleanCode -> CleanCode_Kreditor
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'CVR_CleanCode')
BEGIN
    EXEC sp_rename '[dim].[Kreditor].[CVR_CleanCode]', 'CleanCode_Kreditor', 'COLUMN';
    PRINT 'Renamed dim.Kreditor.CVR_CleanCode -> CleanCode_Kreditor';
END
GO

-- dim.Aftale: Aftale_CleanCode -> CleanCode_Aftale
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'Aftale_CleanCode')
BEGIN
    EXEC sp_rename '[dim].[Aftale].[Aftale_CleanCode]', 'CleanCode_Aftale', 'COLUMN';
    PRINT 'Renamed dim.Aftale.Aftale_CleanCode -> CleanCode_Aftale';
END
GO

-- ============================================================================
-- 2. Rename dim.Faktura -> dim.FakturaEgenskaber (via dim.Faktura_egenskab)
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura_egenskab]') AND type = 'U')
BEGIN
    EXEC sp_rename '[dim].[Faktura]', 'FakturaEgenskaber';
    PRINT 'Renamed dim.Faktura -> dim.FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura_egenskab]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND type = 'U')
BEGIN
    EXEC sp_rename '[dim].[Faktura_egenskab]', 'FakturaEgenskaber';
    PRINT 'Renamed dim.Faktura_egenskab -> dim.FakturaEgenskaber';
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[FakturaEgenskaber](
        [DW_EK_Faktura] INT NOT NULL,
        [FakturaNr] NVARCHAR(100) NULL,
        [FakturaValutaKode] NVARCHAR(10) NULL,
        [FirmaNavn] NVARCHAR(255) NULL,
        [LeverandorCVR] NVARCHAR(50) NULL,
        [Adresse] NVARCHAR(255) NULL,
        [ByNavn] NVARCHAR(255) NULL,
        [Postnummer] NVARCHAR(20) NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_Dim_FakturaEgenskaber] PRIMARY KEY CLUSTERED ([DW_EK_Faktura] ASC)
    ) ON [PRIMARY];
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND name = 'PK_Dim_Faktura')
BEGIN
    ALTER TABLE [dim].[FakturaEgenskaber] DROP CONSTRAINT [PK_Dim_Faktura];
    ALTER TABLE [dim].[FakturaEgenskaber] ADD CONSTRAINT [PK_Dim_FakturaEgenskaber] PRIMARY KEY CLUSTERED ([DW_EK_Faktura] ASC);
    PRINT 'Renamed PK_Dim_Faktura -> PK_Dim_FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND name = 'PK_Dim_Faktura_egenskab')
BEGIN
    ALTER TABLE [dim].[FakturaEgenskaber] DROP CONSTRAINT [PK_Dim_Faktura_egenskab];
    ALTER TABLE [dim].[FakturaEgenskaber] ADD CONSTRAINT [PK_Dim_FakturaEgenskaber] PRIMARY KEY CLUSTERED ([DW_EK_Faktura] ASC);
    PRINT 'Renamed PK_Dim_Faktura_egenskab -> PK_Dim_FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND name = 'IX_Dim_Faktura_DW_EK')
BEGIN
    DROP INDEX [IX_Dim_Faktura_DW_EK] ON [dim].[FakturaEgenskaber];
    PRINT 'Dropped renamed IX_Dim_Faktura_DW_EK from FakturaEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND name = 'IX_Dim_Faktura_egenskab_DW_EK')
BEGIN
    DROP INDEX [IX_Dim_Faktura_egenskab_DW_EK] ON [dim].[FakturaEgenskaber];
    PRINT 'Dropped IX_Dim_Faktura_egenskab_DW_EK from FakturaEgenskaber';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND name = 'IX_Dim_FakturaEgenskaber_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_FakturaEgenskaber_DW_EK] ON [dim].[FakturaEgenskaber]
    ([DW_EK_Faktura] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- 3. Add DW_EK_Aftale column to dim.Vare
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND type = 'U')
BEGIN
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Vare' AND COLUMN_NAME = 'DW_EK_Aftale'
    )
    BEGIN
        ALTER TABLE [dim].[Vare] ADD [DW_EK_Aftale] INT NOT NULL DEFAULT -1;
        PRINT 'Added DW_EK_Aftale to dim.Vare';
    END
END
GO

-- ============================================================================
-- 4. Narrow DW_EK columns from BIGINT to INT
-- ============================================================================

-- dim.Kontoplan
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Kontoplan'
      AND COLUMN_NAME = 'DW_EK_Kontoplan' AND DATA_TYPE = 'bigint'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND name = 'PK_Dim_Kontoplan')
        ALTER TABLE [dim].[Kontoplan] DROP CONSTRAINT [PK_Dim_Kontoplan];
    ALTER TABLE [dim].[Kontoplan] ALTER COLUMN [DW_EK_Kontoplan] INT NOT NULL;
    ALTER TABLE [dim].[Kontoplan] ADD CONSTRAINT [PK_Dim_Kontoplan] PRIMARY KEY CLUSTERED ([DW_EK_Kontoplan] ASC);
    PRINT 'Narrowed dim.Kontoplan.DW_EK_Kontoplan from BIGINT to INT';
END
GO

-- dim.Kreditor
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Kreditor'
      AND COLUMN_NAME = 'DW_EK_Kreditor' AND DATA_TYPE = 'bigint'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'PK_Dim_Kreditor')
        ALTER TABLE [dim].[Kreditor] DROP CONSTRAINT [PK_Dim_Kreditor];
    ALTER TABLE [dim].[Kreditor] ALTER COLUMN [DW_EK_Kreditor] INT NOT NULL;
    ALTER TABLE [dim].[Kreditor] ADD CONSTRAINT [PK_Dim_Kreditor] PRIMARY KEY CLUSTERED ([DW_EK_Kreditor] ASC);
    PRINT 'Narrowed dim.Kreditor.DW_EK_Kreditor from BIGINT to INT';
END
GO

-- dim.Vare
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Vare'
      AND COLUMN_NAME = 'DW_EK_Vare' AND DATA_TYPE = 'bigint'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'PK_Dim_Vare')
        ALTER TABLE [dim].[Vare] DROP CONSTRAINT [PK_Dim_Vare];
    ALTER TABLE [dim].[Vare] ALTER COLUMN [DW_EK_Vare] INT NOT NULL;
    ALTER TABLE [dim].[Vare] ADD CONSTRAINT [PK_Dim_Vare] PRIMARY KEY CLUSTERED ([DW_EK_Vare] ASC);
    PRINT 'Narrowed dim.Vare.DW_EK_Vare from BIGINT to INT';
END
GO

-- dim.Vare: DW_EK_Aftale
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Vare'
      AND COLUMN_NAME = 'DW_EK_Aftale' AND DATA_TYPE = 'bigint'
)
BEGIN
    DECLARE @df_vare_aftale NVARCHAR(256);
    SELECT @df_vare_aftale = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vare]') AND c.name = 'DW_EK_Aftale';
    IF @df_vare_aftale IS NOT NULL
        EXEC('ALTER TABLE [dim].[Vare] DROP CONSTRAINT [' + @df_vare_aftale + ']');
    ALTER TABLE [dim].[Vare] ALTER COLUMN [DW_EK_Aftale] INT NOT NULL;
    ALTER TABLE [dim].[Vare] ADD DEFAULT -1 FOR [DW_EK_Aftale];
    PRINT 'Narrowed dim.Vare.DW_EK_Aftale from BIGINT to INT';
END
GO

-- dim.Aftale (composite PK with DW_ValidFrom)
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Aftale'
      AND COLUMN_NAME = 'DW_EK_Aftale' AND DATA_TYPE = 'bigint'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'IX_Dim_Aftale_Current')
        DROP INDEX [IX_Dim_Aftale_Current] ON [dim].[Aftale];
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'PK_Dim_Aftale')
        ALTER TABLE [dim].[Aftale] DROP CONSTRAINT [PK_Dim_Aftale];
    ALTER TABLE [dim].[Aftale] ALTER COLUMN [DW_EK_Aftale] INT NOT NULL;
    ALTER TABLE [dim].[Aftale] ADD CONSTRAINT [PK_Dim_Aftale] PRIMARY KEY CLUSTERED ([DW_EK_Aftale] ASC, [DW_ValidFrom] ASC);
    CREATE NONCLUSTERED INDEX [IX_Dim_Aftale_Current] ON [dim].[Aftale]
    ([DW_EK_Aftale] ASC)
    INCLUDE ([AftaleNummer], [AftaleType], [Aftaletekst])
    WHERE [DW_IsDeleted] = 0 AND [DW_ValidTo] = '9999-12-31';
    PRINT 'Narrowed dim.Aftale.DW_EK_Aftale from BIGINT to INT';
END
GO

-- dim.FakturaEgenskaber
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'FakturaEgenskaber'
      AND COLUMN_NAME = 'DW_EK_Faktura' AND DATA_TYPE = 'bigint'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND name = 'IX_Dim_FakturaEgenskaber_DW_EK')
        DROP INDEX [IX_Dim_FakturaEgenskaber_DW_EK] ON [dim].[FakturaEgenskaber];
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND name = 'PK_Dim_FakturaEgenskaber')
        ALTER TABLE [dim].[FakturaEgenskaber] DROP CONSTRAINT [PK_Dim_FakturaEgenskaber];
    ALTER TABLE [dim].[FakturaEgenskaber] ALTER COLUMN [DW_EK_Faktura] INT NOT NULL;
    ALTER TABLE [dim].[FakturaEgenskaber] ADD CONSTRAINT [PK_Dim_FakturaEgenskaber] PRIMARY KEY CLUSTERED ([DW_EK_Faktura] ASC);
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_FakturaEgenskaber_DW_EK] ON [dim].[FakturaEgenskaber]
    ([DW_EK_Faktura] ASC) ON [PRIMARY];
    PRINT 'Narrowed dim.FakturaEgenskaber.DW_EK_Faktura from BIGINT to INT';
END
GO

-- dim.Fakturalinje
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Fakturalinje'
      AND COLUMN_NAME = 'DW_EK_Fakturalinje' AND DATA_TYPE = 'bigint'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = 'IX_Dim_Fakturalinje_DW_EK')
        DROP INDEX [IX_Dim_Fakturalinje_DW_EK] ON [dim].[Fakturalinje];
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = 'PK_Dim_Fakturalinje')
        ALTER TABLE [dim].[Fakturalinje] DROP CONSTRAINT [PK_Dim_Fakturalinje];
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [DW_EK_Fakturalinje] INT NOT NULL;
    ALTER TABLE [dim].[Fakturalinje] ADD CONSTRAINT [PK_Dim_Fakturalinje] PRIMARY KEY CLUSTERED ([DW_EK_Fakturalinje] ASC);
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Fakturalinje_DW_EK] ON [dim].[Fakturalinje]
    ([DW_EK_Fakturalinje] ASC) ON [PRIMARY];
    PRINT 'Narrowed dim.Fakturalinje.DW_EK_Fakturalinje from BIGINT to INT';
END
GO

-- dim.Postering
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Postering'
      AND COLUMN_NAME = 'DW_EK_Postering' AND DATA_TYPE = 'bigint'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'[dim].[Postering]') AND name = 'PK_Dim_Postering')
        ALTER TABLE [dim].[Postering] DROP CONSTRAINT [PK_Dim_Postering];
    ALTER TABLE [dim].[Postering] ALTER COLUMN [DW_EK_Postering] INT NOT NULL;
    ALTER TABLE [dim].[Postering] ADD CONSTRAINT [PK_Dim_Postering] PRIMARY KEY CLUSTERED ([DW_EK_Postering] ASC);
    PRINT 'Narrowed dim.Postering.DW_EK_Postering from BIGINT to INT';
END
GO

-- ============================================================================
-- 5. Change Postering column types from BIGINT to NVARCHAR(100)
-- ============================================================================

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Postering'
      AND COLUMN_NAME = 'FinansposteringId' AND DATA_TYPE = 'bigint'
)
BEGIN
    ALTER TABLE [dim].[Postering] ALTER COLUMN [FinansposteringId] NVARCHAR(100) NULL;
    PRINT 'Changed dim.Postering.FinansposteringId from BIGINT to NVARCHAR(100)';
END
GO

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Postering'
      AND COLUMN_NAME = 'KreditortransaktionId' AND DATA_TYPE = 'bigint'
)
BEGIN
    ALTER TABLE [dim].[Postering] ALTER COLUMN [KreditortransaktionId] NVARCHAR(100) NULL;
    PRINT 'Changed dim.Postering.KreditortransaktionId from BIGINT to NVARCHAR(100)';
END
GO

-- ============================================================================
-- 6. Rename audit columns in dim.Lager, dim.Rekvirent, and dim.EAN
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'DW_Insert_DateTime')
    EXEC sp_rename 'dim.Lager.DW_Insert_DateTime', 'DW_Timestamp_Insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'DW_Update_DateTime')
    EXEC sp_rename 'dim.Lager.DW_Update_DateTime', 'DW_Timestamp_Update', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND name = 'DW_Insert_DateTime')
    EXEC sp_rename 'dim.Rekvirent.DW_Insert_DateTime', 'DW_Timestamp_Insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND name = 'DW_Update_DateTime')
    EXEC sp_rename 'dim.Rekvirent.DW_Update_DateTime', 'DW_Timestamp_Update', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[EAN]') AND name = 'DW_Insert_DateTime')
    EXEC sp_rename 'dim.EAN.DW_Insert_DateTime', 'DW_Timestamp_Insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[EAN]') AND name = 'DW_Update_DateTime')
    EXEC sp_rename 'dim.EAN.DW_Update_DateTime', 'DW_Timestamp_Update', 'COLUMN';
GO

-- ============================================================================
-- dim.Postering: Add ref_LedgertransId column for new grain
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = 'ref_LedgertransId')
BEGIN
    ALTER TABLE [dim].[Postering] ADD [ref_LedgertransId] BIGINT NULL;
    PRINT 'Added ref_LedgertransId column to dim.Postering';
END
GO

-- ============================================================================
-- dim.Postering: Add bk_ref_LedgertransId business key column
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = 'bk_ref_LedgertransId')
BEGIN
    ALTER TABLE [dim].[Postering] ADD [bk_ref_LedgertransId] BIGINT NULL;
    PRINT 'Added bk_ref_LedgertransId business key column to dim.Postering';
END
GO

-- ============================================================================
-- dim.Postering: Rename KreditortransaktionId -> ref_VendTransId
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = 'KreditortransaktionId')
BEGIN
    EXEC sp_rename '[dim].[Postering].[KreditortransaktionId]', 'ref_VendTransId', 'COLUMN';
    PRINT 'Renamed dim.Postering.KreditortransaktionId -> ref_VendTransId';
END
GO

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Postering'
      AND COLUMN_NAME = 'ref_VendTransId' AND DATA_TYPE <> 'bigint'
)
BEGIN
    ALTER TABLE [dim].[Postering] ALTER COLUMN [ref_VendTransId] BIGINT NULL;
    PRINT 'Changed dim.Postering.ref_VendTransId to BIGINT';
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = 'ref_VendTransId')
BEGIN
    ALTER TABLE [dim].[Postering] ADD [ref_VendTransId] BIGINT NULL;
    PRINT 'Added ref_VendTransId column to dim.Postering';
END
GO

-- ============================================================================
-- dim.Postering: Drop FinansposteringId (no longer exposed in DSA/DSO)
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = 'FinansposteringId')
BEGIN
    ALTER TABLE [dim].[Postering] DROP COLUMN [FinansposteringId];
    PRINT 'Dropped FinansposteringId from dim.Postering';
END
GO
