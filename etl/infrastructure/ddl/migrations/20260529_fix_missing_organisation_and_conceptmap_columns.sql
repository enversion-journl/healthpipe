-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Fix schema drift: add missing columns to dim.Organisation and
-- conceptmap.okosted_to_sor.
--
-- dim.Organisation may be missing data columns (SourceType, CodeSource, etc.)
-- on databases where the table was created from an older DDL and the
-- IF NOT EXISTS guard in 02_dims.sql prevented the new schema from applying.
--
-- conceptmap.okosted_to_sor may be missing DW_EK_Organisation_Source/Target
-- on databases where migration 20260423 was tracked as applied before the
-- table existed (no-op).
--
-- All steps are idempotent; safe to re-run.
-- ============================================================================

-- ============================================================================
-- 1. dim.Organisation — add missing data columns
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'SourceType')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [SourceType] VARCHAR(10) NOT NULL DEFAULT 'UNKNOWN';
    PRINT 'Added SourceType to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'CodeSource')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [CodeSource] VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN';
    PRINT 'Added CodeSource to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'CodeSOR')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [CodeSOR] VARCHAR(50) NULL;
    PRINT 'Added CodeSOR to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSOR')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [DW_EK_Org_ParentSOR] INT NULL;
    PRINT 'Added DW_EK_Org_ParentSOR to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSource')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [DW_EK_Org_ParentSource] INT NULL;
    PRINT 'Added DW_EK_Org_ParentSource to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'ParentSOR')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [ParentSOR] VARCHAR(50) NULL;
    PRINT 'Added ParentSOR to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'ParentSource')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [ParentSource] VARCHAR(50) NULL;
    PRINT 'Added ParentSource to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'Name')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [Name] NVARCHAR(500) NULL;
    PRINT 'Added Name to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'Type')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [Type] VARCHAR(100) NULL;
    PRINT 'Added Type to dim.Organisation';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'Active')
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [Active] BIT DEFAULT 1;
    PRINT 'Added Active to dim.Organisation';
END
GO

-- Create missing indexes (guard: only if column exists and index does not)
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'SourceType')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'IX_dim_Organisation_SourceType')
BEGIN
    CREATE INDEX IX_dim_Organisation_SourceType ON [dim].[Organisation] ([SourceType]);
    PRINT 'Created index IX_dim_Organisation_SourceType';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'CodeSource')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'IX_dim_Organisation_CodeSource')
BEGIN
    CREATE INDEX IX_dim_Organisation_CodeSource ON [dim].[Organisation] ([CodeSource]);
    PRINT 'Created index IX_dim_Organisation_CodeSource';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSOR')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'IX_dim_Organisation_ParentSOR')
BEGIN
    CREATE INDEX IX_dim_Organisation_ParentSOR
        ON [dim].[Organisation] ([DW_EK_Org_ParentSOR])
        WHERE [DW_EK_Org_ParentSOR] IS NOT NULL;
    PRINT 'Created index IX_dim_Organisation_ParentSOR';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSource')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'IX_dim_Organisation_ParentSource')
BEGIN
    CREATE INDEX IX_dim_Organisation_ParentSource
        ON [dim].[Organisation] ([DW_EK_Org_ParentSource])
        WHERE [DW_EK_Org_ParentSource] IS NOT NULL;
    PRINT 'Created index IX_dim_Organisation_ParentSource';
END
GO

-- Update the UNKNOWN sentinel row to include the new column values
IF EXISTS (SELECT 1 FROM [dim].[Organisation] WHERE DW_EK_Organisation = -1)
BEGIN
    EXEC(N'
        UPDATE [dim].[Organisation]
        SET [SourceType] = ''UNKNOWN'',
            [CodeSource] = ''UNKNOWN'',
            [Name] = ''Unknown Organisation'',
            [Active] = 1
        WHERE [DW_EK_Organisation] = -1
          AND [SourceType] = ''UNKNOWN''
          AND [Name] IS NULL
    ');
    PRINT 'Updated UNKNOWN sentinel row in dim.Organisation';
END
GO

-- ============================================================================
-- 2. conceptmap.* — re-add DW_EK columns if missing
--    (covers databases where migration 20260423 was a no-op)
-- ============================================================================

-- conceptmap.rekvirent_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]') AND name = 'DW_EK_Rekvirent')
BEGIN
    ALTER TABLE [conceptmap].[rekvirent_to_sor] ADD [DW_EK_Rekvirent] INT NULL;
    PRINT 'Added DW_EK_Rekvirent to conceptmap.rekvirent_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[rekvirent_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.rekvirent_to_sor';
END
GO

-- conceptmap.lager_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]') AND name = 'DW_EK_Lager')
BEGIN
    ALTER TABLE [conceptmap].[lager_to_sor] ADD [DW_EK_Lager] INT NULL;
    PRINT 'Added DW_EK_Lager to conceptmap.lager_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[lager_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.lager_to_sor';
END
GO

-- conceptmap.ean_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]') AND name = 'DW_EK_EAN')
BEGIN
    ALTER TABLE [conceptmap].[ean_to_sor] ADD [DW_EK_EAN] INT NULL;
    PRINT 'Added DW_EK_EAN to conceptmap.ean_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[ean_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.ean_to_sor';
END
GO

-- conceptmap.okosted_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
         AND name IN (N'DW_EK_Organisation_Source', N'DW_EK_Organization_Source')
   )
BEGIN
    ALTER TABLE [conceptmap].[okosted_to_sor] ADD [DW_EK_Organisation_Source] INT NULL;
    PRINT 'Added DW_EK_Organisation_Source to conceptmap.okosted_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[okosted_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.okosted_to_sor';
END
GO
