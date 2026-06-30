-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Cleanup: Remove OKOSTED rows from dim/map.Organisation and simplify schema.
-- Part of the dim.Okosted extraction.
--
-- ORDERING: delete OKOSTED rows FIRST (needs SourceType column), then
-- rename/drop columns.
-- Safe to re-run: every step is guarded.
-- ============================================================================

-- ============================================================================
-- STEP 1: Delete OKOSTED rows (must run BEFORE column drops)
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'SourceType'
)
BEGIN
    EXEC(N'DELETE FROM [dim].[Organisation] WHERE [SourceType] = N''OKOSTED''');
    PRINT 'Deleted OKOSTED rows from dim.Organisation';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND name = 'SourceType'
)
BEGIN
    EXEC(N'DELETE FROM [map].[Organisation] WHERE [SourceType] = N''OKOSTED''');
    PRINT 'Deleted OKOSTED rows from map.Organisation';
END
GO

-- ============================================================================
-- STEP 2: Rename columns on dim.Organisation
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'CodeSource'
)
BEGIN
    EXEC sp_rename '[dim].[Organisation].[CodeSource]', 'Code', 'COLUMN';
    PRINT 'Renamed dim.Organisation.CodeSource -> Code';
END
GO

-- Drop filtered index before renaming the column it references (sp_rename
-- is blocked when a filtered-index WHERE clause references the column).
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_dim_Organisation_ParentSOR'
      AND object_id = OBJECT_ID(N'[dim].[Organisation]')
)
BEGIN
    DROP INDEX IX_dim_Organisation_ParentSOR ON [dim].[Organisation];
    PRINT 'Dropped filtered index IX_dim_Organisation_ParentSOR (before column rename)';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSOR'
)
BEGIN
    EXEC sp_rename '[dim].[Organisation].[DW_EK_Org_ParentSOR]', 'DW_EK_Org_Parent', 'COLUMN';
    PRINT 'Renamed dim.Organisation.DW_EK_Org_ParentSOR -> DW_EK_Org_Parent';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'ParentSOR'
)
BEGIN
    EXEC sp_rename '[dim].[Organisation].[ParentSOR]', 'ParentCode', 'COLUMN';
    PRINT 'Renamed dim.Organisation.ParentSOR -> ParentCode';
END
GO

-- ============================================================================
-- STEP 3: Drop indexes that reference columns we are about to drop
-- (must happen BEFORE column drops — SQL Server blocks DROP COLUMN when an
--  index depends on it)
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_dim_Organisation_SourceType'
      AND object_id = OBJECT_ID(N'[dim].[Organisation]')
)
BEGIN
    DROP INDEX IX_dim_Organisation_SourceType ON [dim].[Organisation];
    PRINT 'Dropped index IX_dim_Organisation_SourceType';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_dim_Organisation_CodeSource'
      AND object_id = OBJECT_ID(N'[dim].[Organisation]')
)
BEGIN
    DROP INDEX IX_dim_Organisation_CodeSource ON [dim].[Organisation];
    PRINT 'Dropped index IX_dim_Organisation_CodeSource';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_dim_Organisation_ParentSource'
      AND object_id = OBJECT_ID(N'[dim].[Organisation]')
)
BEGIN
    DROP INDEX IX_dim_Organisation_ParentSource ON [dim].[Organisation];
    PRINT 'Dropped index IX_dim_Organisation_ParentSource';
END
GO

-- ============================================================================
-- STEP 4: Drop columns from dim.Organisation
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'SourceType'
)
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [SourceType];
    PRINT 'Dropped SourceType from dim.Organisation';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'CodeSOR'
)
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [CodeSOR];
    PRINT 'Dropped CodeSOR from dim.Organisation';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSource'
)
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [DW_EK_Org_ParentSource];
    PRINT 'Dropped DW_EK_Org_ParentSource from dim.Organisation';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'ParentSource'
)
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [ParentSource];
    PRINT 'Dropped ParentSource from dim.Organisation';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'Type'
)
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [Type];
    PRINT 'Dropped Type from dim.Organisation';
END
GO

-- ============================================================================
-- STEP 5: Rename/drop on map.Organisation
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND name = 'CodeSource'
)
BEGIN
    EXEC sp_rename '[map].[Organisation].[CodeSource]', 'Code', 'COLUMN';
    PRINT 'Renamed map.Organisation.CodeSource -> Code';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND name = 'SourceType'
)
BEGIN
    ALTER TABLE [map].[Organisation] DROP COLUMN [SourceType];
    PRINT 'Dropped SourceType from map.Organisation';
END
GO

-- ============================================================================
-- STEP 6: Recreate indexes with new column names
-- ============================================================================
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_dim_Organisation_Parent'
      AND object_id = OBJECT_ID(N'[dim].[Organisation]')
)
AND EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_Parent'
)
BEGIN
    CREATE INDEX IX_dim_Organisation_Parent
        ON [dim].[Organisation] ([DW_EK_Org_Parent])
        WHERE [DW_EK_Org_Parent] IS NOT NULL;
    PRINT 'Created filtered index IX_dim_Organisation_Parent';
END
GO

-- Create new Code index if it doesn't exist
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_dim_Organisation_Code'
      AND object_id = OBJECT_ID(N'[dim].[Organisation]')
)
AND EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'Code'
)
BEGIN
    CREATE INDEX IX_dim_Organisation_Code ON [dim].[Organisation] ([Code]);
    PRINT 'Created index IX_dim_Organisation_Code';
END
GO

-- ============================================================================
-- STEP 7: Rename conceptmap column
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
      AND name = 'DW_EK_Organisation_Source'
)
BEGIN
    EXEC sp_rename '[conceptmap].[okosted_to_sor].[DW_EK_Organisation_Source]', 'DW_EK_Okosted', 'COLUMN';
    PRINT 'Renamed conceptmap.okosted_to_sor.DW_EK_Organisation_Source -> DW_EK_Okosted';
END
GO
