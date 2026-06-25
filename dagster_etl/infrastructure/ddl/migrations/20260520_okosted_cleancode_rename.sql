-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Okosted map: rename columns and strip OKOSTED_ prefix from CleanCode data.
--
-- CleanCode -> CleanCode_Okosted (standard naming convention)
-- CodeOkosted -> bk_Code (map bk_ convention)
-- Data: 'OKOSTED_SUF.11.1' -> 'SUF.11.1'
--
-- Safe to re-run: every step is guarded.
-- ============================================================================

-- ============================================================================
-- STEP 1: Rename CleanCode -> CleanCode_Okosted
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND name = 'CleanCode'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND name = 'CleanCode_Okosted'
)
BEGIN
    EXEC sp_rename '[map].[Okosted].[CleanCode]', 'CleanCode_Okosted', 'COLUMN';
    PRINT 'Renamed map.Okosted.CleanCode -> CleanCode_Okosted';
END
GO

-- ============================================================================
-- STEP 2: Rename CodeOkosted -> bk_Code
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND name = 'CodeOkosted'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND name = 'bk_Code'
)
BEGIN
    EXEC sp_rename '[map].[Okosted].[CodeOkosted]', 'bk_Code', 'COLUMN';
    PRINT 'Renamed map.Okosted.CodeOkosted -> bk_Code';
END
GO

-- ============================================================================
-- STEP 3: Strip OKOSTED_ prefix from existing data
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND name = 'CleanCode_Okosted'
)
BEGIN
    EXEC(N'
        UPDATE [map].[Okosted]
        SET [CleanCode_Okosted] = SUBSTRING([CleanCode_Okosted], 9, LEN([CleanCode_Okosted]))
        WHERE [CleanCode_Okosted] LIKE N''OKOSTED_%''
    ');
    PRINT 'Stripped OKOSTED_ prefix from map.Okosted.CleanCode_Okosted data';
END
GO

-- ============================================================================
-- STEP 4: Drop old index and recreate with new column name
-- ============================================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_map_Okosted_CleanCode'
      AND object_id = OBJECT_ID(N'[map].[Okosted]')
)
BEGIN
    DROP INDEX IX_map_Okosted_CleanCode ON [map].[Okosted];
    PRINT 'Dropped index IX_map_Okosted_CleanCode';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_map_Okosted_CleanCode_Okosted'
      AND object_id = OBJECT_ID(N'[map].[Okosted]')
)
AND EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND name = 'CleanCode_Okosted'
)
BEGIN
    CREATE INDEX IX_map_Okosted_CleanCode_Okosted
        ON [map].[Okosted] ([CleanCode_Okosted]);
    PRINT 'Created index IX_map_Okosted_CleanCode_Okosted';
END
GO
