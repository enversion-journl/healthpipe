-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: fct.Vareudlevering referential integrity changes (#27494)
-- 1. Drop transitive DW_EK_Organization column (now resolved in DSO view)
-- 2. Add direct DW_EK_IlsVare FK column
-- 3. Add supporting indexes

-- Step 1: Drop IX_Fct_Vareudlevering_Organization index
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'IX_Fct_Vareudlevering_Organization'
)
BEGIN
    DROP INDEX [IX_Fct_Vareudlevering_Organization] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Vareudlevering_Organization';
END
GO

-- Step 2: Drop default constraint on DW_EK_Organization (dynamic lookup)
DECLARE @df_name NVARCHAR(256);
SELECT @df_name = dc.name
FROM sys.default_constraints dc
INNER JOIN sys.columns c
    ON dc.parent_object_id = c.object_id
   AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
  AND c.name = N'DW_EK_Organization';

IF @df_name IS NOT NULL
BEGIN
    EXEC('ALTER TABLE [fct].[Vareudlevering] DROP CONSTRAINT [' + @df_name + ']');
    PRINT 'Dropped default constraint on DW_EK_Organization: ' + @df_name;
END
GO

-- Step 3: Drop column DW_EK_Organization
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'DW_EK_Organization'
)
BEGIN
    ALTER TABLE [fct].[Vareudlevering] DROP COLUMN [DW_EK_Organization];
    PRINT 'Dropped column DW_EK_Organization from fct.Vareudlevering';
END
GO

-- Step 4: Add column DW_EK_IlsVare
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'DW_EK_IlsVare'
)
BEGIN
    ALTER TABLE [fct].[Vareudlevering] ADD [DW_EK_IlsVare] INT NOT NULL DEFAULT -1;
    PRINT 'Added column DW_EK_IlsVare to fct.Vareudlevering';
END
GO

-- Step 5: Add index IX_Fct_Vareudlevering_IlsVare
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'IX_Fct_Vareudlevering_IlsVare'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_IlsVare] ON [fct].[Vareudlevering]
    ([DW_EK_IlsVare] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_IlsVare';
END
GO

-- Step 6: Add index IX_Fct_Vareudlevering_bk_prrowid
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'IX_Fct_Vareudlevering_bk_prrowid'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_bk_prrowid] ON [fct].[Vareudlevering]
    ([bk_prrowid] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_bk_prrowid';
END
GO
