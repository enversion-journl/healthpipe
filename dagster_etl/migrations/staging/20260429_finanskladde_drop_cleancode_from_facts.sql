-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop CleanCode_* columns from fct.Finanskladde and fct.Finanskladdelinje.
--
-- The CleanCode columns are not needed in the fact tables — DW_EK is the
-- canonical surrogate key, and CleanCode is resolved upstream via the map
-- tables.
--
-- Safe to re-run: IF EXISTS guards on the column names.
-- Apply to: staging database. A matching migration exists under migrations/dso/.
-- Task: #27805
-- ============================================================================

-- ---- fct.Finanskladde ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Finanskladde]')
      AND name = 'CleanCode_Finanskladde'
)
BEGIN
    DECLARE @sql_fk NVARCHAR(MAX) = N'';
    SELECT @sql_fk = @sql_fk + 'DROP INDEX [' + i.name + '] ON [fct].[Finanskladde]; '
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fct].[Finanskladde]')
      AND c.name = 'CleanCode_Finanskladde'
      AND i.is_primary_key = 0;
    IF LEN(@sql_fk) > 0 EXEC sp_executesql @sql_fk;

    ALTER TABLE [fct].[Finanskladde] DROP COLUMN [CleanCode_Finanskladde];
    PRINT 'Dropped CleanCode_Finanskladde from fct.Finanskladde';
END
GO

-- ---- fct.Finanskladdelinje ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
      AND name = 'CleanCode_Finanskladdelinje'
)
BEGIN
    DECLARE @sql_fkl NVARCHAR(MAX) = N'';
    SELECT @sql_fkl = @sql_fkl + 'DROP INDEX [' + i.name + '] ON [fct].[Finanskladdelinje]; '
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
      AND c.name = 'CleanCode_Finanskladdelinje'
      AND i.is_primary_key = 0;
    IF LEN(@sql_fkl) > 0 EXEC sp_executesql @sql_fkl;

    ALTER TABLE [fct].[Finanskladdelinje] DROP COLUMN [CleanCode_Finanskladdelinje];
    PRINT 'Dropped CleanCode_Finanskladdelinje from fct.Finanskladdelinje';
END
GO
