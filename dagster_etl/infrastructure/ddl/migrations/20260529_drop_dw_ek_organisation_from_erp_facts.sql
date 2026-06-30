-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop the now-orphaned DW_EK_Organisation column from the ERP fact tables
-- fct.Postering, fct.Budget, fct.Finanskladdelinje.
--
-- After the dim.Okosted extraction these three facts reference Okosted via
-- DW_EK_Okosted (-> map.Okosted) and no asset writes DW_EK_Organisation any
-- longer; it was frozen at the -1 sentinel. The other facts (Fakturalinje,
-- Faktura, Vareudlevering, Lagerbeholdning, AfdelingLagerbeholdning) still use
-- DW_EK_Organisation and are intentionally left untouched.
--
-- Applied via the centralized loader to BOTH the staging (DSA) and DSO
-- databases, so the strict staging<->DSO schema-match validation in the fct
-- sync stays satisfied. Run full_infrastructure_job (staging + DSO migrations)
-- before the next ERP sync.
--
-- Each step drops the dependent index and the auto-named DEFAULT constraint
-- before dropping the column. Safe to re-run: guarded on column existence.
-- ============================================================================

DECLARE @sql NVARCHAR(MAX);

-- ---- fct.Postering ----
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organisation'
)
BEGIN
    SET @sql = N'';
    SELECT @sql = @sql + 'DROP INDEX [' + i.name + '] ON [fct].[Postering]; '
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fct].[Postering]')
      AND c.name = 'DW_EK_Organisation'
      AND i.is_primary_key = 0;
    IF LEN(@sql) > 0 EXEC sp_executesql @sql;

    SET @sql = N'';
    SELECT @sql = @sql + 'ALTER TABLE [fct].[Postering] DROP CONSTRAINT [' + dc.name + ']; '
    FROM sys.default_constraints dc
    JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]')
      AND c.name = 'DW_EK_Organisation';
    IF LEN(@sql) > 0 EXEC sp_executesql @sql;

    ALTER TABLE [fct].[Postering] DROP COLUMN [DW_EK_Organisation];
    PRINT 'Dropped DW_EK_Organisation from fct.Postering';
END
GO

-- ---- fct.Budget ----
DECLARE @sql NVARCHAR(MAX);
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'DW_EK_Organisation'
)
BEGIN
    SET @sql = N'';
    SELECT @sql = @sql + 'DROP INDEX [' + i.name + '] ON [fct].[Budget]; '
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fct].[Budget]')
      AND c.name = 'DW_EK_Organisation'
      AND i.is_primary_key = 0;
    IF LEN(@sql) > 0 EXEC sp_executesql @sql;

    SET @sql = N'';
    SELECT @sql = @sql + 'ALTER TABLE [fct].[Budget] DROP CONSTRAINT [' + dc.name + ']; '
    FROM sys.default_constraints dc
    JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Budget]')
      AND c.name = 'DW_EK_Organisation';
    IF LEN(@sql) > 0 EXEC sp_executesql @sql;

    ALTER TABLE [fct].[Budget] DROP COLUMN [DW_EK_Organisation];
    PRINT 'Dropped DW_EK_Organisation from fct.Budget';
END
GO

-- ---- fct.Finanskladdelinje ----
DECLARE @sql NVARCHAR(MAX);
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = 'DW_EK_Organisation'
)
BEGIN
    SET @sql = N'';
    SELECT @sql = @sql + 'DROP INDEX [' + i.name + '] ON [fct].[Finanskladdelinje]; '
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
      AND c.name = 'DW_EK_Organisation'
      AND i.is_primary_key = 0;
    IF LEN(@sql) > 0 EXEC sp_executesql @sql;

    SET @sql = N'';
    SELECT @sql = @sql + 'ALTER TABLE [fct].[Finanskladdelinje] DROP CONSTRAINT [' + dc.name + ']; '
    FROM sys.default_constraints dc
    JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
      AND c.name = 'DW_EK_Organisation';
    IF LEN(@sql) > 0 EXEC sp_executesql @sql;

    ALTER TABLE [fct].[Finanskladdelinje] DROP COLUMN [DW_EK_Organisation];
    PRINT 'Dropped DW_EK_Organisation from fct.Finanskladdelinje';
END
GO
