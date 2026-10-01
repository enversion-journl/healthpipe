-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Drop redundant DW_EK_Organisation on facts now resolved via vw_fct ConceptMap joins.
-- Organisation for reporting: use vw_fct.* (DW_EK_Organisation_Lager, _Rekvirent, _Regningsmodtager).
-- Postering retains fct.DW_EK_Organisation as okosted source key for ConceptMap_Okosted_To_SOR.

-- ---- fct.Lagerbeholdning ----
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
      AND name = N'IX_Fct_Lagerbeholdning_Organisation'
)
BEGIN
    DROP INDEX [IX_Fct_Lagerbeholdning_Organisation] ON [fct].[Lagerbeholdning];
    PRINT 'Dropped IX_Fct_Lagerbeholdning_Organisation';
END
GO

DECLARE @df_lb NVARCHAR(256);
SELECT @df_lb = dc.name
FROM sys.default_constraints dc
INNER JOIN sys.columns c
    ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
  AND c.name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
IF @df_lb IS NOT NULL
    EXEC('ALTER TABLE [fct].[Lagerbeholdning] DROP CONSTRAINT [' + @df_lb + ']');
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization')
)
BEGIN
    DECLARE @col_lb SYSNAME;
    SELECT @col_lb = name FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
    EXEC('ALTER TABLE [fct].[Lagerbeholdning] DROP COLUMN [' + @col_lb + ']');
    PRINT 'Dropped organisation column from fct.Lagerbeholdning';
END
GO

-- ---- fct.AfdelingLagerbeholdning ----
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
      AND name = N'IX_Fct_AfdelingLagerbeholdning_Organisation'
)
BEGIN
    DROP INDEX [IX_Fct_AfdelingLagerbeholdning_Organisation] ON [fct].[AfdelingLagerbeholdning];
    PRINT 'Dropped IX_Fct_AfdelingLagerbeholdning_Organisation';
END
GO

DECLARE @df_alb NVARCHAR(256);
SELECT @df_alb = dc.name
FROM sys.default_constraints dc
INNER JOIN sys.columns c
    ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
  AND c.name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
IF @df_alb IS NOT NULL
    EXEC('ALTER TABLE [fct].[AfdelingLagerbeholdning] DROP CONSTRAINT [' + @df_alb + ']');
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization')
)
BEGIN
    DECLARE @col_alb SYSNAME;
    SELECT @col_alb = name FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
    EXEC('ALTER TABLE [fct].[AfdelingLagerbeholdning] DROP COLUMN [' + @col_alb + ']');
    PRINT 'Dropped organisation column from fct.AfdelingLagerbeholdning';
END
GO

-- ---- fct.Faktura ----
DECLARE @df_fak NVARCHAR(256);
SELECT @df_fak = dc.name
FROM sys.default_constraints dc
INNER JOIN sys.columns c
    ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]')
  AND c.name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
IF @df_fak IS NOT NULL
    EXEC('ALTER TABLE [fct].[Faktura] DROP CONSTRAINT [' + @df_fak + ']');
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization')
)
BEGIN
    DECLARE @col_fak SYSNAME;
    SELECT @col_fak = name FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
    EXEC('ALTER TABLE [fct].[Faktura] DROP COLUMN [' + @col_fak + ']');
    PRINT 'Dropped organisation column from fct.Faktura';
END
GO

-- ---- fct.Fakturalinje ----
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = N'IX_Fct_Fakturalinje_Organisation'
)
BEGIN
    DROP INDEX [IX_Fct_Fakturalinje_Organisation] ON [fct].[Fakturalinje];
    PRINT 'Dropped IX_Fct_Fakturalinje_Organisation';
END
GO

DECLARE @df_fl NVARCHAR(256);
SELECT @df_fl = dc.name
FROM sys.default_constraints dc
INNER JOIN sys.columns c
    ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
  AND c.name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
IF @df_fl IS NOT NULL
    EXEC('ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + @df_fl + ']');
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization')
)
BEGIN
    DECLARE @col_fl SYSNAME;
    SELECT @col_fl = name FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name IN (N'DW_EK_Organisation', N'DW_EK_Organization');
    EXEC('ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [' + @col_fl + ']');
    PRINT 'Dropped organisation column from fct.Fakturalinje';
END
GO

-- Deprecate static EAN seed (replaced by ref.ConceptMap_EAN_To_SOR from cm_ean_to_sor pipeline).
IF EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[EAN_SOR_Mapping]') AND type = 'U'
)
BEGIN
    DROP TABLE [ref].[EAN_SOR_Mapping];
    PRINT 'Dropped ref.EAN_SOR_Mapping (superseded by ref.ConceptMap_EAN_To_SOR)';
END
GO
