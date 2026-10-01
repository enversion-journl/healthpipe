-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop legacy-named header-measure columns from fct.Fakturalinje.
--
-- WHY: The original 20260601_faktura_add_full_coverage_columns.sql added
--   header-grain measures to fct.Fakturalinje under their original names
--   (MomsBeloeb, MomsGrundlag, FakturaTotalBeloeb, etc.) before the file was
--   rewritten in place with new names (VAT_AfgiftsKategori,
--   Valutakode_MomsGrundlag, etc.). Because migration_history already tracked
--   the filename, the rewrite never re-ran, so both generations of columns
--   coexist on servers that received the original version.
--
--   20260611_fakturalinje_remove_header_measures.sql dropped the new-name
--   columns (LinjeTotal, VAT_AfgiftsKategori, Valutakode_*) but not the
--   legacy names below. No MERGE or staging code references these columns;
--   their header-grain data lives on fct.Faktura.
--
--   DW_EK_Organisation was targeted by 20260602_faktura_drop_dw_ek_organisation
--   but may still be present on some servers; included here for completeness.
--
-- Applied to BOTH staging and DSO (each tracks meta.migration_history).
-- Safe to re-run: IF EXISTS guards; auto-named DEFAULT constraints and
-- indexes dropped first. GO-separated batches to isolate variable scoping.
-- ============================================================================

-- ---- MomsBeloeb ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'MomsBeloeb';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'MomsBeloeb')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [MomsBeloeb];
    PRINT 'Dropped MomsBeloeb from fct.Fakturalinje';
END
GO

-- ---- MomsGrundlag ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'MomsGrundlag';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'MomsGrundlag')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [MomsGrundlag];
    PRINT 'Dropped MomsGrundlag from fct.Fakturalinje';
END
GO

-- ---- MomsProcent ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'MomsProcent';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'MomsProcent')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [MomsProcent];
    PRINT 'Dropped MomsProcent from fct.Fakturalinje';
END
GO

-- ---- AfgiftsKategori (bare name; VAT_AfgiftsKategori already dropped by 20260611) ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'AfgiftsKategori';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'AfgiftsKategori')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [AfgiftsKategori];
    PRINT 'Dropped AfgiftsKategori from fct.Fakturalinje';
END
GO

-- ---- Valutakode_Moms (bare name; split Valutakode_MomsGrundlag/MomsBeloeb already dropped) ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'Valutakode_Moms';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_Moms')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_Moms];
    PRINT 'Dropped Valutakode_Moms from fct.Fakturalinje';
END
GO

-- ---- FakturaTotalBeloeb ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'FakturaTotalBeloeb';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'FakturaTotalBeloeb')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [FakturaTotalBeloeb];
    PRINT 'Dropped FakturaTotalBeloeb from fct.Fakturalinje';
END
GO

-- ---- FakturaLinjeTotalBeloeb ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'FakturaLinjeTotalBeloeb';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'FakturaLinjeTotalBeloeb')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [FakturaLinjeTotalBeloeb];
    PRINT 'Dropped FakturaLinjeTotalBeloeb from fct.Fakturalinje';
END
GO

-- ---- FakturaMomsTotal ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'FakturaMomsTotal';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'FakturaMomsTotal')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [FakturaMomsTotal];
    PRINT 'Dropped FakturaMomsTotal from fct.Fakturalinje';
END
GO

-- ---- Valutakode_Total ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'Valutakode_Total';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_Total')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_Total];
    PRINT 'Dropped Valutakode_Total from fct.Fakturalinje';
END
GO

-- ---- DW_EK_Organisation (may survive from earlier incomplete migration) ----
-- This column has DEFAULT -1 and possibly an index; use full metadata cleanup.
DECLARE @sql NVARCHAR(MAX);

-- Drop indexes referencing DW_EK_Organisation.
DECLARE @drop_ix NVARCHAR(MAX) = N'';
SELECT @drop_ix = @drop_ix + N'DROP INDEX ' + QUOTENAME(i.name) + N' ON [fct].[Fakturalinje]; '
  FROM sys.indexes i
  JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
  JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
 WHERE i.object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
   AND c.name = N'DW_EK_Organisation'
   AND i.name IS NOT NULL;
IF @drop_ix <> N'' EXEC(@drop_ix);

-- Drop default constraint.
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'DW_EK_Organisation';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organisation')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [DW_EK_Organisation];
    PRINT 'Dropped DW_EK_Organisation from fct.Fakturalinje';
END
GO
