-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- SCHEMA CHANGE ONLY. This migration replaces the 8 undifferentiated
-- GebyrKategori* columns on dim.Faktura and dim.Fakturalinje with 6 new
-- Rabat/Tillaeg columns (3 each). It ONLY adds the new (empty) columns and
-- drops the old ones — it does NOT backfill or transform any data.
--
-- The new columns are populated by the dim ETL (stg_dim_faktura_mysupply /
-- stg_dim_fakturalinje_mysupply) on the mandatory full reload this PR requires.
-- That staging logic is where the actual split happens: Indikering = 'false'
-- -> Rabat, everything else -> Tillaeg; KategoriTotal embedded into Aarsag as
-- '{Aarsag}:{Total}'; rows with NULL / 0 / 0.00 Total excluded. Until the
-- reload runs, the new columns are NULL and the old data is gone — do not ship
-- the post-migration, pre-reload state to consumers.
--
-- Idempotent: safe to re-run.

-- ============================================================================
-- dim.Faktura — add 6 new columns
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'GebyrRabatAarsag')
    ALTER TABLE [dim].[Faktura] ADD [GebyrRabatAarsag] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'GebyrRabatKategori')
    ALTER TABLE [dim].[Faktura] ADD [GebyrRabatKategori] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'GebyrRabatFinanskonto')
    ALTER TABLE [dim].[Faktura] ADD [GebyrRabatFinanskonto] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'GebyrTillaegAarsag')
    ALTER TABLE [dim].[Faktura] ADD [GebyrTillaegAarsag] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'GebyrTillaegKategori')
    ALTER TABLE [dim].[Faktura] ADD [GebyrTillaegKategori] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'GebyrTillaegFinanskonto')
    ALTER TABLE [dim].[Faktura] ADD [GebyrTillaegFinanskonto] NVARCHAR(MAX) NULL;

-- ============================================================================
-- dim.Faktura — drop 8 old columns
-- ============================================================================

DECLARE @sql NVARCHAR(MAX);

-- Helper: drop default constraint before dropping column
DECLARE @drop_cols TABLE (col_name SYSNAME);
INSERT INTO @drop_cols (col_name) VALUES
    (N'GebyrKategori'), (N'GebyrKategoriIndikering'), (N'GebyrKategoriÅrsag'),
    (N'GebyrKategoriKvantitet'), (N'GebyrKategoriKvantitet_Type'),
    (N'GebyrKategoriKvantitet_List'), (N'GebyrKategoriTotal'), (N'GebyrKategoriFinanskonto');

DECLARE @col SYSNAME;
DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT col_name FROM @drop_cols;
OPEN col_cur;
FETCH NEXT FROM col_cur INTO @col;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = @col)
    BEGIN
        -- Drop auto-named default constraint if any
        SELECT @sql = N'ALTER TABLE [dim].[Faktura] DROP CONSTRAINT ' + QUOTENAME(dc.name)
        FROM sys.default_constraints dc
        JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Faktura]') AND c.name = @col;
        IF @sql IS NOT NULL EXEC sp_executesql @sql;
        SET @sql = NULL;

        SET @sql = N'ALTER TABLE [dim].[Faktura] DROP COLUMN ' + QUOTENAME(@col);
        EXEC sp_executesql @sql;
        SET @sql = NULL;
    END
    FETCH NEXT FROM col_cur INTO @col;
END
CLOSE col_cur;
DEALLOCATE col_cur;

-- ============================================================================
-- dim.Fakturalinje — add 6 new columns
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'FakturaUdstederGebyrRabatAarsag')
    ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrRabatAarsag] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'FakturaUdstederGebyrRabatKategori')
    ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrRabatKategori] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'FakturaUdstederGebyrRabatFinanskonto')
    ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrRabatFinanskonto] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'FakturaUdstederGebyrTillaegAarsag')
    ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrTillaegAarsag] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'FakturaUdstederGebyrTillaegKategori')
    ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrTillaegKategori] NVARCHAR(MAX) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'FakturaUdstederGebyrTillaegFinanskonto')
    ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrTillaegFinanskonto] NVARCHAR(MAX) NULL;

-- ============================================================================
-- dim.Fakturalinje — drop 8 old columns
-- ============================================================================

DELETE FROM @drop_cols;
INSERT INTO @drop_cols (col_name) VALUES
    (N'FakturaUdstederGebyrKategori'), (N'FakturaUdstederGebyrKategoriIndikering'),
    (N'FakturaUdstederGebyrKategoriÅrsag'), (N'FakturaUdstederGebyrKategoriKvantitet'),
    (N'FakturaUdstederGebyrKategoriKvantitet_Type'), (N'FakturaUdstederGebyrKategoriKvantitet_List'),
    (N'FakturaUdstederGebyrKategoriTotal'), (N'FakturaUdstederGebyrKategoriFinanskonto');

DECLARE col_cur2 CURSOR LOCAL FAST_FORWARD FOR
    SELECT col_name FROM @drop_cols;
OPEN col_cur2;
FETCH NEXT FROM col_cur2 INTO @col;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = @col)
    BEGIN
        SELECT @sql = N'ALTER TABLE [dim].[Fakturalinje] DROP CONSTRAINT ' + QUOTENAME(dc.name)
        FROM sys.default_constraints dc
        JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND c.name = @col;
        IF @sql IS NOT NULL EXEC sp_executesql @sql;
        SET @sql = NULL;

        SET @sql = N'ALTER TABLE [dim].[Fakturalinje] DROP COLUMN ' + QUOTENAME(@col);
        EXEC sp_executesql @sql;
        SET @sql = NULL;
    END
    FETCH NEXT FROM col_cur2 INTO @col;
END
CLOSE col_cur2;
DEALLOCATE col_cur2;
