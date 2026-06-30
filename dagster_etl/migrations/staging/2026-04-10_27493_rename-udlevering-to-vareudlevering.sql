-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Rename Udlevering -> Vareudlevering / VareudleveringEgenskaber
-- Date: 2026-04-10
--
-- Renames three persistent tables plus all associated columns, constraints,
-- and indexes. Also drops stale DSO views.
--
-- Run this script on BOTH the staging database AND the DSO database
-- BEFORE deploying new code.
--
-- Idempotent: safe to re-run (each step is guarded by IF EXISTS checks).
--
-- NOTE: SQL Server blocks sp_rename on columns with dependent indexes.
-- Therefore we DROP indexes first, rename columns, then RECREATE indexes.
-- ============================================================================

-- ============================================================================
-- 1. RENAME TABLES
-- ============================================================================

-- 1a. map.Udlevering -> map.Vareudlevering
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND type = 'U')
BEGIN
    EXEC sp_rename N'[map].[Udlevering]', N'Vareudlevering';
    PRINT 'Renamed table map.Udlevering -> map.Vareudlevering';
END
GO

-- 1b. dim.Udlevering -> dim.VareudleveringEgenskaber
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Udlevering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND type = 'U')
BEGIN
    EXEC sp_rename N'[dim].[Udlevering]', N'VareudleveringEgenskaber';
    PRINT 'Renamed table dim.Udlevering -> dim.VareudleveringEgenskaber';
END
GO

-- 1c. fct.Udlevering -> fct.Vareudlevering
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Udlevering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND type = 'U')
BEGIN
    EXEC sp_rename N'[fct].[Udlevering]', N'Vareudlevering';
    PRINT 'Renamed table fct.Udlevering -> fct.Vareudlevering';
END
GO

-- ============================================================================
-- 2. DROP NON-CLUSTERED INDEXES (must happen before column renames)
--    SQL Server blocks sp_rename on columns with dependent indexes.
-- ============================================================================

-- 2a. Map: IX_Map_Udlevering_DW_EK (references DW_EK_Udlevering, CleanCode_Udlevering)
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Map_Udlevering_DW_EK' AND object_id = OBJECT_ID(N'[map].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Map_Udlevering_DW_EK] ON [map].[Vareudlevering];
    PRINT 'Dropped index IX_Map_Udlevering_DW_EK';
END
GO

-- 2b. Dim: IX_Dim_Udlevering_CleanCode (references CleanCode_Udlevering)
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Dim_Udlevering_CleanCode' AND object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]'))
BEGIN
    DROP INDEX [IX_Dim_Udlevering_CleanCode] ON [dim].[VareudleveringEgenskaber];
    PRINT 'Dropped index IX_Dim_Udlevering_CleanCode';
END
GO

-- 2b-cleanup. Dim: Drop accidental indexes on dim.VareudleveringEgenskaber
IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'UQ__Vareudle__3570C97AB695692E' AND parent_object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]'))
BEGIN
    ALTER TABLE [dim].[VareudleveringEgenskaber] DROP CONSTRAINT [UQ__Vareudle__3570C97AB695692E];
    PRINT 'Dropped accidental unique constraint UQ__Vareudle__3570C97AB695692E';
END
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'nsh_test' AND object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]'))
BEGIN
    DROP INDEX [nsh_test] ON [dim].[VareudleveringEgenskaber];
    PRINT 'Dropped accidental index nsh_test';
END
GO

-- 2b-watermark. Dim: IX_dim_Udlevering_Watermark -> IX_dim_VareudleveringEgenskaber_Watermark
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_dim_Udlevering_Watermark' AND object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]'))
BEGIN
    EXEC sp_rename N'[dim].[VareudleveringEgenskaber].[IX_dim_Udlevering_Watermark]', N'IX_dim_VareudleveringEgenskaber_Watermark', N'INDEX';
    PRINT 'Renamed index IX_dim_Udlevering_Watermark -> IX_dim_VareudleveringEgenskaber_Watermark';
END
GO

-- 2c. Fct: IX_Fct_Udlevering_CleanCode (references CleanCode_Udlevering)
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_CleanCode' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_CleanCode] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_CleanCode';
END
GO

-- 2d. Fct: IX_Fct_Udlevering_Organization
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Organization' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Organization] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Organization';
END
GO

-- 2e. Fct: IX_Fct_Udlevering_Lager
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Lager' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Lager] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Lager';
END
GO

-- 2f. Fct: IX_Fct_Udlevering_Rekvirent
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Rekvirent' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Rekvirent] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Rekvirent';
END
GO

-- 2g. Fct: IX_Fct_Udlevering_Vare
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Vare' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Vare] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Vare';
END
GO

-- 2h. Fct: IX_Fct_Udlevering_Dato_Udlevering
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Dato_Udlevering' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Dato_Udlevering] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Dato_Udlevering';
END
GO

-- 2i. Fct: IX_Fct_Udlevering_Dato_Udloeb
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Dato_Udloeb' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Dato_Udloeb] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Dato_Udloeb';
END
GO

-- 2j. Fct: IX_Fct_Udlevering_Dato_Postering
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Dato_Postering' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Dato_Postering] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Dato_Postering';
END
GO

-- 2k. Fct: IX_Fct_Udlevering_Dato_Aflevering
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Udlevering_Dato_Aflevering' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    DROP INDEX [IX_Fct_Udlevering_Dato_Aflevering] ON [fct].[Vareudlevering];
    PRINT 'Dropped index IX_Fct_Udlevering_Dato_Aflevering';
END
GO

-- ============================================================================
-- 3. RENAME COLUMNS (indexes are now dropped, so sp_rename is safe)
-- ============================================================================

-- 3a. DW_EK_Udlevering -> DW_EK_Vareudlevering (map)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND name = 'DW_EK_Udlevering')
BEGIN
    EXEC sp_rename N'[map].[Vareudlevering].[DW_EK_Udlevering]', N'DW_EK_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column map.Vareudlevering.DW_EK_Udlevering -> DW_EK_Vareudlevering';
END
GO

-- 3b. CleanCode_Udlevering -> CleanCode_Vareudlevering (map)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND name = 'CleanCode_Udlevering')
BEGIN
    EXEC sp_rename N'[map].[Vareudlevering].[CleanCode_Udlevering]', N'CleanCode_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column map.Vareudlevering.CleanCode_Udlevering -> CleanCode_Vareudlevering';
END
GO

-- 3c. DW_EK_Udlevering -> DW_EK_Vareudlevering (dim)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND name = 'DW_EK_Udlevering')
BEGIN
    EXEC sp_rename N'[dim].[VareudleveringEgenskaber].[DW_EK_Udlevering]', N'DW_EK_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column dim.VareudleveringEgenskaber.DW_EK_Udlevering -> DW_EK_Vareudlevering';
END
GO

-- 3d. CleanCode_Udlevering -> CleanCode_Vareudlevering (dim)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND name = 'CleanCode_Udlevering')
BEGIN
    EXEC sp_rename N'[dim].[VareudleveringEgenskaber].[CleanCode_Udlevering]', N'CleanCode_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column dim.VareudleveringEgenskaber.CleanCode_Udlevering -> CleanCode_Vareudlevering';
END
GO

-- 3e. bk_id -> bk_prrowid (dim)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND name = 'bk_id')
BEGIN
    EXEC sp_rename N'[dim].[VareudleveringEgenskaber].[bk_id]', N'bk_prrowid', N'COLUMN';
    PRINT 'Renamed column dim.VareudleveringEgenskaber.bk_id -> bk_prrowid';
END
GO

-- 3f. DW_EK_Udlevering -> DW_EK_Vareudlevering (fct)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'DW_EK_Udlevering')
BEGIN
    EXEC sp_rename N'[fct].[Vareudlevering].[DW_EK_Udlevering]', N'DW_EK_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column fct.Vareudlevering.DW_EK_Udlevering -> DW_EK_Vareudlevering';
END
GO

-- 3g. CleanCode_Udlevering -> CleanCode_Vareudlevering (fct)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'CleanCode_Udlevering')
BEGIN
    EXEC sp_rename N'[fct].[Vareudlevering].[CleanCode_Udlevering]', N'CleanCode_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column fct.Vareudlevering.CleanCode_Udlevering -> CleanCode_Vareudlevering';
END
GO

-- 3h. Udlevering_Antal -> Vareudlevering_Antal (fct)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'Udlevering_Antal')
BEGIN
    EXEC sp_rename N'[fct].[Vareudlevering].[Udlevering_Antal]', N'Vareudlevering_Antal', N'COLUMN';
    PRINT 'Renamed column fct.Vareudlevering.Udlevering_Antal -> Vareudlevering_Antal';
END
GO

-- 3i. Udlevering_Beloeb -> Vareudlevering_Beloeb (fct)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'Udlevering_Beloeb')
BEGIN
    EXEC sp_rename N'[fct].[Vareudlevering].[Udlevering_Beloeb]', N'Vareudlevering_Beloeb', N'COLUMN';
    PRINT 'Renamed column fct.Vareudlevering.Udlevering_Beloeb -> Vareudlevering_Beloeb';
END
GO

-- 3j. Datotid_Udlevering -> Datotid_Vareudlevering (fct)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'Datotid_Udlevering')
BEGIN
    EXEC sp_rename N'[fct].[Vareudlevering].[Datotid_Udlevering]', N'Datotid_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column fct.Vareudlevering.Datotid_Udlevering -> Datotid_Vareudlevering';
END
GO

-- 3k. DW_EK_Dato_Udlevering -> DW_EK_Dato_Vareudlevering (fct)
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'DW_EK_Dato_Udlevering')
BEGIN
    EXEC sp_rename N'[fct].[Vareudlevering].[DW_EK_Dato_Udlevering]', N'DW_EK_Dato_Vareudlevering', N'COLUMN';
    PRINT 'Renamed column fct.Vareudlevering.DW_EK_Dato_Udlevering -> DW_EK_Dato_Vareudlevering';
END
GO

-- ============================================================================
-- 4. RENAME CONSTRAINTS (Primary Keys)
-- ============================================================================

-- 4a. PK_Map_Udlevering -> PK_Map_Vareudlevering
IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'PK_Map_Udlevering')
BEGIN
    EXEC sp_rename N'[map].[PK_Map_Udlevering]', N'PK_Map_Vareudlevering', N'OBJECT';
    PRINT 'Renamed constraint PK_Map_Udlevering -> PK_Map_Vareudlevering';
END
GO

-- 4b. PK_Dim_Udlevering -> PK_Dim_VareudleveringEgenskaber
IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'PK_Dim_Udlevering')
BEGIN
    EXEC sp_rename N'[dim].[PK_Dim_Udlevering]', N'PK_Dim_VareudleveringEgenskaber', N'OBJECT';
    PRINT 'Renamed constraint PK_Dim_Udlevering -> PK_Dim_VareudleveringEgenskaber';
END
GO

-- 4c. PK_Fct_Udlevering -> PK_Fct_Vareudlevering
IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'PK_Fct_Udlevering')
BEGIN
    EXEC sp_rename N'[fct].[PK_Fct_Udlevering]', N'PK_Fct_Vareudlevering', N'OBJECT';
    PRINT 'Renamed constraint PK_Fct_Udlevering -> PK_Fct_Vareudlevering';
END
GO

-- ============================================================================
-- 5. RECREATE INDEXES (with new names on renamed columns)
-- ============================================================================

-- 5a. Map: IX_Map_Vareudlevering_DW_EK
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Map_Vareudlevering_DW_EK' AND object_id = OBJECT_ID(N'[map].[Vareudlevering]'))
   AND EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND name = 'DW_EK_Vareudlevering')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Vareudlevering_DW_EK]
    ON [map].[Vareudlevering] ([DW_EK_Vareudlevering] ASC)
    INCLUDE ([CleanCode_Vareudlevering]) ON [PRIMARY];
    PRINT 'Created index IX_Map_Vareudlevering_DW_EK';
END
GO

-- 5b. Dim: IX_Dim_VareudleveringEgenskaber_CleanCode
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Dim_VareudleveringEgenskaber_CleanCode' AND object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]'))
   AND EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND name = 'CleanCode_Vareudlevering')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_VareudleveringEgenskaber_CleanCode]
    ON [dim].[VareudleveringEgenskaber] ([CleanCode_Vareudlevering] ASC)
    WHERE [CleanCode_Vareudlevering] IS NOT NULL ON [PRIMARY];
    PRINT 'Created index IX_Dim_VareudleveringEgenskaber_CleanCode';
END
GO

-- 5c. Fct: IX_Fct_Vareudlevering_CleanCode
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_CleanCode' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
   AND EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'CleanCode_Vareudlevering')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_CleanCode]
    ON [fct].[Vareudlevering] ([CleanCode_Vareudlevering] ASC)
    WHERE [CleanCode_Vareudlevering] IS NOT NULL ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_CleanCode';
END
GO

-- 5d. Fct: IX_Fct_Vareudlevering_Organization
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Organization' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Organization]
    ON [fct].[Vareudlevering] ([DW_EK_Organization] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Organization';
END
GO

-- 5e. Fct: IX_Fct_Vareudlevering_Lager
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Lager' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Lager]
    ON [fct].[Vareudlevering] ([DW_EK_Lager] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Lager';
END
GO

-- 5f. Fct: IX_Fct_Vareudlevering_Rekvirent
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Rekvirent' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Rekvirent]
    ON [fct].[Vareudlevering] ([DW_EK_Rekvirent] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Rekvirent';
END
GO

-- 5g. Fct: IX_Fct_Vareudlevering_Vare
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Vare' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Vare]
    ON [fct].[Vareudlevering] ([DW_EK_Vare] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Vare';
END
GO

-- 5h. Fct: IX_Fct_Vareudlevering_Dato_Vareudlevering
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Dato_Vareudlevering' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Vareudlevering]
    ON [fct].[Vareudlevering] ([DW_EK_Dato_Vareudlevering] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Dato_Vareudlevering';
END
GO

-- 5i. Fct: IX_Fct_Vareudlevering_Dato_Udloeb
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Dato_Udloeb' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Udloeb]
    ON [fct].[Vareudlevering] ([DW_EK_Dato_Udloeb] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Dato_Udloeb';
END
GO

-- 5j. Fct: IX_Fct_Vareudlevering_Dato_Postering
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Dato_Postering' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Postering]
    ON [fct].[Vareudlevering] ([DW_EK_Dato_Postering] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Dato_Postering';
END
GO

-- 5k. Fct: IX_Fct_Vareudlevering_Dato_Aflevering
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Fct_Vareudlevering_Dato_Aflevering' AND object_id = OBJECT_ID(N'[fct].[Vareudlevering]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_Dato_Aflevering]
    ON [fct].[Vareudlevering] ([DW_EK_Dato_Aflevering] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_Dato_Aflevering';
END
GO

-- ============================================================================
-- 6. DROP STALE DSO VIEWS (only relevant on DSO database)
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[vw_dim].[Udlevering]'))
BEGIN
    DROP VIEW [vw_dim].[Udlevering];
    PRINT 'Dropped stale view vw_dim.Udlevering';
END
GO

IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[vw_fct].[Udlevering]'))
BEGIN
    DROP VIEW [vw_fct].[Udlevering];
    PRINT 'Dropped stale view vw_fct.Udlevering';
END
GO

PRINT '=== Migration complete ===';
GO
