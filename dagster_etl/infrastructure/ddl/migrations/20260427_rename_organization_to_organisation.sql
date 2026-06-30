-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename Organization → Organisation across DSO data model.
--
-- Touches: dim.Organization, map.Organization, sec.BrugerOrganization,
--          vw_dim.Organization, sec.PopulateBrugerOrganization,
--          sec.HentRBSikkerhed_v2, and DW_EK_Organization columns on
--          dim.Lager, dim.Rekvirent, fct.Fakturalinje, fct.Postering,
--          fct.Faktura.
-- Also renames internal indexes/PKs/UQs containing "Organization".
-- Drops the dependent view/procedure/function so the underlying tables can be
-- renamed; recreation is handled by the end-state DDL files which run after
-- this migration (CREATE OR ALTER on each):
--   - infrastructure/ddl/views/security/01_vw_dim_security.sql (vw_dim.Organisation, no ModifiedDate alias — [internal ticket])
--   - infrastructure/ddl/tables/security/02_functions.sql      (sec.HentRBSikkerhed_v2)
--   - infrastructure/ddl/tables/security/03_procedures.sql     (sec.PopulateBrugerOrganisation)
-- Safe to re-run: every step is guarded.
-- Tasks: #27718, #27719
-- ============================================================================

-- ---- 1. Drop dependents that bind by name ----

IF OBJECT_ID(N'[vw_dim].[Organization]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_dim].[Organization];
    PRINT 'Dropped view vw_dim.Organization';
END
GO

IF OBJECT_ID(N'[sec].[PopulateBrugerOrganization]', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE [sec].[PopulateBrugerOrganization];
    PRINT 'Dropped procedure sec.PopulateBrugerOrganization';
END
GO

IF OBJECT_ID(N'[sec].[HentRBSikkerhed_v2]', 'IF') IS NOT NULL
BEGIN
    DROP FUNCTION [sec].[HentRBSikkerhed_v2];
    PRINT 'Dropped function sec.HentRBSikkerhed_v2';
END
GO

-- ---- 1.5. Pre-cleanup: drop dual-column state on fct.Fakturalinje and
--           fct.Postering. 20260410_erp_fcts_migrations.sql adds
--           DW_EK_Organization to these two tables; on servers where the
--           end-state DDL has already created DW_EK_Organisation, both
--           columns now coexist and would block the sp_rename below. The
--           added legacy column was created with DEFAULT -1 (no real data),
--           so dropping it is safe.

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organization')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organisation')
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Fct_Fakturalinje_Organization' AND object_id = OBJECT_ID(N'[fct].[Fakturalinje]'))
        DROP INDEX [IX_Fct_Fakturalinje_Organization] ON [fct].[Fakturalinje];

    DECLARE @df_name NVARCHAR(200);
    SELECT @df_name = dc.name
      FROM sys.default_constraints dc
      JOIN sys.columns c ON c.default_object_id = dc.object_id
     WHERE c.object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
       AND c.name = N'DW_EK_Organization';
    IF @df_name IS NOT NULL
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + @df_name + N']');

    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [DW_EK_Organization];
    PRINT 'Dropped duplicate DW_EK_Organization from fct.Fakturalinje (DW_EK_Organisation already exists)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organization')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organisation')
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Fct_Postering_Organization' AND object_id = OBJECT_ID(N'[fct].[Postering]'))
        DROP INDEX [IX_Fct_Postering_Organization] ON [fct].[Postering];

    DECLARE @df_name NVARCHAR(200);
    SELECT @df_name = dc.name
      FROM sys.default_constraints dc
      JOIN sys.columns c ON c.default_object_id = dc.object_id
     WHERE c.object_id = OBJECT_ID(N'[fct].[Postering]')
       AND c.name = N'DW_EK_Organization';
    IF @df_name IS NOT NULL
        EXEC(N'ALTER TABLE [fct].[Postering] DROP CONSTRAINT [' + @df_name + N']');

    ALTER TABLE [fct].[Postering] DROP COLUMN [DW_EK_Organization];
    PRINT 'Dropped duplicate DW_EK_Organization from fct.Postering (DW_EK_Organisation already exists)';
END
GO

-- ---- 2. Rename DW_EK_Organization columns on 8 tables ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organization]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[dim].[Organization].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on dim.Organization';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organization]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[map].[Organization].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on map.Organization';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[dim].[Lager].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on dim.Lager';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[dim].[Rekvirent].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on dim.Rekvirent';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[fct].[Fakturalinje].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Fakturalinje';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[fct].[Faktura].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Faktura';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[BrugerOrganization]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[sec].[BrugerOrganization].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on sec.BrugerOrganization';
END
GO

-- ---- 3. Rename indexes ----

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organization_SourceType' AND object_id = OBJECT_ID(N'[dim].[Organization]'))
BEGIN
    EXEC sp_rename N'[dim].[Organization].[IX_dim_Organization_SourceType]', N'IX_dim_Organisation_SourceType', N'INDEX';
    PRINT 'Renamed index IX_dim_Organization_SourceType';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organization_CodeSource' AND object_id = OBJECT_ID(N'[dim].[Organization]'))
BEGIN
    EXEC sp_rename N'[dim].[Organization].[IX_dim_Organization_CodeSource]', N'IX_dim_Organisation_CodeSource', N'INDEX';
    PRINT 'Renamed index IX_dim_Organization_CodeSource';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organization_ParentSOR' AND object_id = OBJECT_ID(N'[dim].[Organization]'))
BEGIN
    EXEC sp_rename N'[dim].[Organization].[IX_dim_Organization_ParentSOR]', N'IX_dim_Organisation_ParentSOR', N'INDEX';
    PRINT 'Renamed index IX_dim_Organization_ParentSOR';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organization_ParentSource' AND object_id = OBJECT_ID(N'[dim].[Organization]'))
BEGIN
    EXEC sp_rename N'[dim].[Organization].[IX_dim_Organization_ParentSource]', N'IX_dim_Organisation_ParentSource', N'INDEX';
    PRINT 'Renamed index IX_dim_Organization_ParentSource';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_map_Organization_CleanCode' AND object_id = OBJECT_ID(N'[map].[Organization]'))
BEGIN
    EXEC sp_rename N'[map].[Organization].[IX_map_Organization_CleanCode]', N'IX_map_Organisation_CleanCode', N'INDEX';
    PRINT 'Renamed index IX_map_Organization_CleanCode';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_BrugerOrganization_Bruger' AND object_id = OBJECT_ID(N'[sec].[BrugerOrganization]'))
BEGIN
    EXEC sp_rename N'[sec].[BrugerOrganization].[IX_BrugerOrganization_Bruger]', N'IX_BrugerOrganisation_Bruger', N'INDEX';
    PRINT 'Renamed index IX_BrugerOrganization_Bruger';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_BrugerOrganization_Org' AND object_id = OBJECT_ID(N'[sec].[BrugerOrganization]'))
BEGIN
    EXEC sp_rename N'[sec].[BrugerOrganization].[IX_BrugerOrganization_Org]', N'IX_BrugerOrganisation_Org', N'INDEX';
    PRINT 'Renamed index IX_BrugerOrganization_Org';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Fct_Fakturalinje_Organization' AND object_id = OBJECT_ID(N'[fct].[Fakturalinje]'))
BEGIN
    EXEC sp_rename N'[fct].[Fakturalinje].[IX_Fct_Fakturalinje_Organization]', N'IX_Fct_Fakturalinje_Organisation', N'INDEX';
    PRINT 'Renamed index IX_Fct_Fakturalinje_Organization';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Fct_Postering_Organization' AND object_id = OBJECT_ID(N'[fct].[Postering]'))
BEGIN
    EXEC sp_rename N'[fct].[Postering].[IX_Fct_Postering_Organization]', N'IX_Fct_Postering_Organisation', N'INDEX';
    PRINT 'Renamed index IX_Fct_Postering_Organization';
END
GO

-- ---- 4. Rename PK / UQ constraints ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'PK_Map_Organization' AND type = 'PK' AND schema_id = SCHEMA_ID('map'))
BEGIN
    EXEC sp_rename N'[map].[PK_Map_Organization]', N'PK_Map_Organisation', N'OBJECT';
    PRINT 'Renamed constraint PK_Map_Organization';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'PK_BrugerOrganization' AND type = 'PK' AND schema_id = SCHEMA_ID('sec'))
BEGIN
    EXEC sp_rename N'[sec].[PK_BrugerOrganization]', N'PK_BrugerOrganisation', N'OBJECT';
    PRINT 'Renamed constraint PK_BrugerOrganization';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'UQ_BrugerOrganization_Bruger_Org' AND type = 'UQ' AND schema_id = SCHEMA_ID('sec'))
BEGIN
    EXEC sp_rename N'[sec].[UQ_BrugerOrganization_Bruger_Org]', N'UQ_BrugerOrganisation_Bruger_Org', N'OBJECT';
    PRINT 'Renamed constraint UQ_BrugerOrganization_Bruger_Org';
END
GO

-- ---- 5. Rename tables ----
-- Skip sp_rename when the target name already exists (DDL may have created Organisation first).

IF OBJECT_ID(N'[dim].[Organization]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[Organisation]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[dim].[Organization]', N'Organisation', N'OBJECT';
    PRINT 'Renamed table dim.Organization -> dim.Organisation';
END
ELSE IF OBJECT_ID(N'[dim].[Organization]', 'U') IS NOT NULL
BEGIN
    PRINT 'Skipped dim.Organization rename: dim.Organisation already exists';
END
GO

IF OBJECT_ID(N'[map].[Organization]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[Organisation]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[map].[Organization]', N'Organisation', N'OBJECT';
    PRINT 'Renamed table map.Organization -> map.Organisation';
END
ELSE IF OBJECT_ID(N'[map].[Organization]', 'U') IS NOT NULL
BEGIN
    PRINT 'Skipped map.Organization rename: map.Organisation already exists';
END
GO

IF OBJECT_ID(N'[sec].[BrugerOrganization]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[sec].[BrugerOrganisation]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[sec].[BrugerOrganization]', N'BrugerOrganisation', N'OBJECT';
    PRINT 'Renamed table sec.BrugerOrganization -> sec.BrugerOrganisation';
END
ELSE IF OBJECT_ID(N'[sec].[BrugerOrganization]', 'U') IS NOT NULL
BEGIN
    PRINT 'Skipped sec.BrugerOrganization rename: sec.BrugerOrganisation already exists';
END
GO
