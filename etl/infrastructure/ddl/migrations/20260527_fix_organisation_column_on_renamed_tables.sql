-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Fix DW_EK_Organization → DW_EK_Organisation on already-renamed tables.
--
-- The 20260427 migration renames columns on the OLD table names
-- (dim.Organization, sec.BrugerOrganization, etc.) but misses the case where
-- the table was already renamed to the new name while the column still has the
-- old "Organization" (z) spelling.
--
-- This migration covers the new-name tables so every path converges on the
-- correct column name.  Safe to re-run: every step is guarded.
-- ============================================================================

-- dim.Organisation
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[dim].[Organisation].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on dim.Organisation';
END
GO

-- map.Organisation
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[map].[Organisation].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on map.Organisation';
END
GO

-- sec.BrugerOrganisation
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[BrugerOrganisation]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[BrugerOrganisation]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[sec].[BrugerOrganisation].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on sec.BrugerOrganisation';
END
GO

-- dim.Lager
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[dim].[Lager].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on dim.Lager';
END
GO

-- dim.Rekvirent
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[dim].[Rekvirent].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on dim.Rekvirent';
END
GO

-- fct.Fakturalinje
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[fct].[Fakturalinje].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Fakturalinje';
END
GO

-- fct.Postering
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Postering';
END
GO

-- fct.Faktura
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[fct].[Faktura].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Faktura';
END
GO
