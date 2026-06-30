-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ConceptMap: add DW_EK columns for staging -> DSO ref sync
-- Idempotent: only ALTER when table exists and column is missing.
-- ============================================================================

-- conceptmap.rekvirent_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]') AND name = 'DW_EK_Rekvirent')
BEGIN
    ALTER TABLE [conceptmap].[rekvirent_to_sor] ADD [DW_EK_Rekvirent] INT NULL;
    PRINT 'Added DW_EK_Rekvirent to conceptmap.rekvirent_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[rekvirent_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.rekvirent_to_sor';
END
GO

-- conceptmap.lager_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]') AND name = 'DW_EK_Lager')
BEGIN
    ALTER TABLE [conceptmap].[lager_to_sor] ADD [DW_EK_Lager] INT NULL;
    PRINT 'Added DW_EK_Lager to conceptmap.lager_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[lager_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.lager_to_sor';
END
GO

-- conceptmap.ean_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]') AND name = 'DW_EK_EAN')
BEGIN
    ALTER TABLE [conceptmap].[ean_to_sor] ADD [DW_EK_EAN] INT NULL;
    PRINT 'Added DW_EK_EAN to conceptmap.ean_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[ean_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.ean_to_sor';
END
GO

-- conceptmap.okosted_to_sor
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
         AND name IN (N'DW_EK_Organisation_Source', N'DW_EK_Organization_Source')
   )
BEGIN
    ALTER TABLE [conceptmap].[okosted_to_sor] ADD [DW_EK_Organisation_Source] INT NULL;
    PRINT 'Added DW_EK_Organisation_Source to conceptmap.okosted_to_sor';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]') AND type = 'U')
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
         AND name IN (N'DW_EK_Organisation_Target', N'DW_EK_Organization_Target')
   )
BEGIN
    ALTER TABLE [conceptmap].[okosted_to_sor] ADD [DW_EK_Organisation_Target] INT NULL;
    PRINT 'Added DW_EK_Organisation_Target to conceptmap.okosted_to_sor';
END
GO
