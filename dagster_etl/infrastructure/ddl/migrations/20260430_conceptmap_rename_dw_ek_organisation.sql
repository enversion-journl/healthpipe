-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ConceptMap: rename DW_EK_Organization_* -> DW_EK_Organisation_* on
-- conceptmap.* (staging persistence) and ref.ConceptMap_* (DSO physical tables).
-- Ref rename blocks guard on object type U so staging (ref = views) skips sp_rename.
-- Idempotent: each step runs only if the old column name exists.
-- Task: #27699
-- ============================================================================

-- conceptmap.rekvirent_to_sor
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]')
      AND name = 'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[conceptmap].[rekvirent_to_sor].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on conceptmap.rekvirent_to_sor';
END
GO

-- conceptmap.lager_to_sor
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[lager_to_sor]')
      AND name = 'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[conceptmap].[lager_to_sor].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on conceptmap.lager_to_sor';
END
GO

-- conceptmap.ean_to_sor
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[ean_to_sor]')
      AND name = 'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[conceptmap].[ean_to_sor].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on conceptmap.ean_to_sor';
END
GO

-- conceptmap.okosted_to_sor
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
      AND name = 'DW_EK_Organization_Source'
)
BEGIN
    EXEC sp_rename N'[conceptmap].[okosted_to_sor].[DW_EK_Organization_Source]', N'DW_EK_Organisation_Source', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Source -> DW_EK_Organisation_Source on conceptmap.okosted_to_sor';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
      AND name = 'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[conceptmap].[okosted_to_sor].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on conceptmap.okosted_to_sor';
END
GO

-- ============================================================================
-- DSO ref.ConceptMap_* (user tables only — staging uses views over conceptmap.*)
-- ============================================================================

-- ref.ConceptMap_Rekvirent_To_SOR
IF EXISTS (
    SELECT 1
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o ON c.object_id = o.object_id
    WHERE o.object_id = OBJECT_ID(N'[ref].[ConceptMap_Rekvirent_To_SOR]')
      AND o.type = N'U'
      AND c.name = N'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[ref].[ConceptMap_Rekvirent_To_SOR].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on ref.ConceptMap_Rekvirent_To_SOR';
END
GO

-- ref.ConceptMap_Lager_To_SOR
IF EXISTS (
    SELECT 1
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o ON c.object_id = o.object_id
    WHERE o.object_id = OBJECT_ID(N'[ref].[ConceptMap_Lager_To_SOR]')
      AND o.type = N'U'
      AND c.name = N'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[ref].[ConceptMap_Lager_To_SOR].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on ref.ConceptMap_Lager_To_SOR';
END
GO

-- ref.ConceptMap_EAN_To_SOR
IF EXISTS (
    SELECT 1
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o ON c.object_id = o.object_id
    WHERE o.object_id = OBJECT_ID(N'[ref].[ConceptMap_EAN_To_SOR]')
      AND o.type = N'U'
      AND c.name = N'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[ref].[ConceptMap_EAN_To_SOR].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on ref.ConceptMap_EAN_To_SOR';
END
GO

-- ref.ConceptMap_Okosted_To_SOR
IF EXISTS (
    SELECT 1
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o ON c.object_id = o.object_id
    WHERE o.object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]')
      AND o.type = N'U'
      AND c.name = N'DW_EK_Organization_Source'
)
BEGIN
    EXEC sp_rename N'[ref].[ConceptMap_Okosted_To_SOR].[DW_EK_Organization_Source]', N'DW_EK_Organisation_Source', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Source -> DW_EK_Organisation_Source on ref.ConceptMap_Okosted_To_SOR';
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o ON c.object_id = o.object_id
    WHERE o.object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]')
      AND o.type = N'U'
      AND c.name = N'DW_EK_Organization_Target'
)
BEGIN
    EXEC sp_rename N'[ref].[ConceptMap_Okosted_To_SOR].[DW_EK_Organization_Target]', N'DW_EK_Organisation_Target', N'COLUMN';
    PRINT 'Renamed DW_EK_Organization_Target -> DW_EK_Organisation_Target on ref.ConceptMap_Okosted_To_SOR';
END
GO
