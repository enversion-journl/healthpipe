-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Fix Okosted ConceptMap ref table: rename stale DW_EK_Organisation_Source
-- to DW_EK_Okosted on DSO, and drop the duplicate column on staging.
--
-- Background:
--   Migration 20260518 renamed conceptmap.okosted_to_sor.DW_EK_Organisation_Source
--   -> DW_EK_Okosted on the STAGING side only.  The DSO physical table
--   ref.ConceptMap_Okosted_To_SOR was never renamed.
--
--   Migration 20260529_fix_missing_organisation_and_conceptmap_columns re-added
--   DW_EK_Organisation_Source to conceptmap.okosted_to_sor without checking for
--   the renamed DW_EK_Okosted, creating a stale duplicate on staging.
--
-- This migration:
--   1. DSO: renames ref.ConceptMap_Okosted_To_SOR.DW_EK_Organisation_Source
--           -> DW_EK_Okosted (guard: old exists, new does not, user table only)
--   2. Staging: drops the stale DW_EK_Organisation_Source from
--              conceptmap.okosted_to_sor when BOTH columns exist (duplicate)
--
-- Idempotent: safe to re-run on fresh and already-migrated databases.
-- Task: #28024
-- ============================================================================

-- ============================================================================
-- 1. DSO: Rename DW_EK_Organisation_Source -> DW_EK_Okosted on
--    ref.ConceptMap_Okosted_To_SOR (user table only, not on staging views)
-- ============================================================================
IF EXISTS (
    SELECT 1
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o ON c.object_id = o.object_id
    WHERE o.object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]')
      AND o.type = N'U'
      AND c.name = N'DW_EK_Organisation_Source'
)
AND NOT EXISTS (
    SELECT 1
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o ON c.object_id = o.object_id
    WHERE o.object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]')
      AND o.type = N'U'
      AND c.name = N'DW_EK_Okosted'
)
BEGIN
    EXEC sp_rename N'[ref].[ConceptMap_Okosted_To_SOR].[DW_EK_Organisation_Source]', N'DW_EK_Okosted', N'COLUMN';
    PRINT 'Renamed ref.ConceptMap_Okosted_To_SOR.DW_EK_Organisation_Source -> DW_EK_Okosted';
END
GO

-- ============================================================================
-- 2. Staging: Drop duplicate DW_EK_Organisation_Source from
--    conceptmap.okosted_to_sor when both it and DW_EK_Okosted exist
-- ============================================================================

-- 2a. Drop any DEFAULT constraint on the stale column first
DECLARE @dc_okosted NVARCHAR(256);
SELECT @dc_okosted = dc.name
FROM sys.default_constraints dc
JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE c.object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
  AND c.name = N'DW_EK_Organisation_Source';

IF @dc_okosted IS NOT NULL
BEGIN
    EXEC(N'ALTER TABLE [conceptmap].[okosted_to_sor] DROP CONSTRAINT [' + @dc_okosted + N']');
    PRINT 'Dropped DEFAULT constraint on conceptmap.okosted_to_sor.DW_EK_Organisation_Source';
END
GO

-- 2b. Drop the stale column only if the correct DW_EK_Okosted already exists
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
      AND name = N'DW_EK_Organisation_Source'
)
AND EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[conceptmap].[okosted_to_sor]')
      AND name = N'DW_EK_Okosted'
)
BEGIN
    ALTER TABLE [conceptmap].[okosted_to_sor] DROP COLUMN [DW_EK_Organisation_Source];
    PRINT 'Dropped stale DW_EK_Organisation_Source from conceptmap.okosted_to_sor (duplicate of DW_EK_Okosted)';
END
GO
