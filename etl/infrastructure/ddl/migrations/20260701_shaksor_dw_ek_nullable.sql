-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Make conceptmap.SHAKToSOR surrogate-key columns nullable on STAGING.
--
-- Background:
--   cm_shak_to_sor calls merge_conceptmap_table (INSERT/UPDATE of the mapping
--   rows) BEFORE update_conceptmap_dw_ek resolves DW_EK_SHAK and
--   DW_EK_Organisation_Target (joins to map.SHAK / ref.Organisation). The
--   staging table must therefore allow NULL in those columns at insert time,
--   exactly like the other conceptmap.*ToSOR staging tables
--   (see 20260423_conceptmap_add_dw_ek.sql, which added them as INT NULL).
--
--   The initial SHAKToSOR DDL created them NOT NULL, so the first prod run of
--   cm_shak_to_sor failed with:
--     "Cannot insert the value NULL into column 'DW_EK_SHAK',
--      table '...conceptmap.SHAKToSOR'; column does not allow nulls."
--   The base DDL (tables/conceptmap/01_conceptmap_tables.sql) is IF NOT EXISTS,
--   so it does not fix a table that was already created — hence this migration.
--
-- Scope: STAGING only. On DSO the table keeps DW_EK_SHAK as its PRIMARY KEY
--   (NOT NULL); the DSO sync only ever receives resolved rows via
--   conceptmap.vw_shak_to_sor. The DB_NAME guard makes this a no-op on DSO.
--
-- Idempotent: each ALTER runs only while the column is still NOT NULL.
-- Work item: #28796
-- ============================================================================

IF DB_NAME() LIKE N'%staging%'
   AND EXISTS (
       SELECT 1 FROM sys.objects
       WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]') AND type = N'U'
   )
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]')
          AND name = N'DW_EK_SHAK' AND is_nullable = 0
    )
    BEGIN
        ALTER TABLE [conceptmap].[SHAKToSOR] ALTER COLUMN [DW_EK_SHAK] INT NULL;
        PRINT 'Made conceptmap.SHAKToSOR.DW_EK_SHAK nullable (staging)';
    END

    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]')
          AND name = N'DW_EK_Organisation_Target' AND is_nullable = 0
    )
    BEGIN
        ALTER TABLE [conceptmap].[SHAKToSOR] ALTER COLUMN [DW_EK_Organisation_Target] INT NULL;
        PRINT 'Made conceptmap.SHAKToSOR.DW_EK_Organisation_Target nullable (staging)';
    END
END
