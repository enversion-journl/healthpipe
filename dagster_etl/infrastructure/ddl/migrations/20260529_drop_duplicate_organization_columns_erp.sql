-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop leftover DW_EK_Organization (legacy "z") columns where the modern
-- DW_EK_Organisation ("s") column already exists (dual-column state).
--
-- WHY THIS IS A NEW FILE (and not a fix to 20260427):
--   20260427_rename_organization_to_organisation.sql already contains a
--   pre-cleanup block that drops the duplicate DW_EK_Organization on
--   fct.Fakturalinje / fct.Postering. But execute_migrations records every
--   applied filename in [meta].[migration_history] and skips it on every later
--   run (ddl_loader.execute_migrations). On databases where the dual-column
--   state arose *after* 20260427 was already recorded as applied — e.g. the
--   end-state DDL added DW_EK_Organisation while the legacy column lingered, or
--   20260427 ran as a no-op before the table existed — that cleanup never gets
--   re-evaluated. Re-running the infrastructure job does nothing because:
--     - the only migration that drops the duplicate is already "applied", and
--     - the table DDL (03_facts.sql) is IF NOT EXISTS <table>, so it never
--       ALTERs the live table to remove the stray column.
--   Symptom: dso_sync_fct_postering fails with
--     "Columns in STAGING but not DSO: ['DW_EK_Organization']".
--
-- This migration re-runs the drop-duplicate logic, metadata-driven so it covers
-- the legacy index/default-constraint regardless of their names, across every
-- table that carries the Organization→Organisation rename history. The legacy
-- column was always created with DEFAULT -1 and no real data, so dropping it is
-- safe. Every step is guarded; safe to re-run.
-- ============================================================================

DECLARE @tables TABLE (full_name NVARCHAR(300));
INSERT INTO @tables (full_name) VALUES
    (N'[fct].[Fakturalinje]'),
    (N'[fct].[Postering]'),
    (N'[fct].[Faktura]'),
    (N'[fct].[Budget]'),
    (N'[fct].[Lagerbeholdning]'),
    (N'[fct].[AfdelingLagerbeholdning]'),
    (N'[dim].[Lager]'),
    (N'[dim].[Rekvirent]'),
    (N'[dim].[Organisation]'),
    (N'[map].[Organisation]'),
    (N'[sec].[BrugerOrganisation]');

DECLARE @full NVARCHAR(300);
DECLARE tbl_cur CURSOR LOCAL FAST_FORWARD FOR SELECT full_name FROM @tables;
OPEN tbl_cur;
FETCH NEXT FROM tbl_cur INTO @full;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(@full) IS NOT NULL
       AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@full) AND name = 'DW_EK_Organization')
       AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@full) AND name = 'DW_EK_Organisation')
    BEGIN
        -- Drop any index that references the legacy column (names vary per table).
        DECLARE @drop_ix NVARCHAR(MAX) = N'';
        SELECT @drop_ix = @drop_ix + N'DROP INDEX ' + QUOTENAME(i.name) + N' ON ' + @full + N'; '
          FROM sys.indexes i
          JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
          JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
         WHERE i.object_id = OBJECT_ID(@full)
           AND c.name = N'DW_EK_Organization'
           AND i.name IS NOT NULL;
        IF @drop_ix <> N'' EXEC(@drop_ix);

        -- Drop the default constraint on the legacy column.
        -- NOTE: EXEC() does not allow function calls (e.g. QUOTENAME) in its
        -- concatenated argument, so build the statement into a variable first.
        DECLARE @df SYSNAME = NULL;
        SELECT @df = dc.name
          FROM sys.default_constraints dc
          JOIN sys.columns c ON c.default_object_id = dc.object_id
         WHERE c.object_id = OBJECT_ID(@full) AND c.name = N'DW_EK_Organization';
        IF @df IS NOT NULL
        BEGIN
            DECLARE @sql_df NVARCHAR(MAX) = N'ALTER TABLE ' + @full + N' DROP CONSTRAINT ' + QUOTENAME(@df);
            EXEC(@sql_df);
        END

        DECLARE @sql_col NVARCHAR(MAX) = N'ALTER TABLE ' + @full + N' DROP COLUMN [DW_EK_Organization]';
        EXEC(@sql_col);
        PRINT 'Dropped duplicate DW_EK_Organization from ' + @full + ' (DW_EK_Organisation already exists)';
    END

    FETCH NEXT FROM tbl_cur INTO @full;
END
CLOSE tbl_cur;
DEALLOCATE tbl_cur;
GO
