-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop DW_EK_Organisation from fct.Faktura and fct.Fakturalinje.
--
-- WHY: The Organisation FK is being removed from the invoice-header and
--   invoice-line facts. Organisation context for ERP invoices is not modelled
--   on these grains; the column carried only -1 (UNKNOWN) and added a
--   meaningless FK + index. fct.Postering keeps its DW_EK_Organisation.
--
--   03_facts.sql (CREATE TABLE) is IF NOT EXISTS <table>, so it never ALTERs a
--   live table to remove the column — hence this migration for existing
--   databases. Runs on BOTH staging and DSO via staging_migrations /
--   dso_migrations; the DSO sync (fct_faktura, fct_fakturalinje) reflects
--   source columns, so the column must be dropped on both sides to keep the
--   reflected column lists aligned.
--
-- Metadata-driven so the index/default-constraint are dropped regardless of
-- their names. Every step is guarded; safe to re-run (the file is recorded in
-- [meta].[migration_history] and skipped on later runs anyway).
-- ============================================================================

DECLARE @tables TABLE (full_name NVARCHAR(300));
INSERT INTO @tables (full_name) VALUES
    (N'[fct].[Faktura]'),
    (N'[fct].[Fakturalinje]');

DECLARE @full NVARCHAR(300);
DECLARE tbl_cur CURSOR LOCAL FAST_FORWARD FOR SELECT full_name FROM @tables;
OPEN tbl_cur;
FETCH NEXT FROM tbl_cur INTO @full;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(@full) IS NOT NULL
       AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@full) AND name = 'DW_EK_Organisation')
    BEGIN
        -- Drop any index that references the column (names vary per table).
        DECLARE @drop_ix NVARCHAR(MAX) = N'';
        SELECT @drop_ix = @drop_ix + N'DROP INDEX ' + QUOTENAME(i.name) + N' ON ' + @full + N'; '
          FROM sys.indexes i
          JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
          JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
         WHERE i.object_id = OBJECT_ID(@full)
           AND c.name = N'DW_EK_Organisation'
           AND i.name IS NOT NULL;
        IF @drop_ix <> N'' EXEC(@drop_ix);

        -- Drop the default constraint on the column.
        -- NOTE: EXEC() does not allow function calls (e.g. QUOTENAME) in its
        -- concatenated argument, so build the statement into a variable first.
        DECLARE @df SYSNAME = NULL;
        SELECT @df = dc.name
          FROM sys.default_constraints dc
          JOIN sys.columns c ON c.default_object_id = dc.object_id
         WHERE c.object_id = OBJECT_ID(@full) AND c.name = N'DW_EK_Organisation';
        IF @df IS NOT NULL
        BEGIN
            DECLARE @sql_df NVARCHAR(MAX) = N'ALTER TABLE ' + @full + N' DROP CONSTRAINT ' + QUOTENAME(@df);
            EXEC(@sql_df);
        END

        DECLARE @sql_col NVARCHAR(MAX) = N'ALTER TABLE ' + @full + N' DROP COLUMN [DW_EK_Organisation]';
        EXEC(@sql_col);
        PRINT 'Dropped DW_EK_Organisation from ' + @full;
    END

    FETCH NEXT FROM tbl_cur INTO @full;
END
CLOSE tbl_cur;
DEALLOCATE tbl_cur;
GO
