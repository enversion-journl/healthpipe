-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop kreditor_id, kreditornummer, kreditorkonto, sprog_id and branche_id
-- from dim.Kreditor.
--
-- WHY (kreditor_id, kreditornummer): dim.Kreditor is CVR-grain
--   (JuridiskEnhed), but KreditorId and Kreditornummer identify a vendor
--   ACCOUNT — and one CVR routinely has multiple parallel active accounts
--   (182k+ CVRs with competing account rows, none of them SCD versions of
--   each other). Any single account id/number picked at CVR grain is
--   arbitrary. They are internal source-system keys (used inside pre-staging
--   reconstructions), not dimension attributes; facts resolve dw_ek_Kreditor
--   via the CVR CleanCode and never via these columns.
--
-- WHY (kreditorkonto, sprog_id, branche_id): legacy-DWH schema placeholders
--   replicated from the old dimension spec (erp_dims_and_facts.yml). The
--   SydAX feed (Kreditor.Kreditor) never delivered these fields — no extract,
--   arc or staging table carries them — so DimUpd wrote literal NULLs on
--   every run since the rebuild. Dropped as permanently-empty dead weight.
--
--   02_dims.sql (CREATE TABLE) is IF NOT EXISTS, so it never ALTERs a live
--   table — hence this migration for existing databases. Runs on BOTH
--   staging and DSO via staging_migrations / dso_migrations; the DSO sync
--   (dim_Kreditor) reflects source columns, so the columns must be dropped on
--   both sides. vw_dim.Kreditor is SELECT * and refreshes on the next
--   infrastructure run.
--
-- Every step is guarded; safe to re-run.
-- ============================================================================

DECLARE @cols TABLE (col SYSNAME);
INSERT INTO @cols (col) VALUES
    (N'kreditor_id'),
    (N'kreditornummer'),
    (N'kreditorkonto'),
    (N'sprog_id'),
    (N'branche_id');

DECLARE @col SYSNAME;
DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR SELECT col FROM @cols;
OPEN col_cur;
FETCH NEXT FROM col_cur INTO @col;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(N'[dim].[Kreditor]') IS NOT NULL
       AND EXISTS (SELECT 1 FROM sys.columns
                   WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = @col)
    BEGIN
        -- Drop any index that references the column (names vary per environment).
        DECLARE @drop_ix NVARCHAR(MAX) = N'';
        SELECT @drop_ix = @drop_ix + N'DROP INDEX ' + QUOTENAME(i.name) + N' ON [dim].[Kreditor]; '
          FROM sys.indexes i
          JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
          JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
         WHERE i.object_id = OBJECT_ID(N'[dim].[Kreditor]')
           AND c.name = @col
           AND i.name IS NOT NULL;
        IF @drop_ix <> N'' EXEC(@drop_ix);

        -- Drop the default constraint on the column, if any.
        DECLARE @df SYSNAME = NULL;
        SELECT @df = dc.name
          FROM sys.default_constraints dc
          JOIN sys.columns c ON c.default_object_id = dc.object_id
         WHERE c.object_id = OBJECT_ID(N'[dim].[Kreditor]') AND c.name = @col;
        IF @df IS NOT NULL
        BEGIN
            DECLARE @sql_df NVARCHAR(MAX) = N'ALTER TABLE [dim].[Kreditor] DROP CONSTRAINT ' + QUOTENAME(@df);
            EXEC(@sql_df);
            SET @df = NULL;
        END

        DECLARE @sql_col NVARCHAR(MAX) = N'ALTER TABLE [dim].[Kreditor] DROP COLUMN ' + QUOTENAME(@col);
        EXEC(@sql_col);
        PRINT 'Dropped ' + @col + ' from [dim].[Kreditor]';
    END

    FETCH NEXT FROM col_cur INTO @col;
END
CLOSE col_cur;
DEALLOCATE col_cur;
GO
