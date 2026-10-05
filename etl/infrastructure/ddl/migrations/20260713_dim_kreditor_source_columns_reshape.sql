-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Reshape dim.Kreditor per-source attribute columns.
--
-- WHY: the per-source columns were inconsistent — only ILS had per-source
--   attribute columns, prefix-named (ils_kreditornavn, ils_adresse1, ...),
--   while the coalesced business columns (kreditornavn, vejnavn, bynavn,
--   postnummer) draw from up to three systems. New convention:
--     * business column = COALESCE across sources (unchanged names)
--     * per-source columns = postfix-named (<attribut>_<system>) for every
--       contributing source
--     * dw_source_<system> BIT indicators (pattern from dim.UNSPSC) replace
--       the redundant cvrnr_sydax/cvrnr_ils/cvrnr_mysupply columns (the CVR
--       IS the grain — presence flags carry the same information)
--
-- Renames (data preserved):
--   ils_kreditornavn -> kreditornavn_ils
--   ils_adresse1     -> vejnavn_ils
--   ils_adresse2     -> bynavn_ils
--   ils_postdistrikt -> postnummer_ils
--   ils_kontakt      -> kontakt
-- Drops:
--   ils_bynavn (derived duplicate of bynavn_ils), cvrnr_sydax, cvrnr_ils,
--   cvrnr_mysupply
-- Adds:
--   kreditornavn_sydax, kreditornavn_mysupply, vejnavn_mysupply,
--   bynavn_mysupply, postnummer_mysupply,
--   dw_source_sydax, dw_source_ils, dw_source_mysupply
--
-- Runs on BOTH staging and DSO (the DSO sync reflects source columns).
-- Every step is guarded; safe to re-run.
-- ============================================================================

IF OBJECT_ID(N'[dim].[Kreditor]') IS NOT NULL
BEGIN
    -- ------------------------------------------------------------------
    -- 1) Renames (guarded: old exists AND new does not)
    -- ------------------------------------------------------------------
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'ils_kreditornavn')
       AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'kreditornavn_ils')
        EXEC sp_rename 'dim.Kreditor.ils_kreditornavn', 'kreditornavn_ils', 'COLUMN';

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'ils_adresse1')
       AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'vejnavn_ils')
        EXEC sp_rename 'dim.Kreditor.ils_adresse1', 'vejnavn_ils', 'COLUMN';

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'ils_adresse2')
       AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'bynavn_ils')
        EXEC sp_rename 'dim.Kreditor.ils_adresse2', 'bynavn_ils', 'COLUMN';

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'ils_postdistrikt')
       AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'postnummer_ils')
        EXEC sp_rename 'dim.Kreditor.ils_postdistrikt', 'postnummer_ils', 'COLUMN';

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'ils_kontakt')
       AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'kontakt')
        EXEC sp_rename 'dim.Kreditor.ils_kontakt', 'kontakt', 'COLUMN';

    -- ------------------------------------------------------------------
    -- 2) Drops (guarded; default constraints dropped first)
    -- ------------------------------------------------------------------
    DECLARE @col SYSNAME;
    DECLARE @cols TABLE (col SYSNAME);
    INSERT INTO @cols (col) VALUES
        (N'ils_bynavn'),
        (N'cvrnr_sydax'),
        (N'cvrnr_ils'),
        (N'cvrnr_mysupply');

    DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR SELECT col FROM @cols;
    OPEN col_cur;
    FETCH NEXT FROM col_cur INTO @col;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.columns
                   WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = @col)
        BEGIN
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

    -- ------------------------------------------------------------------
    -- 3) Adds (guarded)
    -- ------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'kreditornavn_sydax')
        ALTER TABLE [dim].[Kreditor] ADD [kreditornavn_sydax] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'kreditornavn_mysupply')
        ALTER TABLE [dim].[Kreditor] ADD [kreditornavn_mysupply] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'vejnavn_mysupply')
        ALTER TABLE [dim].[Kreditor] ADD [vejnavn_mysupply] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'bynavn_mysupply')
        ALTER TABLE [dim].[Kreditor] ADD [bynavn_mysupply] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'postnummer_mysupply')
        ALTER TABLE [dim].[Kreditor] ADD [postnummer_mysupply] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'dw_source_sydax')
        ALTER TABLE [dim].[Kreditor] ADD [dw_source_sydax] BIT NOT NULL DEFAULT 0;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'dw_source_ils')
        ALTER TABLE [dim].[Kreditor] ADD [dw_source_ils] BIT NOT NULL DEFAULT 0;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = 'dw_source_mysupply')
        ALTER TABLE [dim].[Kreditor] ADD [dw_source_mysupply] BIT NOT NULL DEFAULT 0;
END
GO
