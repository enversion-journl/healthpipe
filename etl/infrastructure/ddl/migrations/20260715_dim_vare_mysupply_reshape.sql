-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- OPS-28148: dim.Vare reshape — MySupply-styret dimension med source-flags.
-- ILS-FK'en (dw_ek_ILSVare) og ILS-attributsaettet udgaar (bor i dim.ILSVare,
-- naas via bridge.VareILSVare). varenavn_kort omdoebes til varenavn_mysupply;
-- source-flags tilfoejes. varenavn saettes af ETL (MySupply-navn eller UNKNOWN).
-- Runs on BOTH staging and DSO. Idempotent: safe to re-run.
-- ============================================================================

-- 1) Rename (old/new-guard)
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'varenavn_kort')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'varenavn_mysupply')
    EXEC sp_rename 'dim.Vare.varenavn_kort', 'varenavn_mysupply', 'COLUMN';
GO

-- 2) Drop index paa dw_ek_ILSVare foer kolonnedrop
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'IX_Dim_Vare_IlsVare')
    DROP INDEX [IX_Dim_Vare_IlsVare] ON [dim].[Vare];
GO

-- 3) Drop kolonner (default-constraints droppes foerst; dynamisk SQL)
DECLARE @col SYSNAME;
DECLARE @cols TABLE (col SYSNAME);
INSERT INTO @cols (col) VALUES
    (N'ils_varenummer'),
    (N'faelles_varenummer'),
    (N'stregkode'),
    (N'stoerrelse_tekst'),
    (N'sammensat_navn'),
    (N'basisenhed'),
    (N'vareart_kode'),
    (N'vareart_navn'),
    (N'varegruppe_kode'),
    (N'varegruppe_navn'),
    (N'varetype'),
    (N'kreditornummer'),
    (N'momskd'),
    (N'afgiftkd'),
    (N'dw_ek_ILSVare'),
    (N'varenavn_ils');

DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR SELECT col FROM @cols;
OPEN col_cur;
FETCH NEXT FROM col_cur INTO @col;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = @col)
    BEGIN
        DECLARE @df SYSNAME = NULL;
        SELECT @df = dc.name
          FROM sys.default_constraints dc
          JOIN sys.columns c ON c.default_object_id = dc.object_id
         WHERE c.object_id = OBJECT_ID(N'[dim].[Vare]') AND c.name = @col;
        IF @df IS NOT NULL
        BEGIN
            DECLARE @sql_df NVARCHAR(MAX) = N'ALTER TABLE [dim].[Vare] DROP CONSTRAINT ' + QUOTENAME(@df);
            EXEC(@sql_df);
            SET @df = NULL;
        END

        DECLARE @sql_col NVARCHAR(MAX) = N'ALTER TABLE [dim].[Vare] DROP COLUMN ' + QUOTENAME(@col);
        EXEC(@sql_col);
        PRINT 'Dropped ' + @col + ' from [dim].[Vare]';
    END
    FETCH NEXT FROM col_cur INTO @col;
END
CLOSE col_cur;
DEALLOCATE col_cur;
GO

-- 4) Add nye kolonner (guarded)
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'dw_source_mysupply')
    ALTER TABLE [dim].[Vare] ADD [dw_source_mysupply] BIT NOT NULL DEFAULT 0;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'dw_source_ils')
    ALTER TABLE [dim].[Vare] ADD [dw_source_ils] BIT NOT NULL DEFAULT 0;
GO

-- 5) Oprydning: staging-tabeller fra det gamle flow (staging er aldrig
--    persistent; producenterne slettes i samme PR)
DROP TABLE IF EXISTS [stg].[pre_Vare_mysupply];
DROP TABLE IF EXISTS [stg].[pre_bridge_vare_ils];
DROP TABLE IF EXISTS [stg].[pre_bridge_vare_ilsvare_ils];
DROP TABLE IF EXISTS [stg].[dim_ILSVare_ils];
GO
