-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Postering descriptor cleanup
--
-- dim.Postering:  add DokumentId (BIGINT NULL) — descriptor moves to the 1:1 dim
--                 (BIGINT matches the SydAX source Kreditortransaktion.DokumentId;
--                  an earlier INT version is widened in place below)
-- fct.Postering:  drop 8 descriptive columns now owned exclusively by
--                 dim.Postering: SidstUdlignetBilagsnummer, Bilagsnummer_Trans,
--                 Bilagsnummer_Postering, DokumentId, Valutakode_Trans,
--                 Valutakode_Postering, Transaktionstekst, Posteringstekst.
--                 FakturaNr stays on the fact (degenerate dimension — the
--                 ref.Faktura_To_Postering bridge and the IX_Fct_Postering_*
--                 indexes read it from fct.Postering).
--
-- Runs on BOTH staging and DSO (staging_migrations / dso_migrations apply the
-- same directory). Idempotent: all operations guarded.
--
-- The REBUILD reclaims LOB space left behind by the dropped NVARCHAR(MAX)
-- columns; it only fires when at least one column was actually dropped, so
-- re-runs are cheap. Expect a long-running batch on ~60M rows.
-- ============================================================================

-- ---------- dim.Postering: add DokumentId (BIGINT) ----------
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = 'DokumentId')
BEGIN
    ALTER TABLE [dim].[Postering] ADD [DokumentId] BIGINT NULL;
    PRINT 'Added dim.Postering.DokumentId';
END
GO

-- Widen DokumentId to BIGINT if an earlier run of this migration added it as INT.
-- The SydAX source (Kreditor.Kreditortransaktion.DokumentId) is bigint; an INT
-- column would silently NULL values > 2,147,483,647 via TRY_CONVERT(INT,...).
IF EXISTS (
    SELECT 1
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dim].[Postering]')
      AND c.name = 'DokumentId'
      AND t.name = 'int'
)
BEGIN
    ALTER TABLE [dim].[Postering] ALTER COLUMN [DokumentId] BIGINT NULL;
    PRINT 'Widened dim.Postering.DokumentId from INT to BIGINT';
END
GO

-- ---------- fct.Postering: drop descriptor columns + reclaim space ----------
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
BEGIN
    DECLARE @dropped INT = 0;

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'SidstUdlignetBilagsnummer')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [SidstUdlignetBilagsnummer];
        SET @dropped += 1;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'Bilagsnummer_Trans')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [Bilagsnummer_Trans];
        SET @dropped += 1;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'Bilagsnummer_Postering')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [Bilagsnummer_Postering];
        SET @dropped += 1;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DokumentId')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [DokumentId];
        SET @dropped += 1;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'Valutakode_Trans')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [Valutakode_Trans];
        SET @dropped += 1;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'Valutakode_Postering')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [Valutakode_Postering];
        SET @dropped += 1;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'Transaktionstekst')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [Transaktionstekst];
        SET @dropped += 1;
    END

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'Posteringstekst')
    BEGIN
        ALTER TABLE [fct].[Postering] DROP COLUMN [Posteringstekst];
        SET @dropped += 1;
    END

    IF @dropped > 0
    BEGIN
        PRINT CONCAT('Dropped ', @dropped, ' descriptor columns from fct.Postering - rebuilding to reclaim LOB space');
        ALTER TABLE [fct].[Postering] REBUILD;
        PRINT 'fct.Postering rebuilt';
    END
    ELSE
        PRINT 'fct.Postering descriptor columns already dropped - nothing to do';
END
GO
