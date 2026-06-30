-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: (Re)add the VAT-valuta / zero-rated coverage columns on
--            fct.Fakturalinje.
--
-- WHY: 20260601_faktura_add_full_coverage_columns.sql was rewritten in place
--   after it had already been recorded in [meta].[migration_history] on some
--   servers. Its fct.Fakturalinje block originally added a different column set
--   (MomsBeloeb, MomsGrundlag, Valutakode_Moms, FakturaTotalBeloeb, ...); the
--   rewrite replaced them with the columns below. Because the filename was
--   already tracked, staging_migrations SKIPS the rewritten file, so these
--   columns never get added — and FctUpd_fakturalinje.py's MERGE then fails
--   with "Invalid column name 'Valutakode_MomsGrundlag'".
--
--   This is a NEW filename, so it runs once on every server (including those
--   that already applied 20260601). Must match the columns written by
--   FctUpd_fakturalinje.py's MERGE and defined in 03_facts.sql.
--
--   The dim.FakturaEgenskaber / dim.Fakturalinje columns from 20260601 are
--   unchanged between the two versions, so they are NOT re-issued here.
--
-- Idempotent: every ADD is guarded by a sys.columns check.
-- ============================================================================

DECLARE @tbl_fct INT = OBJECT_ID(N'[fct].[Fakturalinje]');

IF @tbl_fct IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_MomsGrundlag')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_MomsGrundlag] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_MomsBeloeb')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_MomsBeloeb] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'VAT_AfgiftsKategori')
        ALTER TABLE [fct].[Fakturalinje] ADD [VAT_AfgiftsKategori] NVARCHAR(50) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'ZeroRated_MomsGrundlag')
        ALTER TABLE [fct].[Fakturalinje] ADD [ZeroRated_MomsGrundlag] DECIMAL(18, 4) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_ZeroRated_MomsGrundlag')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_ZeroRated_MomsGrundlag] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'ZeroRated_MomsBeloeb')
        ALTER TABLE [fct].[Fakturalinje] ADD [ZeroRated_MomsBeloeb] DECIMAL(18, 4) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_ZeroRated_MomsBeloeb')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_ZeroRated_MomsBeloeb] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_LinjeTotal')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_LinjeTotal] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_FakturaTotal')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_FakturaTotal] NVARCHAR(10) NULL;
END
GO
