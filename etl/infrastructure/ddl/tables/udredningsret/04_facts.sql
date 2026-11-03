-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Udredningsret: Fact Tables
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/implementation/sql_etl_guide.md § Migrations
-- ============================================================================

-- fct.Udredningsforloeb
-- Grain: one row per (helbredsforloeb, udredningsforloeb) — the forloeb built
-- in staging from the live markers of one helbredsforloeb, keyed by
-- dw_ek_Udredningsforloeb from map.Udredningsforloeb, so a redelivery that
-- renumbers SDS' F_ID updates the same row.
--
-- There is NO dw_ek_UdredningsretEgenskaber column: the transaction dimension
-- dim.UdredningsretEgenskaber shares this PK (as fct.DRGKontakt and
-- dim.DRGKontaktEgenskaber do), so every descriptive attribute joins on the
-- fact's own key.
--
-- dw_ek_Helbredsforloeb is a grouping key minted in map.Helbredsforloeb and
-- has no dimension of its own; it scopes the retirement of forloeb that
-- vanish from a delivery (spec section 6). helbredsforloeb_id is SDS' own
-- surrogate, kept as a degenerate "as last delivered" attribute — it is
-- renumbered at the source and must never be used as a key.
--
-- indgaar_i_indikator is the delivery's own indicator membership (a forloeb
-- with both a henvist and a non-henvist MRK_H marker); udredningstid_dage is
-- only carried for those rows (decision U2), so the measure can be summed
-- without filtering twice.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Udredningsforloeb]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[Udredningsforloeb](
        [dw_ek_Udredningsforloeb] INT NOT NULL,
        [clean_code_Udredningsforloeb] VARCHAR(100) NULL,
        [dw_ek_Helbredsforloeb] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_Helbredsforloeb] DEFAULT -1,
        [dw_ek_Borger] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_Borger] DEFAULT -1,
        [dw_ek_Organisation_forloeb] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_Organisation_forloeb] DEFAULT -1,
        [dw_ek_Organisation_kontakt] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_Organisation_kontakt] DEFAULT -1,
        [dw_ek_UdredningsretKlassifikation] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_UdredningsretKlassifikation] DEFAULT -1,
        [dw_ek_Dato_start] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_Dato_start] DEFAULT -1,
        [dw_ek_Dato_slut] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_Dato_slut] DEFAULT -1,
        [dw_ek_Dato_opgoerelse] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_ek_Dato_opgoerelse] DEFAULT -1,
        [udredningstid_dage] INT NULL,
        [indgaar_i_indikator] BIT NOT NULL CONSTRAINT [DF_Udredningsforloeb_indgaar_i_indikator] DEFAULT 0,
        [overholdt] BIT NULL,
        [inden_30_dage] BIT NULL,
        [alder_ved_henvisning] SMALLINT NULL,
        [aktionsdiagnose_kode] NVARCHAR(20) NULL,
        [diagnosekontakt_uuid] NVARCHAR(36) NULL,
        [helbredsforloeb_id] NVARCHAR(50) NULL,
        [antal_markoerer] INT NULL,
        [antal_udredningsplaner] INT NULL,
        [antal_afbrydelser] INT NULL,
        [dw_haendelsestidspunkt] DATETIME2 NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Fct_Udredningsforloeb] PRIMARY KEY CLUSTERED ([dw_ek_Udredningsforloeb] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_clean_code] ON [fct].[Udredningsforloeb]
    ([clean_code_Udredningsforloeb] ASC) WHERE [clean_code_Udredningsforloeb] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_Borger] ON [fct].[Udredningsforloeb]
    ([dw_ek_Borger] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_Helbredsforloeb] ON [fct].[Udredningsforloeb]
    ([dw_ek_Helbredsforloeb] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_Klassifikation] ON [fct].[Udredningsforloeb]
    ([dw_ek_UdredningsretKlassifikation] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_Organisation_Forloeb] ON [fct].[Udredningsforloeb]
    ([dw_ek_Organisation_forloeb] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_Dato_Start] ON [fct].[Udredningsforloeb]
    ([dw_ek_Dato_start] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_Dato_Opgoerelse] ON [fct].[Udredningsforloeb]
    ([dw_ek_Dato_opgoerelse] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_Watermark] ON [fct].[Udredningsforloeb]
    ([dw_timestamp_update] ASC) ON [PRIMARY];
END
GO

-- fct.UdredningsretMarkoer
-- Grain: one row per delivered marker — the archive's own merge key
-- (markoer_noegle, loebenummer) carried through staging as
-- clean_code_UdredningsretMarkoer, so the fact row and the archive row are 1:1
-- and the archive's soft delete carries straight through to dw_is_deleted.
--
-- The marker is the child event of fct.Udredningsforloeb, and
-- dw_ek_Udredningsforloeb is the parent key resolved through
-- map.Udredningsforloeb. A staged (non-NULL) parent key without a map member
-- fails the load rather than landing silently on the sentinel, so the fact
-- cannot hold an orphan. -1 means the marker carries no F_ID at all and
-- therefore belongs to no forloeb (1,168,808 rows measured on the delivery of
-- 2026-09-07) — that is design, not a failed lookup (decision U5), and the DQA
-- sentinel cap for the role is set with headroom.
--
-- dw_ek_Helbredsforloeb is a grouping key minted in map.Helbredsforloeb and has
-- no dimension of its own; helbredsforloeb_id is SDS' own surrogate, kept as a
-- degenerate "as last delivered" attribute — it is renumbered at the source and
-- must never be used as a key.
--
-- markoer_kode and markoer_dato are staged but deliberately NOT carried: the
-- code is dim.Forloebsmarkoer via dw_ek_Forloebsmarkoer and the date is
-- dw_ek_Dato_markoer plus dw_haendelsestidspunkt. No degenerate copies.
--
-- antal_kilderaekker is the number of fully identical source rows the archive
-- collapsed into this one, so SUM(antal_kilderaekker) over the live fact rows
-- reproduces the delivery's own row count — the load's control total.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[UdredningsretMarkoer]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[UdredningsretMarkoer](
        [dw_ek_UdredningsretMarkoer] INT NOT NULL,
        [clean_code_UdredningsretMarkoer] VARCHAR(100) NULL,
        [dw_ek_Udredningsforloeb] INT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_ek_Udredningsforloeb] DEFAULT -1,
        [dw_ek_Helbredsforloeb] INT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_ek_Helbredsforloeb] DEFAULT -1,
        [dw_ek_Forloebsmarkoer] INT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_ek_Forloebsmarkoer] DEFAULT -1,
        [dw_ek_Borger] INT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_ek_Borger] DEFAULT -1,
        [dw_ek_Organisation_forloeb] INT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_ek_Organisation_forloeb] DEFAULT -1,
        [dw_ek_Dato_markoer] INT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_ek_Dato_markoer] DEFAULT -1,
        [helbredsforloeb_id] NVARCHAR(50) NULL,
        [forloebselement_uuid] NVARCHAR(36) NULL,
        [markoer_uuid] NVARCHAR(36) NULL,
        [indgaar_i_indikator_H] BIT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_indgaar_i_indikator_H] DEFAULT 0,
        [indgaar_i_indikator_L_N] BIT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_indgaar_i_indikator_L_N] DEFAULT 0,
        [sygehusregion] NVARCHAR(10) NULL,
        [bopaelsregion] NVARCHAR(10) NULL,
        [antal_kilderaekker] INT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_antal_kilderaekker] DEFAULT 1,
        [dw_haendelsestidspunkt] DATETIME2 NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Fct_UdredningsretMarkoer] PRIMARY KEY CLUSTERED ([dw_ek_UdredningsretMarkoer] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_UdredningsretMarkoer_clean_code] ON [fct].[UdredningsretMarkoer]
    ([clean_code_UdredningsretMarkoer] ASC) WHERE [clean_code_UdredningsretMarkoer] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_UdredningsretMarkoer_Udredningsforloeb] ON [fct].[UdredningsretMarkoer]
    ([dw_ek_Udredningsforloeb] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_UdredningsretMarkoer_Helbredsforloeb] ON [fct].[UdredningsretMarkoer]
    ([dw_ek_Helbredsforloeb] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_UdredningsretMarkoer_Forloebsmarkoer] ON [fct].[UdredningsretMarkoer]
    ([dw_ek_Forloebsmarkoer] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_UdredningsretMarkoer_Borger] ON [fct].[UdredningsretMarkoer]
    ([dw_ek_Borger] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_UdredningsretMarkoer_Dato_Markoer] ON [fct].[UdredningsretMarkoer]
    ([dw_ek_Dato_markoer] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_UdredningsretMarkoer_Watermark] ON [fct].[UdredningsretMarkoer]
    ([dw_timestamp_update] ASC) ON [PRIMARY];
END
GO
