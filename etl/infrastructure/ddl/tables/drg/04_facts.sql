-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG: Fact Table
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/NON_FHIR_ELT_Implementeation_Guide.md § DDL Migration Convention
-- ============================================================================

-- fct.DRGKontakt
-- Grain: (leverancetype, takstsystem, kontakt) — every combination of a
-- contact and a takstsystem is its own row, since the same contact is priced
-- under several takstsystemer. The merge key is dw_ek_DRGKontakt (minted on
-- the uuid-based clean_code), so year revisions update in place.
--
-- leverancetype/takstsystem/aktivitetsaar are degenerate slice keys on the
-- fact: they define the grain and the delivery's archive unit, and the
-- control total (fct vs arc) and partition completeness group by them.
-- aktivitetsaar is a SMALLINT column, not a dw_ek_Dato role: the activity
-- year is a categorical slice of the delivery, not a calendar event, and a
-- 1/1-date key would invite date arithmetic that means nothing here.
--
-- tungeste/udfaldsgivende are the delivery's own carrier flags kept as
-- degenerate flags (the forloeb measures lie on the carrying contact).
-- All other descriptives live in dim.DRGKontaktEgenskaber (shared PK).
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND type = 'U')
BEGIN
    CREATE TABLE [fct].[DRGKontakt](
        [dw_ek_DRGKontakt] INT NOT NULL,
        [clean_code_DRGKontakt] VARCHAR(255) NULL,
        [leverancetype] NVARCHAR(10) NULL,
        [takstsystem] NVARCHAR(3) NULL,
        [aktivitetsaar] SMALLINT NULL,
        [dw_ek_DRGKontaktKlassifikation] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_DRGKontaktKlassifikation] DEFAULT -1,
        [dw_ek_DRGDiagnoseGruppe] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_DRGDiagnoseGruppe] DEFAULT -1,
        [dw_ek_DRGProcedureGruppe] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_DRGProcedureGruppe] DEFAULT -1,
        [dw_ek_Borger] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Borger] DEFAULT -1,
        [dw_ek_Organisation_sygehus] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Organisation_sygehus] DEFAULT -1,
        [dw_ek_Organisation_kontaktansvar] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Organisation_kontaktansvar] DEFAULT -1,
        [dw_ek_Organisation_forloebselement] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Organisation_forloebselement] DEFAULT -1,
        [dw_ek_Organisation_henvisende] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Organisation_henvisende] DEFAULT -1,
        [dw_ek_Dato_ind] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Dato_ind] DEFAULT -1,
        [dato_tid_ind] DATETIME2 NULL,
        [dw_ek_Dato_ud] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Dato_ud] DEFAULT -1,
        [dato_tid_ud] DATETIME2 NULL,
        [dw_ek_Dato_henvisning] INT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_ek_Dato_henvisning] DEFAULT -1,
        [pris_drg] DECIMAL(19,4) NULL,
        [langliggerpris] DECIMAL(19,4) NULL,
        [totalpris_drg] DECIMAL(19,4) NULL,
        [langliggerdage] INT NULL,
        [liggedage] INT NULL,
        [kontaktdage] INT NULL,
        [varighed_time] DECIMAL(19,4) NULL,
        [forloeb_totalpris] DECIMAL(19,4) NULL,
        [forloeb_pris_drg] DECIMAL(19,4) NULL,
        [forloeb_langliggerpris] DECIMAL(19,4) NULL,
        [forloeb_loensumafgift_pris] DECIMAL(19,4) NULL,
        [forloeb_loensumafgift_langliggerpris] DECIMAL(19,4) NULL,
        [forloeb_antal_kontakter] INT NULL,
        [forloeb_mrk_naerhed_1] NVARCHAR(6) NULL,
        [forloeb_mrk_naerhed_2] NVARCHAR(6) NULL,
        [forloeb_mrk_naerhed_4] NVARCHAR(6) NULL,
        [forloeb_mrk_kirurgi] NVARCHAR(6) NULL,
        [forloeb_pris_psykiatriforloeb] DECIMAL(19,4) NULL,
        [forloeb_dtd_justeret_takst] DECIMAL(19,4) NULL,
        [forloeb_loensumafgift_pris_psyk] DECIMAL(19,4) NULL,
        [forloeb_liggedage] DECIMAL(19,4) NULL,
        [forloeb_antal_kontakter_psyk] INT NULL,
        [tungeste_drgkontakt] NVARCHAR(6) NULL,
        [udfaldsgivende_kontakt] NVARCHAR(6) NULL,
        [dw_haendelsestidspunkt] DATETIME2 NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_DRGKontakt_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontakt_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontakt_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Fct_DRGKontakt] PRIMARY KEY CLUSTERED ([dw_ek_DRGKontakt] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_clean_code] ON [fct].[DRGKontakt]
    ([clean_code_DRGKontakt] ASC) WHERE [clean_code_DRGKontakt] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Slice] ON [fct].[DRGKontakt]
    ([leverancetype] ASC, [takstsystem] ASC, [aktivitetsaar] ASC) INCLUDE ([totalpris_drg], [dw_is_deleted]) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Borger] ON [fct].[DRGKontakt]
    ([dw_ek_Borger] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Klassifikation] ON [fct].[DRGKontakt]
    ([dw_ek_DRGKontaktKlassifikation] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_DiagnoseGruppe] ON [fct].[DRGKontakt]
    ([dw_ek_DRGDiagnoseGruppe] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_ProcedureGruppe] ON [fct].[DRGKontakt]
    ([dw_ek_DRGProcedureGruppe] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Organisation_sygehus] ON [fct].[DRGKontakt]
    ([dw_ek_Organisation_sygehus] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Organisation_kontaktansvar] ON [fct].[DRGKontakt]
    ([dw_ek_Organisation_kontaktansvar] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Dato_ind] ON [fct].[DRGKontakt]
    ([dw_ek_Dato_ind] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Dato_ud] ON [fct].[DRGKontakt]
    ([dw_ek_Dato_ud] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_Watermark] ON [fct].[DRGKontakt]
    ([dw_timestamp_update] ASC) ON [PRIMARY];
END
GO
