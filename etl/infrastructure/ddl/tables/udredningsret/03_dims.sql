-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Udredningsret: Dimension Tables
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/implementation/sql_etl_guide.md § Migrations
--
-- Attribute column sets mirror DSA/SQL/staging/udredningsret_kolonner.py
-- (EGENSKABER_ATTRIBUTTER, KLASSIFIKATION_ATTRIBUTTER); consistency is
-- enforced by tests/unit/test_udredningsret_dwh.py.
-- ============================================================================

-- dim.UdredningsretEgenskaber
-- Transaction dimension: dw_ek_Udredningsforloeb is the shared PK with
-- fct.Udredningsforloeb (both sourced from map.Udredningsforloeb via the
-- clean_code), so there is exactly one row here per fact row and the fact
-- needs no separate egenskaber key. Carries every descriptive attribute —
-- SDS' own ids (f_id, pk) included, as "last delivered" values — so the fact
-- stays free of text columns. No borger key and no CPR: identity is the
-- fact's side.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[UdredningsretEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[UdredningsretEgenskaber](
        [dw_ek_Udredningsforloeb] INT NOT NULL,
        [clean_code_Udredningsforloeb] VARCHAR(100) NULL,
        [f_id] NVARCHAR(500) NULL,
        [pk] NVARCHAR(20) NULL,
        [start_markoer_kode] NVARCHAR(20) NULL,
        [start_markoer_uuid] NVARCHAR(36) NULL,
        [slut_markoer_kode] NVARCHAR(20) NULL,
        [slut_markoer_uuid] NVARCHAR(36) NULL,
        [seneste_markoer_dato] NVARCHAR(10) NULL,
        [foerste_forloebselement_uuid] NVARCHAR(36) NULL,
        [helbredsforloeb_id] NVARCHAR(50) NULL,
        [forloeb_enhed_sor] NVARCHAR(25) NULL,
        [kontakt_enhed_sor] NVARCHAR(25) NULL,
        [aktionsdiagnose_kode] NVARCHAR(20) NULL,
        [diagnosekontakt_uuid] NVARCHAR(36) NULL,
        [opgoerelseskvartal] NVARCHAR(10) NULL,
        [opgoerelsesmaaned] NVARCHAR(10) NULL,
        [sygehusregion] NVARCHAR(10) NULL,
        [bopaelsregion] NVARCHAR(10) NULL,
        [speciale] NVARCHAR(100) NULL,
        [forloebslabel] NVARCHAR(20) NULL,
        [mrk_l_n] NVARCHAR(5) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretEgenskaber_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretEgenskaber_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Dim_UdredningsretEgenskaber] PRIMARY KEY CLUSTERED ([dw_ek_Udredningsforloeb] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_UdredningsretEgenskaber_clean_code] ON [dim].[UdredningsretEgenskaber]
    ([clean_code_Udredningsforloeb] ASC) WHERE [clean_code_Udredningsforloeb] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Dim_UdredningsretEgenskaber_Watermark] ON [dim].[UdredningsretEgenskaber]
    ([dw_timestamp_update] ASC) ON [PRIMARY];
END
GO

-- dim.UdredningsretKlassifikation
-- The primary analysis dimension: one member per combination of the eight
-- indicator attributes (2,948 measured on dev). Every raw value is kept beside its
-- Danish text, so a report reads the text and a check reads the code. The
-- texts are rendered from the dicts in udredningsret_kolonner.py, except
-- forloebslabel_tekst, which is codesystem.SKSConcept.display for the ALAL
-- codes and is refreshed globally after each load (as DRG's drg_beskrivelse).
-- speciale is free text from the delivery and has no text column.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[UdredningsretKlassifikation]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[UdredningsretKlassifikation](
        [dw_ek_UdredningsretKlassifikation] INT NOT NULL,
        [clean_code_UdredningsretKlassifikation] NVARCHAR(100) NULL,
        [overholdt] NVARCHAR(5) NULL,
        [overholdt_tekst] NVARCHAR(50) NULL,
        [aarsag] NVARCHAR(5) NULL,
        [aarsag_tekst] NVARCHAR(200) NULL,
        [inden_30_dage] NVARCHAR(5) NULL,
        [inden_30_dage_tekst] NVARCHAR(50) NULL,
        [psykiatri] NVARCHAR(5) NULL,
        [psykiatri_tekst] NVARCHAR(50) NULL,
        [hjerteomraade] NVARCHAR(5) NULL,
        [hjerteomraade_tekst] NVARCHAR(50) NULL,
        [forloebslabel] NVARCHAR(20) NULL,
        [forloebslabel_tekst] NVARCHAR(500) NULL,
        [bopaelsregion_forloeb] NVARCHAR(10) NULL,
        [bopaelsregion_forloeb_tekst] NVARCHAR(50) NULL,
        [speciale] NVARCHAR(100) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretKlassifikation_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretKlassifikation_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Dim_UdredningsretKlassifikation] PRIMARY KEY CLUSTERED ([dw_ek_UdredningsretKlassifikation] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_UdredningsretKlassifikation_clean_code] ON [dim].[UdredningsretKlassifikation]
    ([clean_code_UdredningsretKlassifikation] ASC) WHERE [clean_code_UdredningsretKlassifikation] IS NOT NULL ON [PRIMARY];
END
GO

-- dim.Forloebsmarkoer
-- Shared code dimension: one member per distinct marker code in the archive
-- (433 measured), independent of forloeb and helbredsforloeb. tekst comes
-- from codesystem.SKSConcept; a code without a catalogue row is still a
-- normal member with tekst 'Ukendt' and tekst_kilde 'ukendt' — never a hard
-- fail. gruppe/gruppe_tekst are SDS' appendix-1 grouping of the codes
-- (MARKOER_GRUPPER), with 'oevrige' for anything outside the seven groups.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Forloebsmarkoer]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Forloebsmarkoer](
        [dw_ek_Forloebsmarkoer] INT NOT NULL,
        [clean_code_Forloebsmarkoer] VARCHAR(50) NULL,
        [kode] NVARCHAR(20) NULL,
        [tekst] NVARCHAR(500) NULL,
        [gruppe] NVARCHAR(40) NULL,
        [gruppe_tekst] NVARCHAR(100) NULL,
        [tekst_kilde] NVARCHAR(30) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Forloebsmarkoer_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Forloebsmarkoer_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Dim_Forloebsmarkoer] PRIMARY KEY CLUSTERED ([dw_ek_Forloebsmarkoer] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Forloebsmarkoer_clean_code] ON [dim].[Forloebsmarkoer]
    ([clean_code_Forloebsmarkoer] ASC) WHERE [clean_code_Forloebsmarkoer] IS NOT NULL ON [PRIMARY];
END
GO
