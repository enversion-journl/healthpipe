-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG: Dimension Tables
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/NON_FHIR_ELT_Implementeation_Guide.md § DDL Migration Convention
--
-- Attribute column sets mirror DSA/SQL/staging/drg_kontakt_kolonner.py;
-- consistency is enforced by tests/unit/test_drg_dwh.py.
-- ============================================================================

-- dim.DRGKontaktEgenskaber
-- Transaction dimension: dw_ek_DRGKontakt is the shared PK with
-- fct.DRGKontakt (both sourced from map.DRGKontakt via clean_code).
-- Carries every descriptive attribute — low-frequency and free-text values
-- included — so the fact stays free of text columns. The delivery's own
-- demographics are authoritative for the settlement; dim.Borger's address
-- history is deliberately NOT a substitute.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGKontaktEgenskaber]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[DRGKontaktEgenskaber](
        [dw_ek_DRGKontakt] INT NOT NULL,
        [clean_code_DRGKontakt] VARCHAR(255) NULL,
        [leverancetype] NVARCHAR(10) NULL,
        [takstsystem] NVARCHAR(3) NULL,
        [aktivitetsaar] NVARCHAR(6) NULL,
        [forloeb_noegle] NVARCHAR(36) NULL,
        [tungeste_drgkontakt] NVARCHAR(6) NULL,
        [udfaldsgivende_kontakt] NVARCHAR(6) NULL,
        [kontakt_afsluttet] NVARCHAR(6) NULL,
        [drg_kode] NVARCHAR(10) NULL,
        [drg_grouper] NVARCHAR(10) NULL,
        [mdc] NVARCHAR(10) NULL,
        [regel] NVARCHAR(10) NULL,
        [rtc] NVARCHAR(10) NULL,
        [d90ua05] NVARCHAR(10) NULL,
        [trimpunkt] NVARCHAR(10) NULL,
        [kontakttype] NVARCHAR(100) NULL,
        [drgkontakttype] NVARCHAR(100) NULL,
        [prioritet] NVARCHAR(100) NULL,
        [kontaktaarsag] NVARCHAR(100) NULL,
        [afslutningsmaade] NVARCHAR(100) NULL,
        [henvisningsmaade] NVARCHAR(100) NULL,
        [henvisningsaarsag] NVARCHAR(100) NULL,
        [speciale] NVARCHAR(480) NULL,
        [sygehusnavn] NVARCHAR(510) NULL,
        [sygehustype] NVARCHAR(10) NULL,
        [sygehus_region] NVARCHAR(20) NULL,
        [kontaktansvar_enhedsnavn] NVARCHAR(510) NULL,
        [betalingsaftale] NVARCHAR(100) NULL,
        [betaler] NVARCHAR(100) NULL,
        [specialiseringsniveau] NVARCHAR(100) NULL,
        [bo_kom] NVARCHAR(20) NULL,
        [bo_region] NVARCHAR(20) NULL,
        [bo_kom_kilde] NVARCHAR(10) NULL,
        [betaler_kom] NVARCHAR(20) NULL,
        [betaler_region] NVARCHAR(20) NULL,
        [alder_aar] NVARCHAR(11) NULL,
        [koen] NVARCHAR(1) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontaktEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontaktEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_DRGKontaktEgenskaber] PRIMARY KEY CLUSTERED ([dw_ek_DRGKontakt] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_DRGKontaktEgenskaber_clean_code] ON [dim].[DRGKontaktEgenskaber]
    ([clean_code_DRGKontakt] ASC) WHERE [clean_code_DRGKontakt] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Dim_DRGKontaktEgenskaber_Watermark] ON [dim].[DRGKontaktEgenskaber]
    ([dw_timestamp_update] ASC) ON [PRIMARY];
END
GO

-- dim.DRGKontaktKlassifikation
-- The primary analysis dimension: one member per combination of the
-- classification attributes (leverancetype + takstsystem + 17 attributes),
-- ~131.500 members per takstsystem. Beskrivelse, trimpunkt and standardtakst
-- are functionally dependent on (takstsystem, drg_kode) and denormalised on
-- without adding cardinality. Psyk contacts join the same dimension with
-- empty grouper fields.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGKontaktKlassifikation]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[DRGKontaktKlassifikation](
        [dw_ek_DRGKontaktKlassifikation] INT NOT NULL,
        [clean_code_DRGKontaktKlassifikation] NVARCHAR(100) NULL,
        [leverancetype] NVARCHAR(10) NULL,
        [takstsystem] NVARCHAR(3) NULL,
        [drg_kode] NVARCHAR(10) NULL,
        [mdc] NVARCHAR(10) NULL,
        [drg_grouper] NVARCHAR(10) NULL,
        [regel] NVARCHAR(10) NULL,
        [rtc] NVARCHAR(10) NULL,
        [d90ua05] NVARCHAR(10) NULL,
        [drgkontakttype] NVARCHAR(100) NULL,
        [kontakttype] NVARCHAR(100) NULL,
        [prioritet] NVARCHAR(100) NULL,
        [kontaktaarsag] NVARCHAR(100) NULL,
        [afslutningsmaade] NVARCHAR(100) NULL,
        [henvisningsmaade] NVARCHAR(100) NULL,
        [henvisningsaarsag] NVARCHAR(100) NULL,
        [betalingsaftale] NVARCHAR(100) NULL,
        [betaler] NVARCHAR(100) NULL,
        [specialiseringsniveau] NVARCHAR(100) NULL,
        [sygehustype] NVARCHAR(10) NULL,
        [drg_beskrivelse] NVARCHAR(500) NULL,
        [trimpunkt] NVARCHAR(10) NULL,
        [standardtakst] DECIMAL(19,4) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontaktKlassifikation_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontaktKlassifikation_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_DRGKontaktKlassifikation] PRIMARY KEY CLUSTERED ([dw_ek_DRGKontaktKlassifikation] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_DRGKontaktKlassifikation_clean_code] ON [dim].[DRGKontaktKlassifikation]
    ([clean_code_DRGKontaktKlassifikation] ASC) WHERE [clean_code_DRGKontaktKlassifikation] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Dim_DRGKontaktKlassifikation_DRG] ON [dim].[DRGKontaktKlassifikation]
    ([takstsystem] ASC, [drg_kode] ASC) ON [PRIMARY];
END
GO

-- dim.DRGDiagnose
-- Code dimension: one member per (diagnose_kode, art, sideangivelse,
-- tillaegskoder), independent of contact. Attributes are functionally
-- determined by the clean_code (the key IS their hash), so the dimension is
-- append-only — a member's attributes never change. Delt PK med map.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGDiagnose]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[DRGDiagnose](
        [dw_ek_DRGDiagnose] INT NOT NULL,
        [clean_code_DRGDiagnose] NVARCHAR(100) NULL,
        [diagnose_kode] NVARCHAR(20) NULL,
        [art] NVARCHAR(20) NULL,
        [sideangivelse] NVARCHAR(20) NULL,
        [tillaegskoder] NVARCHAR(MAX) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnose_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnose_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_DRGDiagnose] PRIMARY KEY CLUSTERED ([dw_ek_DRGDiagnose] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_DRGDiagnose_clean_code] ON [dim].[DRGDiagnose]
    ([clean_code_DRGDiagnose] ASC) WHERE [clean_code_DRGDiagnose] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Dim_DRGDiagnose_kode] ON [dim].[DRGDiagnose]
    ([diagnose_kode] ASC) ON [PRIMARY];
END
GO

-- dim.DRGProcedure
-- Code dimension: one member per (procedure_kode, sideangivelse,
-- handlingsspec, kontrast, tillaegskoder), independent of contact. No
-- timestamps and no producing unit (those are per-registration on the
-- bridge). Append-only, same rationale as dim.DRGDiagnose.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGProcedure]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[DRGProcedure](
        [dw_ek_DRGProcedure] INT NOT NULL,
        [clean_code_DRGProcedure] NVARCHAR(100) NULL,
        [procedure_kode] NVARCHAR(20) NULL,
        [sideangivelse] NVARCHAR(20) NULL,
        [handlingsspec] NVARCHAR(100) NULL,
        [kontrast] NVARCHAR(100) NULL,
        [tillaegskoder] NVARCHAR(MAX) NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedure_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedure_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Dim_DRGProcedure] PRIMARY KEY CLUSTERED ([dw_ek_DRGProcedure] ASC)
    ) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_DRGProcedure_clean_code] ON [dim].[DRGProcedure]
    ([clean_code_DRGProcedure] ASC) WHERE [clean_code_DRGProcedure] IS NOT NULL ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Dim_DRGProcedure_kode] ON [dim].[DRGProcedure]
    ([procedure_kode] ASC) ON [PRIMARY];
END
GO
