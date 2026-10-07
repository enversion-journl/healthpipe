-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG: Map Tables
-- Source of truth for DW_EK assignment via IDENTITY
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/NON_FHIR_ELT_Implementeation_Guide.md § DDL Migration Convention
--
-- Column sets mirror DSA/SQL/staging/drg_kontakt_kolonner.py; consistency is
-- enforced by tests/unit/test_drg_dwh.py.
-- ============================================================================

-- map.DRGKontakt
-- clean_code = DRGKONTAKT_<leverancetype>_<takstsystem>_<kontakt-uuid>: the
-- stable identity across deliveries (SDS renumbers every integer id per
-- delivery, the LPR3 uuid never changes), so dw_ek_DRGKontakt survives
-- redeliveries by construction. Typed VARCHAR (not NVARCHAR): every part is
-- ASCII (prefix + takstsystem code + LPR3 uuid), so a single-byte type carries
-- it losslessly and halves this clustered key on ~35.6M rows.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGKontakt]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[DRGKontakt](
        [dw_ek_DRGKontakt] INT IDENTITY(1,1) NOT NULL,
        [clean_code_DRGKontakt] VARCHAR(255) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontakt_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontakt_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_DRGKontakt] PRIMARY KEY CLUSTERED ([clean_code_DRGKontakt] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGKontakt]') AND name = 'IX_Map_DRGKontakt_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGKontakt]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_DRGKontakt_DW_EK]
        ON [map].[DRGKontakt] ([dw_ek_DRGKontakt] ASC) INCLUDE ([clean_code_DRGKontakt]) ON [PRIMARY]
    ');
END
GO

-- map.DRGKontaktKlassifikation
-- clean_code = DRGKLASS_<sha256 over the full attribute combination>: a
-- readable concatenation of the 19 key values can exceed the 900-byte
-- clustered-key limit, and the dimension row carries every attribute in
-- readable form. The hash input is length-prefixed per value (injective).
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGKontaktKlassifikation]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[DRGKontaktKlassifikation](
        [dw_ek_DRGKontaktKlassifikation] INT IDENTITY(1,1) NOT NULL,
        [clean_code_DRGKontaktKlassifikation] NVARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontaktKlassifikation_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGKontaktKlassifikation_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_DRGKontaktKlassifikation] PRIMARY KEY CLUSTERED ([clean_code_DRGKontaktKlassifikation] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGKontaktKlassifikation]') AND name = 'IX_Map_DRGKontaktKlassifikation_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGKontaktKlassifikation]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_DRGKontaktKlassifikation_DW_EK]
        ON [map].[DRGKontaktKlassifikation] ([dw_ek_DRGKontaktKlassifikation] ASC) INCLUDE ([clean_code_DRGKontaktKlassifikation]) ON [PRIMARY]
    ');
END
GO

-- map.DRGDiagnose
-- clean_code = DRGDIAG_<sha256 over (diagnose_kode, art, sideangivelse,
-- tillaegskoder)>: the diagnosis-as-recorded, independent of contact.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnose]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[DRGDiagnose](
        [dw_ek_DRGDiagnose] INT IDENTITY(1,1) NOT NULL,
        [clean_code_DRGDiagnose] NVARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnose_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnose_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_DRGDiagnose] PRIMARY KEY CLUSTERED ([clean_code_DRGDiagnose] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnose]') AND name = 'IX_Map_DRGDiagnose_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnose]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_DRGDiagnose_DW_EK]
        ON [map].[DRGDiagnose] ([dw_ek_DRGDiagnose] ASC) INCLUDE ([clean_code_DRGDiagnose]) ON [PRIMARY]
    ');
END
GO

-- map.DRGProcedure
-- clean_code = DRGPROC_<sha256 over (procedure_kode, sideangivelse,
-- handlingsspec, kontrast, tillaegskoder)>: the procedure-as-recorded,
-- independent of contact (timestamps and producing unit are per-registration
-- and live on the bridge, not here).
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGProcedure]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[DRGProcedure](
        [dw_ek_DRGProcedure] INT IDENTITY(1,1) NOT NULL,
        [clean_code_DRGProcedure] NVARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedure_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedure_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_DRGProcedure] PRIMARY KEY CLUSTERED ([clean_code_DRGProcedure] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGProcedure]') AND name = 'IX_Map_DRGProcedure_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGProcedure]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_DRGProcedure_DW_EK]
        ON [map].[DRGProcedure] ([dw_ek_DRGProcedure] ASC) INCLUDE ([clean_code_DRGProcedure]) ON [PRIMARY]
    ');
END
GO

-- map.DRGDiagnoseGruppe
-- clean_code = DRGDIAGGRP_<sha256 over the sorted, distinct set of member
-- clean_code_DRGDiagnose>: interns a contact's DISTINCT set of diagnoses as one
-- group key, so fct.DRGKontakt references the whole set with a single FK and
-- the group->code membership is stored once per set (not per contact).
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnoseGruppe]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[DRGDiagnoseGruppe](
        [dw_ek_DRGDiagnoseGruppe] INT IDENTITY(1,1) NOT NULL,
        [clean_code_DRGDiagnoseGruppe] NVARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnoseGruppe_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnoseGruppe_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_DRGDiagnoseGruppe] PRIMARY KEY CLUSTERED ([clean_code_DRGDiagnoseGruppe] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnoseGruppe]') AND name = 'IX_Map_DRGDiagnoseGruppe_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnoseGruppe]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_DRGDiagnoseGruppe_DW_EK]
        ON [map].[DRGDiagnoseGruppe] ([dw_ek_DRGDiagnoseGruppe] ASC) INCLUDE ([clean_code_DRGDiagnoseGruppe]) ON [PRIMARY]
    ');
END
GO

-- map.DRGProcedureGruppe
-- clean_code = DRGPROCGRP_<sha256 over the sorted, distinct set of
-- (clean_code_DRGProcedure, dw_ek_Organisation_producerende)>: the producing
-- unit is part of the set, so two contacts share a group only when both the
-- codes AND the units match.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGProcedureGruppe]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[DRGProcedureGruppe](
        [dw_ek_DRGProcedureGruppe] INT IDENTITY(1,1) NOT NULL,
        [clean_code_DRGProcedureGruppe] NVARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedureGruppe_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedureGruppe_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Map_DRGProcedureGruppe] PRIMARY KEY CLUSTERED ([clean_code_DRGProcedureGruppe] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGProcedureGruppe]') AND name = 'IX_Map_DRGProcedureGruppe_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGProcedureGruppe]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_DRGProcedureGruppe_DW_EK]
        ON [map].[DRGProcedureGruppe] ([dw_ek_DRGProcedureGruppe] ASC) INCLUDE ([clean_code_DRGProcedureGruppe]) ON [PRIMARY]
    ');
END
GO
