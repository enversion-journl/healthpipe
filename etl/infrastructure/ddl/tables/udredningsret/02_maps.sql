-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Udredningsret: Map Tables
-- Source of truth for DW_EK assignment via IDENTITY
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- See: docs/implementation/sql_etl_guide.md § Migrations
--
-- Column sets mirror DSA/SQL/staging/udredningsret_kolonner.py; consistency is
-- enforced by tests/unit/test_udredningsret_dwh.py.
-- ============================================================================

-- map.Helbredsforloeb
-- clean_code = UDREDHF_<helbredsforloeb_noegle>: the archive's own SHA2_256
-- key over (CPR, the first marker's code/date/unit/forloebselement uuid), so
-- the grouping survives SDS' renumbering of DW_EK_HelbredsForloeb. Typed
-- VARCHAR (not NVARCHAR): prefix + hex digest is ASCII, always 72 characters,
-- so a single-byte type carries it losslessly and halves this clustered key.
-- The helbredsforloeb has NO dimension — the key only groups udredningsforloeb
-- and scopes the retirement of vanished ones (spec section 6).
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Helbredsforloeb]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Helbredsforloeb](
        [dw_ek_Helbredsforloeb] INT IDENTITY(1,1) NOT NULL,
        [clean_code_Helbredsforloeb] VARCHAR(80) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Helbredsforloeb_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Helbredsforloeb_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Map_Helbredsforloeb] PRIMARY KEY CLUSTERED ([clean_code_Helbredsforloeb] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Helbredsforloeb]') AND name = 'IX_Map_Helbredsforloeb_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Helbredsforloeb]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Helbredsforloeb_DW_EK]
        ON [map].[Helbredsforloeb] ([dw_ek_Helbredsforloeb] ASC) INCLUDE ([clean_code_Helbredsforloeb]) ON [PRIMARY]
    ');
END
GO

-- map.Udredningsforloeb
-- clean_code = UDREDFORL_<helbredsforloeb_noegle>_<startdato yyyymmdd>_<loebenummer>:
-- readable rather than hashed (<= 90 ASCII characters). loebenummer is OUR
-- per-(helbredsforloeb, startdato) sequence ordered by the forloebselement
-- uuid — never SDS' F_ID/PK, which are renumbered per delivery — so the key
-- survives a redelivery by construction. Shared by fct.Udredningsforloeb and
-- dim.UdredningsretEgenskaber (shared PK).
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Udredningsforloeb]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Udredningsforloeb](
        [dw_ek_Udredningsforloeb] INT IDENTITY(1,1) NOT NULL,
        [clean_code_Udredningsforloeb] VARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Map_Udredningsforloeb] PRIMARY KEY CLUSTERED ([clean_code_Udredningsforloeb] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Udredningsforloeb]') AND name = 'IX_Map_Udredningsforloeb_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Udredningsforloeb]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Udredningsforloeb_DW_EK]
        ON [map].[Udredningsforloeb] ([dw_ek_Udredningsforloeb] ASC) INCLUDE ([clean_code_Udredningsforloeb]) ON [PRIMARY]
    ');
END
GO

-- map.UdredningsretKlassifikation
-- clean_code = UDREDKLASS_<sha256 over the eight indicator attributes>: a
-- readable concatenation of overholdt, aarsag, inden_30_dage, psykiatri,
-- hjerteomraade, forloebslabel, bopaelsregion_forloeb and speciale would carry
-- free text, so the combination is hashed and the dimension row keeps every
-- attribute in readable form. The hash input is length-prefixed per value
-- (injective), exactly as DRG's classification key. 2,948 combinations
-- measured 2026-09-12.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UdredningsretKlassifikation]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[UdredningsretKlassifikation](
        [dw_ek_UdredningsretKlassifikation] INT IDENTITY(1,1) NOT NULL,
        [clean_code_UdredningsretKlassifikation] NVARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretKlassifikation_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretKlassifikation_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Map_UdredningsretKlassifikation] PRIMARY KEY CLUSTERED ([clean_code_UdredningsretKlassifikation] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[UdredningsretKlassifikation]') AND name = 'IX_Map_UdredningsretKlassifikation_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UdredningsretKlassifikation]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_UdredningsretKlassifikation_DW_EK]
        ON [map].[UdredningsretKlassifikation] ([dw_ek_UdredningsretKlassifikation] ASC) INCLUDE ([clean_code_UdredningsretKlassifikation]) ON [PRIMARY]
    ');
END
GO

-- map.Forloebsmarkoer
-- clean_code = FORLOEBSMARKOER_<UPPER(TRIM(FORLOEBSMARKOER))>: one key per
-- distinct marker code as delivered (433 codes measured). The key's own
-- normalisation — not the collation — decides identity, so two case variants
-- of one code can never mint two rows. A blank code gets no key: the marker
-- fact stages NULL for it, which resolves to the -1 sentinel.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Forloebsmarkoer]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[Forloebsmarkoer](
        [dw_ek_Forloebsmarkoer] INT IDENTITY(1,1) NOT NULL,
        [clean_code_Forloebsmarkoer] VARCHAR(50) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Forloebsmarkoer_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Forloebsmarkoer_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Map_Forloebsmarkoer] PRIMARY KEY CLUSTERED ([clean_code_Forloebsmarkoer] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Forloebsmarkoer]') AND name = 'IX_Map_Forloebsmarkoer_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Forloebsmarkoer]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_Forloebsmarkoer_DW_EK]
        ON [map].[Forloebsmarkoer] ([dw_ek_Forloebsmarkoer] ASC) INCLUDE ([clean_code_Forloebsmarkoer]) ON [PRIMARY]
    ');
END
GO

-- map.UdredningsretMarkoer
-- clean_code = UDREDMRK_<markoer_noegle>_<loebenummer>: precisely the archive's
-- own merge key. markoer_noegle is SHA2_256 over (helbredsforloeb key, marker
-- code, marker date) and loebenummer separates the rows that share it, so the
-- fact row and the archive row are 1:1 by construction — which is what carries
-- a soft delete straight through to the fact. Typed VARCHAR (prefix + hex
-- digest + digits is ASCII, <= 80 characters), so a single-byte type carries
-- the key losslessly and halves this clustered key.
-- Keys are minted for live AND soft-deleted markers: a retired marker must keep
-- its dw_ek so it revives on the same fact row when the delivery brings it back.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UdredningsretMarkoer]') AND type = 'U')
BEGIN
    CREATE TABLE [map].[UdredningsretMarkoer](
        [dw_ek_UdredningsretMarkoer] INT IDENTITY(1,1) NOT NULL,
        [clean_code_UdredningsretMarkoer] VARCHAR(100) NOT NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_UdredningsretMarkoer_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_Map_UdredningsretMarkoer] PRIMARY KEY CLUSTERED ([clean_code_UdredningsretMarkoer] ASC)
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[UdredningsretMarkoer]') AND name = 'IX_Map_UdredningsretMarkoer_DW_EK')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UdredningsretMarkoer]') AND type = 'U')
BEGIN
    EXEC('
        CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_UdredningsretMarkoer_DW_EK]
        ON [map].[UdredningsretMarkoer] ([dw_ek_UdredningsretMarkoer] ASC) INCLUDE ([clean_code_UdredningsretMarkoer]) ON [PRIMARY]
    ');
END
GO
