-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP bridge tables (bridge.*)
-- bridge.LeverandoervareILSVare: fuld mange-til-mange Leverandoervare <-> ILSVare.
-- Delta-vedligeholdt med soft-delete: dw_is_deleted = 1 naar parret er udgaaet
-- af kilderne (saettes kun scoped til delta-entiteter, se bridge_LeverandoervareILSVare).
-- bridge.AftaleLager: aftale <-> lager/sygehus-styring fra ILS lafthsgh (spec
-- 2026-08-31-prisloese-linjer-og-aftalelager). Full rebuild hver koersel,
-- pre-delete-sweep over hele tabellen + MERGE paa (clean_code_Aftale, sgh_raw).
-- Idempotent: safe to re-run.
-- ============================================================================

-- bridge.LeverandoervareILSVare
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND type = 'U'
) BEGIN
CREATE TABLE [bridge].[LeverandoervareILSVare] (
    [dw_ek_Leverandoervare] INT           NOT NULL,
    [dw_ek_ILSVare]         INT           NOT NULL,
    [bridge_kilder]         NVARCHAR(100) NULL,
    [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_LeverandoervareILSVare_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert]   DATETIME2     NOT NULL CONSTRAINT [DF_LeverandoervareILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]   DATETIME2     NOT NULL CONSTRAINT [DF_LeverandoervareILSVare_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Bridge_LeverandoervareILSVare] PRIMARY KEY CLUSTERED ([dw_ek_Leverandoervare] ASC, [dw_ek_ILSVare] ASC)
) ON [PRIMARY];
END
GO

-- Interim-guard: tabellen kan vaere oprettet af en tidligere branch-koersel
-- med er_primaer-kolonnen (aldrig deployet til DEV/PROD). Idempotent reshape.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND name = 'er_primaer'
)
BEGIN
    DECLARE @df SYSNAME = NULL;
    SELECT @df = dc.name
      FROM sys.default_constraints dc
      JOIN sys.columns c ON c.default_object_id = dc.object_id
     WHERE c.object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND c.name = 'er_primaer';
    IF @df IS NOT NULL
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] DROP CONSTRAINT ' + @df);
    ALTER TABLE [bridge].[LeverandoervareILSVare] DROP COLUMN [er_primaer];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND name = 'dw_is_deleted'
)
BEGIN
    ALTER TABLE [bridge].[LeverandoervareILSVare] ADD [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_LeverandoervareILSVare_dw_is_deleted] DEFAULT 0;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND name = 'IX_Bridge_LeverandoervareILSVare_ILSVare'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Bridge_LeverandoervareILSVare_ILSVare]
    ON [bridge].[LeverandoervareILSVare] ([dw_ek_ILSVare] ASC)
    ON [PRIMARY];
END
GO

-- Seed UNKNOWN pair (-1, -1). Dynamisk SQL jf. idempotens-regel.
IF NOT EXISTS (SELECT 1 FROM [bridge].[LeverandoervareILSVare] WHERE [dw_ek_Leverandoervare] = -1 AND [dw_ek_ILSVare] = -1)
BEGIN
    EXEC('
        INSERT INTO [bridge].[LeverandoervareILSVare] (
            [dw_ek_Leverandoervare], [dw_ek_ILSVare], [dw_is_deleted], [dw_timestamp_insert]
        ) VALUES (-1, -1, 0, CAST(SYSDATETIME() AS DATETIME2))
    ');
END
GO

-- bridge.AftaleLager (spec 2026-08-31-prisloese-linjer-og-aftalelager):
-- ILS lafthsgh — aftale x lager/sygehus-styring ("Benyttes af"). Rows kun for
-- STEEREDE aftaler (dim.Aftale.alle_maa_benytte = 0 <=> aftalen HAR rows
-- her); ingen rows for en aftale = alle sygehuse/lagre maa koebe ind.
-- sgh er kildens sammensatte noegle (sygehus+lager); splittet i staging til
-- sygehus (2 tegn hvis praefiks '55', ellers 4) + lager (NULL = helt sygehus).
-- dw_ek_Lager matcher fct.Vareudlevering.dw_ek_Lager (BIGINT); hospital-only
-- raekker (lager = NULL) resolver mod codesystemets SGH_{sygehus}-koncept
-- (blandet granularitet — lager IS NULL markerer niveauet; -1 = aegte map-miss).
-- Full rebuild hver koersel (staging), pre-delete-sweep over hele tabellen +
-- MERGE paa (clean_code_Aftale, sgh_raw) i applieren — se
-- stg_bridge_AftaleLager / bridge_AftaleLager.
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]') AND type = 'U'
) BEGIN
CREATE TABLE [bridge].[AftaleLager] (
    [dw_sk_AftaleLager]    BIGINT        IDENTITY(1,1) NOT NULL,
    [dw_ek_Aftale]         INT           NOT NULL CONSTRAINT [DF_AftaleLager_dw_ek_Aftale] DEFAULT -1,
    [dw_ek_Lager]          BIGINT        NOT NULL CONSTRAINT [DF_AftaleLager_dw_ek_Lager] DEFAULT -1,
    [clean_code_Aftale]    NVARCHAR(200) NOT NULL,
    [sgh_raw]              NVARCHAR(20)  NOT NULL,
    [sygehus]              NVARCHAR(10)  NOT NULL,
    [lager]                NVARCHAR(10)  NULL,
    -- NULL-able med vilje: spejler arc.ils_lafthsgh (sw_benyt/sw_prioritet er
    -- bit NULL i kilden). NULL = kilden har ikke sat flaget — en NOT NULL med
    -- default ville opfinde tilladelses-semantik, og en fremtidig kilde-NULL
    -- ville standse bridge-loadet.
    [maa_benyttes]         BIT           NULL,
    [foerste_prioritet]    BIT           NULL,
    [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_AftaleLager_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert]  DATETIME2     NOT NULL CONSTRAINT [DF_AftaleLager_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update]  DATETIME2     NOT NULL CONSTRAINT [DF_AftaleLager_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_Bridge_AftaleLager] PRIMARY KEY CLUSTERED ([dw_sk_AftaleLager] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]') AND name = 'IX_Bridge_AftaleLager_grain'
)
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Bridge_AftaleLager_grain]
    ON [bridge].[AftaleLager] ([clean_code_Aftale] ASC, [sgh_raw] ASC)
    ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]') AND name = 'IX_Bridge_AftaleLager_Lager'
)
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Bridge_AftaleLager_Lager]
    ON [bridge].[AftaleLager] ([dw_ek_Lager] ASC)
    ON [PRIMARY];
END
GO

-- dw_ek_Aftale: den PRIMAERE join-vej fra dim.Aftale (bridge.AftaleLager
-- resolves alle_maa_benytte=0-aftalers styring). Uden dette indeks scanner
-- ethvert opslag fra dim.Aftale hele bridge-tabellen.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]') AND name = 'IX_Bridge_AftaleLager_Aftale'
)
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Bridge_AftaleLager_Aftale]
    ON [bridge].[AftaleLager] ([dw_ek_Aftale] ASC)
    ON [PRIMARY];
END
GO
