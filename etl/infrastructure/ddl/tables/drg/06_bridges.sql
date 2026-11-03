-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG bridge tables (bridge.*)
--
-- Group->code membership for the code dimensions. fct.DRGKontakt references a
-- contact's whole set of diagnoses / procedures with a single group FK
-- (dw_ek_DRGDiagnoseGruppe / dw_ek_DRGProcedureGruppe, minted in
-- map.DRG*Gruppe); these bridges expand a group into its member codes.
--
-- APPEND-ONLY: a group is identified by the hash of its member set, so its
-- membership is immutable — rows are only ever inserted, never updated or
-- soft-deleted. When a contact's set changes it points to a different (possibly
-- new) group; the old group simply stops being referenced. dw_is_deleted is
-- kept at 0 for sync/DQA uniformity. Idempotent.
-- ============================================================================

-- bridge.DRGDiagnoseGruppe  (group -> diagnose)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[bridge].[DRGDiagnoseGruppe]') AND type = 'U'
) BEGIN
CREATE TABLE [bridge].[DRGDiagnoseGruppe] (
    [dw_ek_DRGDiagnoseGruppe] INT       NOT NULL,
    [dw_ek_DRGDiagnose]       INT       NOT NULL,
    [dw_is_deleted]           BIT       NOT NULL CONSTRAINT [DF_DRGDiagnoseGruppe_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert]     DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnoseGruppe_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
    [dw_timestamp_update]     DATETIME2 NOT NULL CONSTRAINT [DF_DRGDiagnoseGruppe_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [PK_Bridge_DRGDiagnoseGruppe] PRIMARY KEY CLUSTERED ([dw_ek_DRGDiagnoseGruppe] ASC, [dw_ek_DRGDiagnose] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[bridge].[DRGDiagnoseGruppe]') AND name = 'IX_Bridge_DRGDiagnoseGruppe_Diagnose')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[DRGDiagnoseGruppe]') AND type = 'U')
BEGIN
    EXEC('CREATE NONCLUSTERED INDEX [IX_Bridge_DRGDiagnoseGruppe_Diagnose] ON [bridge].[DRGDiagnoseGruppe] ([dw_ek_DRGDiagnose] ASC) ON [PRIMARY]');
END
GO

-- Seed the empty-group pair (-1, -1). Dynamisk SQL jf. idempotens-regel.
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[DRGDiagnoseGruppe]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [bridge].[DRGDiagnoseGruppe] WHERE [dw_ek_DRGDiagnoseGruppe] = -1 AND [dw_ek_DRGDiagnose] = -1)
BEGIN
    EXEC('INSERT INTO [bridge].[DRGDiagnoseGruppe] ([dw_ek_DRGDiagnoseGruppe], [dw_ek_DRGDiagnose], [dw_is_deleted], [dw_timestamp_insert]) VALUES (-1, -1, 0, CAST(SYSDATETIME() AS DATETIME2))');
END
GO

-- bridge.DRGProcedureGruppe  (group -> procedure + producing unit)
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[bridge].[DRGProcedureGruppe]') AND type = 'U'
) BEGIN
CREATE TABLE [bridge].[DRGProcedureGruppe] (
    [dw_ek_DRGProcedureGruppe]        INT       NOT NULL,
    [dw_ek_DRGProcedure]              INT       NOT NULL,
    [dw_ek_Organisation_producerende] INT       NOT NULL CONSTRAINT [DF_DRGProcedureGruppe_dw_ek_Organisation_producerende] DEFAULT -1,
    [dw_is_deleted]                   BIT       NOT NULL CONSTRAINT [DF_DRGProcedureGruppe_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert]             DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedureGruppe_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
    [dw_timestamp_update]             DATETIME2 NOT NULL CONSTRAINT [DF_DRGProcedureGruppe_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
    -- Membership grain (group, procedure-code, producing unit): the producing
    -- unit is part of the group's defining set, so the same code from two units
    -- is two membership rows.
    CONSTRAINT [PK_Bridge_DRGProcedureGruppe] PRIMARY KEY CLUSTERED ([dw_ek_DRGProcedureGruppe] ASC, [dw_ek_DRGProcedure] ASC, [dw_ek_Organisation_producerende] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[bridge].[DRGProcedureGruppe]') AND name = 'IX_Bridge_DRGProcedureGruppe_Procedure')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[DRGProcedureGruppe]') AND type = 'U')
BEGIN
    EXEC('CREATE NONCLUSTERED INDEX [IX_Bridge_DRGProcedureGruppe_Procedure] ON [bridge].[DRGProcedureGruppe] ([dw_ek_DRGProcedure] ASC) ON [PRIMARY]');
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[bridge].[DRGProcedureGruppe]') AND name = 'IX_Bridge_DRGProcedureGruppe_Producerende')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[DRGProcedureGruppe]') AND type = 'U')
BEGIN
    EXEC('CREATE NONCLUSTERED INDEX [IX_Bridge_DRGProcedureGruppe_Producerende] ON [bridge].[DRGProcedureGruppe] ([dw_ek_Organisation_producerende] ASC) ON [PRIMARY]');
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[bridge].[DRGProcedureGruppe]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [bridge].[DRGProcedureGruppe] WHERE [dw_ek_DRGProcedureGruppe] = -1 AND [dw_ek_DRGProcedure] = -1)
BEGIN
    EXEC('INSERT INTO [bridge].[DRGProcedureGruppe] ([dw_ek_DRGProcedureGruppe], [dw_ek_DRGProcedure], [dw_ek_Organisation_producerende], [dw_is_deleted], [dw_timestamp_insert]) VALUES (-1, -1, -1, 0, CAST(SYSDATETIME() AS DATETIME2))');
END
GO
