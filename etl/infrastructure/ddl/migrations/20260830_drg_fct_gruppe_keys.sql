-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG US-5 (group-key redesign): add the code-set group FKs to fct.DRGKontakt.
--
-- fct.DRGKontakt references a contact's whole set of diagnoses / procedures with
-- one FK each (dw_ek_DRGDiagnoseGruppe / dw_ek_DRGProcedureGruppe, minted in
-- map.DRG*Gruppe). Fresh deploys get these from tables/drg/04_facts.sql; this
-- migration brings already-deployed databases to the same schema.
--
-- Idempotent: guarded per column and per index. NOT NULL DEFAULT -1 backfills
-- existing rows with the empty-group sentinel. The real group keys are then
-- populated by a FULL_LOAD run of the DRG chain (a delta only touches its own
-- batch, so the historical rows stay -1 until then) — the US-5 deploy must
-- include one full_load, same bootstrap the fct control total already assumes.
--
-- ADD ... NOT NULL DEFAULT -1 is an instant metadata change on SQL Server
-- Enterprise; on Standard Edition it rewrites the table under a Sch-M lock
-- (heavy on ~35M rows) — schedule the deploy window accordingly.
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND type = 'U')
   AND COL_LENGTH('fct.DRGKontakt', 'dw_ek_DRGDiagnoseGruppe') IS NULL
BEGIN
    ALTER TABLE [fct].[DRGKontakt]
        ADD [dw_ek_DRGDiagnoseGruppe] INT NOT NULL
            CONSTRAINT [DF_DRGKontakt_dw_ek_DRGDiagnoseGruppe] DEFAULT -1;
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND type = 'U')
   AND COL_LENGTH('fct.DRGKontakt', 'dw_ek_DRGProcedureGruppe') IS NULL
BEGIN
    ALTER TABLE [fct].[DRGKontakt]
        ADD [dw_ek_DRGProcedureGruppe] INT NOT NULL
            CONSTRAINT [DF_DRGKontakt_dw_ek_DRGProcedureGruppe] DEFAULT -1;
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND type = 'U')
   AND COL_LENGTH('fct.DRGKontakt', 'dw_ek_DRGDiagnoseGruppe') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND name = 'IX_Fct_DRGKontakt_DiagnoseGruppe')
BEGIN
    EXEC('CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_DiagnoseGruppe] ON [fct].[DRGKontakt] ([dw_ek_DRGDiagnoseGruppe] ASC) ON [PRIMARY]');
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND type = 'U')
   AND COL_LENGTH('fct.DRGKontakt', 'dw_ek_DRGProcedureGruppe') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND name = 'IX_Fct_DRGKontakt_ProcedureGruppe')
BEGIN
    EXEC('CREATE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_ProcedureGruppe] ON [fct].[DRGKontakt] ([dw_ek_DRGProcedureGruppe] ASC) ON [PRIMARY]');
END
GO
