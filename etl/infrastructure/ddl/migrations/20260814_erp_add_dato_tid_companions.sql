-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP dato_tid_ raw-timestamp companions.
--
-- Formaal: tilfoejer nye dato_tid_* raw-timestamp companion-kolonner til de
-- ERP fact-/dim-tabeller der har faaet nye dw_ek_Dato_* entity-keys (Tasks
-- 2-10), samt standardiserer de faa allerede-eksisterende legacy
-- datotid_* companion-kolonner til den nye dato_tid_ stavning (sp_rename).
--
-- Structural-only / ingen data-backfill: ERP-facts er full-load, saa nye
-- kolonner fyldes af naeste ordinaere load. Ingen UPDATE af data-rows.
--
-- Idempotent + guarded (safe to re-run):
--   * ADD-blokke guardes paa OBJECT_ID(...) IS NOT NULL AND COL_LENGTH(...) IS NULL.
--   * sp_rename-blokke guardes paa gammel kolonne findes AND ny kolonne ikke findes.
--   * Blokke adskilles med GO.
--
-- Base-DDL for de omdoebte tabeller (fct.Vareudlevering, fct.Lagerbeholdning,
-- fct.AfdelingLagerbeholdning, dim.ILSVare) har allerede KUN dato_tid_
-- stavningen (rettet i tidligere tasks) — denne migration roerer ikke base-DDL.
--
-- Sektioneret:
--   1) ADD nye dato_tid_* companions (26 kolonner)
--   2) RENAME legacy datotid_* -> dato_tid_* (7 kolonner)
-- ============================================================================

-- ============================================================================
-- SECTION 1: ADD new dato_tid_* companions
-- ============================================================================

-- === fct.Postering ===
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_godkendelse') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_godkendelse] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_lukket') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_lukket] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_trans_oprettet') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_trans_oprettet] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_forfald') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_forfald] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_journal_faktura') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_journal_faktura] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_sidst_udlignet') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_sidst_udlignet] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_transaktion') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_transaktion] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_bogfoering') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_bogfoering] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL AND COL_LENGTH('fct.Postering', 'dato_tid_post_oprettet') IS NULL
    ALTER TABLE [fct].[Postering] ADD [dato_tid_post_oprettet] DATETIME2 NULL;
GO

-- === fct.Faktura ===
IF OBJECT_ID(N'[fct].[Faktura]', 'U') IS NOT NULL AND COL_LENGTH('fct.Faktura', 'dato_tid_faktura') IS NULL
    ALTER TABLE [fct].[Faktura] ADD [dato_tid_faktura] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Faktura]', 'U') IS NOT NULL AND COL_LENGTH('fct.Faktura', 'dato_tid_ordre') IS NULL
    ALTER TABLE [fct].[Faktura] ADD [dato_tid_ordre] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Faktura]', 'U') IS NOT NULL AND COL_LENGTH('fct.Faktura', 'dato_tid_betaling') IS NULL
    ALTER TABLE [fct].[Faktura] ADD [dato_tid_betaling] DATETIME2 NULL;
GO

-- === fct.Fakturalinje ===
IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL AND COL_LENGTH('fct.Fakturalinje', 'dato_tid_faktura') IS NULL
    ALTER TABLE [fct].[Fakturalinje] ADD [dato_tid_faktura] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL AND COL_LENGTH('fct.Fakturalinje', 'dato_tid_ordre') IS NULL
    ALTER TABLE [fct].[Fakturalinje] ADD [dato_tid_ordre] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL AND COL_LENGTH('fct.Fakturalinje', 'dato_tid_betaling') IS NULL
    ALTER TABLE [fct].[Fakturalinje] ADD [dato_tid_betaling] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL AND COL_LENGTH('fct.Fakturalinje', 'dato_tid_modtagelse') IS NULL
    ALTER TABLE [fct].[Fakturalinje] ADD [dato_tid_modtagelse] DATETIME2 NULL;
GO

-- === fct.Budget ===
IF OBJECT_ID(N'[fct].[Budget]', 'U') IS NOT NULL AND COL_LENGTH('fct.Budget', 'dato_tid_budget') IS NULL
    ALTER TABLE [fct].[Budget] ADD [dato_tid_budget] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Budget]', 'U') IS NOT NULL AND COL_LENGTH('fct.Budget', 'dato_tid_budget_adopted') IS NULL
    ALTER TABLE [fct].[Budget] ADD [dato_tid_budget_adopted] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Budget]', 'U') IS NOT NULL AND COL_LENGTH('fct.Budget', 'dato_tid_budget_oprettet') IS NULL
    ALTER TABLE [fct].[Budget] ADD [dato_tid_budget_oprettet] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Budget]', 'U') IS NOT NULL AND COL_LENGTH('fct.Budget', 'dato_tid_budget_aendret') IS NULL
    ALTER TABLE [fct].[Budget] ADD [dato_tid_budget_aendret] DATETIME2 NULL;
GO

-- === fct.Finanskladdelinje ===
IF OBJECT_ID(N'[fct].[Finanskladdelinje]', 'U') IS NOT NULL AND COL_LENGTH('fct.Finanskladdelinje', 'dato_tid_linje') IS NULL
    ALTER TABLE [fct].[Finanskladdelinje] ADD [dato_tid_linje] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[fct].[Finanskladdelinje]', 'U') IS NOT NULL AND COL_LENGTH('fct.Finanskladdelinje', 'dato_tid_oprettet') IS NULL
    ALTER TABLE [fct].[Finanskladdelinje] ADD [dato_tid_oprettet] DATETIME2 NULL;
GO

-- === dim.Finanskladde ===
IF OBJECT_ID(N'[dim].[Finanskladde]', 'U') IS NOT NULL AND COL_LENGTH('dim.Finanskladde', 'dato_tid_oprettet') IS NULL
    ALTER TABLE [dim].[Finanskladde] ADD [dato_tid_oprettet] DATETIME2 NULL;
GO

IF OBJECT_ID(N'[dim].[Finanskladde]', 'U') IS NOT NULL AND COL_LENGTH('dim.Finanskladde', 'dato_tid_aendret') IS NULL
    ALTER TABLE [dim].[Finanskladde] ADD [dato_tid_aendret] DATETIME2 NULL;
GO

-- === fct.Lagerbeholdning ===
IF OBJECT_ID(N'[fct].[Lagerbeholdning]', 'U') IS NOT NULL AND COL_LENGTH('fct.Lagerbeholdning', 'dato_tid_udloeb') IS NULL
    ALTER TABLE [fct].[Lagerbeholdning] ADD [dato_tid_udloeb] DATETIME2 NULL;
GO

-- === fct.AfdelingLagerbeholdning ===
IF OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]', 'U') IS NOT NULL AND COL_LENGTH('fct.AfdelingLagerbeholdning', 'dato_tid_udloeb') IS NULL
    ALTER TABLE [fct].[AfdelingLagerbeholdning] ADD [dato_tid_udloeb] DATETIME2 NULL;
GO

-- ============================================================================
-- SECTION 2: RENAME legacy datotid_ -> dato_tid_ (standardize spelling)
-- ============================================================================

-- === fct.Vareudlevering ===
IF COL_LENGTH('fct.Vareudlevering', 'datotid_vareudlevering') IS NOT NULL AND COL_LENGTH('fct.Vareudlevering', 'dato_tid_vareudlevering') IS NULL
    EXEC sp_rename N'fct.Vareudlevering.datotid_vareudlevering', N'dato_tid_vareudlevering', N'COLUMN';
GO

IF COL_LENGTH('fct.Vareudlevering', 'datotid_udloeb') IS NOT NULL AND COL_LENGTH('fct.Vareudlevering', 'dato_tid_udloeb') IS NULL
    EXEC sp_rename N'fct.Vareudlevering.datotid_udloeb', N'dato_tid_udloeb', N'COLUMN';
GO

IF COL_LENGTH('fct.Vareudlevering', 'datotid_postering') IS NOT NULL AND COL_LENGTH('fct.Vareudlevering', 'dato_tid_postering') IS NULL
    EXEC sp_rename N'fct.Vareudlevering.datotid_postering', N'dato_tid_postering', N'COLUMN';
GO

IF COL_LENGTH('fct.Vareudlevering', 'datotid_aflevering') IS NOT NULL AND COL_LENGTH('fct.Vareudlevering', 'dato_tid_aflevering') IS NULL
    EXEC sp_rename N'fct.Vareudlevering.datotid_aflevering', N'dato_tid_aflevering', N'COLUMN';
GO

-- === fct.Lagerbeholdning ===
IF COL_LENGTH('fct.Lagerbeholdning', 'datotid_snapshot') IS NOT NULL AND COL_LENGTH('fct.Lagerbeholdning', 'dato_tid_snapshot') IS NULL
    EXEC sp_rename N'fct.Lagerbeholdning.datotid_snapshot', N'dato_tid_snapshot', N'COLUMN';
GO

-- === fct.AfdelingLagerbeholdning ===
IF COL_LENGTH('fct.AfdelingLagerbeholdning', 'datotid_snapshot') IS NOT NULL AND COL_LENGTH('fct.AfdelingLagerbeholdning', 'dato_tid_snapshot') IS NULL
    EXEC sp_rename N'fct.AfdelingLagerbeholdning.datotid_snapshot', N'dato_tid_snapshot', N'COLUMN';
GO

-- === dim.ILSVare ===
IF COL_LENGTH('dim.ILSVare', 'datotid_oprettet') IS NOT NULL AND COL_LENGTH('dim.ILSVare', 'dato_tid_oprettet') IS NULL
    EXEC sp_rename N'dim.ILSVare.datotid_oprettet', N'dato_tid_oprettet', N'COLUMN';
GO
