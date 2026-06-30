-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Drop stale lagerbeholdning staging tables
-- Date: 2026-04-15
--
-- Pre-staging and fact staging tables were renamed:
--   stg.lgrbeh_ils                      -> stg.pre_Lagerbeholdning_ils
--   stg.afdlgrbeh_ils                   -> stg.pre_AfdelingLagerbeholdning_ils
--   stg.fct_lagerbeholdning_ils         -> stg.fct_Lagerbeholdning_ils
--   stg.fct_afdeling_lagerbeholdning_ils -> stg.fct_AfdelingLagerbeholdning_ils
--
-- The old tables are transient (DROP+CREATE each run) but may still exist
-- from previous runs. This migration drops them to avoid confusion.
--
-- Run on the staging database.
-- Idempotent: safe to re-run.
-- ============================================================================

IF OBJECT_ID(N'[stg].[lgrbeh_ils]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [stg].[lgrbeh_ils];
    PRINT 'Dropped stale table stg.lgrbeh_ils';
END
GO

IF OBJECT_ID(N'[stg].[afdlgrbeh_ils]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [stg].[afdlgrbeh_ils];
    PRINT 'Dropped stale table stg.afdlgrbeh_ils';
END
GO

IF OBJECT_ID(N'[stg].[fct_lagerbeholdning_ils]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [stg].[fct_lagerbeholdning_ils];
    PRINT 'Dropped stale table stg.fct_lagerbeholdning_ils';
END
GO

IF OBJECT_ID(N'[stg].[fct_afdeling_lagerbeholdning_ils]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [stg].[fct_afdeling_lagerbeholdning_ils];
    PRINT 'Dropped stale table stg.fct_afdeling_lagerbeholdning_ils';
END
GO

PRINT '=== Migration complete ===';
GO
