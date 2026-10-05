-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Drop stale lagerbeholdning staging tables
--
-- The lagerbeholdning pre-staging and fact staging tables were renamed:
--   stg.lgrbeh_ils                       -> stg.pre_Lagerbeholdning_ils
--   stg.afdlgrbeh_ils                    -> stg.pre_AfdelingLagerbeholdning_ils
--   stg.fct_afdeling_lagerbeholdning_ils -> stg.fct_AfdelingLagerbeholdning_ils
--
-- Case-only rename (NOT dropped here): under CI collation
--   stg.fct_lagerbeholdning_ils and stg.fct_Lagerbeholdning_ils are the same
--   object, so DROP of the old spelling would wipe the live staging table.
--
-- The old tables are transient (DROP + CREATE each run) but may still exist
-- from runs before the rename. This migration removes the orphaned old
-- tables; the assets create the new tables at runtime.
--
-- Runs on BOTH staging and DSO (no-op on DSO, which carries no stg tables).
-- Idempotent: safe to re-run.
-- ============================================================================

DROP TABLE IF EXISTS [stg].[lgrbeh_ils];
DROP TABLE IF EXISTS [stg].[afdlgrbeh_ils];
DROP TABLE IF EXISTS [stg].[fct_afdeling_lagerbeholdning_ils];
