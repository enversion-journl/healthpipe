-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Drop orphaned ERP staging tables after pipeline simplification
--
-- ERP flow-separation cleanup renamed/removed three runtime-built staging
-- tables (all recreated by their assets via DROP + SELECT INTO each run):
--   * stg.pre_ils_vare_ils   -> renamed to stg.dim_ILSVare_ils
--                               (asset pre_stg_ILSVare_ils -> stg_dim_ILSVare_ils)
--   * stg.map_ils_vare_ils   -> removed; map.ILSVare now seeds distinct
--                               CleanCodes directly from stg.dim_ILSVare_ils
--   * stg.dim_Vare_mysupply  -> renamed to stg.pre_Vare_mysupply
--                               (asset stg_dim_Vare_mysupply -> pre_stg_Vare_mysupply)
--   * stg.dim_Kreditor_sydax / stg.dim_Kreditor_ils / stg.dim_Kreditor_mysupply
--                            -> replaced by the unified stg.dim_Kreditor
--                               (CVR key set + dw_source flags; attributes
--                               enriched from arc.* in dim_Kreditor)
--
-- The new tables are created by the assets at runtime; this migration only
-- removes the orphaned old tables.
--
-- Idempotent: safe to re-run.
-- ============================================================================

DROP TABLE IF EXISTS [stg].[pre_ils_vare_ils];
DROP TABLE IF EXISTS [stg].[map_ils_vare_ils];
DROP TABLE IF EXISTS [stg].[dim_Vare_mysupply];
DROP TABLE IF EXISTS [stg].[dim_Kreditor_sydax];
DROP TABLE IF EXISTS [stg].[dim_Kreditor_ils];
DROP TABLE IF EXISTS [stg].[dim_Kreditor_mysupply];
