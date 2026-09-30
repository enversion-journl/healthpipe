-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- DFM ERP Migration Cutover: Rename checkpoint entries
--
-- The _dfm_new shadow pipeline assets have been renamed to their production
-- names.  This script updates meta.dfm_state so the existing checkpoints
-- survive the rename.  Run BEFORE deploying the renamed assets.
--
-- Affected tables (12 entries):
--   ext.unspsc_ils_dfm_new          → ext.unspsc_ils
--   ext.vare_ils_delta_dfm_new      → ext.vare_ils_delta
--   ext.lgrbeh_ils_dfm_new          → ext.lgrbeh_ils
--   ext.afdlgrbeh_ils_dfm_new      → ext.afdlgrbeh_ils
--   ext.unspsc_kmd_uddata_dfm_new   → ext.unspsc_kmd_uddata
--   ext.spend_analysis_lines_kmd_uddata_dfm_new → ext.spend_analysis_lines_kmd_uddata
--   arc.unspsc_ils_dfm_new          → arc.unspsc_ils
--   arc.vare_ils_dfm_new            → arc.vare_ils
--   arc.lgrbeh_ils_dfm_new          → arc.lgrbeh_ils
--   arc.afdlgrbeh_ils_dfm_new      → arc.afdlgrbeh_ils
--   arc.unspsc_kmd_uddata_dfm_new   → arc.unspsc_kmd_uddata
--   arc.spend_analysis_lines_kmd_uddata_dfm_new → arc.spend_analysis_lines_kmd_uddata

BEGIN TRANSACTION;

UPDATE [meta].[dfm_state]
SET [table_name] = REPLACE([table_name], '_dfm_new', '')
WHERE [table_name] LIKE '%_dfm_new';

-- Also update the run log so history references the production names
UPDATE [meta].[dfm_run_log]
SET [table_name] = REPLACE([table_name], '_dfm_new', '')
WHERE [table_name] LIKE '%_dfm_new';

COMMIT;
