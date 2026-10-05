-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: drop the `_dfm` cutover postfix from the 44 DFM archive extract
-- tables (SydAX 21, ILS 18, MySupply 5), e.g. ext.sydax_art_dfm -> ext.sydax_art.
--
-- The `_dfm` postfix was a cutover marker so the new DFM extracts would not
-- collide with the legacy partition extracts. That collision no longer applies:
--   - SydAX/ILS legacy ext tables are source-SUFFIXED (art_sydax, vareart_ils),
--     while the DFM tables are source-PREFIXED (sydax_art, ils_vareart) -> the
--     postfix-stripped names do not clash with any surviving legacy table.
--   - The MySupply partition extract has already been removed (mysupply_* now
--     exists only as arc merge targets, a different schema).
--
-- Mechanics
-- ---------
-- ext.* archive-extract tables are transient staging, created per run by the
-- extract factory (DROP TABLE IF EXISTS [ext].[x] + CREATE). Once the renamed
-- extract configs deploy (target_table without `_dfm`), the extract recreates
-- ext.* fresh and the arc merge reads it; the old ext.*_dfm tables become
-- orphans, so this migration drops them.
--
-- The delta checkpoint is owned on the arc side (checkpoint_from=("arc", <arc>),
-- unchanged), so no full reload is triggered. The `ext`-scoped rows in
-- meta.dfm_state / meta.dfm_run_log are keyed by the `_dfm` name; we strip the
-- postfix so run history and any ext checkpoint survive the rename (mirrors
-- 20260528_dfm_erp_migration_rename_checkpoints.sql). flow_start keys are stored
-- under the arc name (removesuffix('_dfm')) and are unaffected.
--
-- Deploy this migration together with the renamed extract configs and the arc
-- merge `source_table` edits (they build the ext name as f"{...}_dfm"):
--   DSI/mssql/extract/sydax_extract_configs.py   (target_table=f"{arc}_dfm" x2 + bridge literal)
--   DSI/mssql/extract/extract_configs_ils.py     (target_table=f"ils_{src_table}_dfm" x2)
--   DSI/mssql/extract/extract_configs_mysupply.py(5 literal target_table="..._dfm")
--   DSI/mssql/archive/sydax_archive_configs.py   (source_table=f"{target_table}_dfm")
--   DSI/mssql/archive/ils_archive_configs.py     (source_table=f"ils_{table}_dfm" x2)
--   DSI/mssql/archive/mysupply_archive_configs.py(source_table=f"{target_table}_dfm")
--
-- Idempotent + tracked once via meta.migration_history; on a fresh database the
-- old names never existed, so every guard below is a no-op.

SET NOCOUNT ON;

-- 1. Rename the ext-scoped checkpoint + run-log rows (strip trailing '_dfm').
--    [_] escapes the underscore so LIKE matches the literal postfix only.
IF OBJECT_ID(N'[meta].[dfm_state]', 'U') IS NOT NULL
    UPDATE [meta].[dfm_state]
    SET [table_name] = LEFT([table_name], LEN([table_name]) - 4)
    WHERE [schema_name] = 'ext'
      AND [table_name] LIKE '%[_]dfm';
GO

IF OBJECT_ID(N'[meta].[dfm_run_log]', 'U') IS NOT NULL
    UPDATE [meta].[dfm_run_log]
    SET [table_name] = LEFT([table_name], LEN([table_name]) - 4)
    WHERE [schema_name] = 'ext'
      AND [table_name] LIKE '%[_]dfm';
GO

-- 2. Drop the 44 orphaned old-named ext staging tables (recreated by the next
--    extract under the postfix-free name).

-- SydAX (21)
IF OBJECT_ID(N'[ext].[sydax_afdeling_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_afdeling_dfm];
IF OBJECT_ID(N'[ext].[sydax_art_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_art_dfm];
IF OBJECT_ID(N'[ext].[sydax_dranst_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_dranst_dfm];
IF OBJECT_ID(N'[ext].[sydax_finanstransaktionstype_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_finanstransaktionstype_dfm];
IF OBJECT_ID(N'[ext].[sydax_gruppering_autoriseret_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_gruppering_autoriseret_dfm];
IF OBJECT_ID(N'[ext].[sydax_funktion_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_funktion_dfm];
IF OBJECT_ID(N'[ext].[sydax_hovedkonto_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_hovedkonto_dfm];
IF OBJECT_ID(N'[ext].[sydax_gruppering_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_gruppering_dfm];
IF OBJECT_ID(N'[ext].[sydax_omkostningssted_hierarki_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_omkostningssted_hierarki_dfm];
IF OBJECT_ID(N'[ext].[sydax_budgetmodel_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_budgetmodel_dfm];
IF OBJECT_ID(N'[ext].[sydax_budgetpoststatus_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_budgetpoststatus_dfm];
IF OBJECT_ID(N'[ext].[sydax_budgetbeloebtype_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_budgetbeloebtype_dfm];
IF OBJECT_ID(N'[ext].[sydax_kreditor_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_kreditor_dfm];
IF OBJECT_ID(N'[ext].[sydax_finanskladde_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_finanskladde_dfm];
IF OBJECT_ID(N'[ext].[sydax_finanskonto_omkostningssted_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_finanskonto_omkostningssted_dfm];
IF OBJECT_ID(N'[ext].[sydax_finanskonto_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_finanskonto_dfm];
IF OBJECT_ID(N'[ext].[sydax_finanskladdelinje_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_finanskladdelinje_dfm];
IF OBJECT_ID(N'[ext].[sydax_budgetpost_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_budgetpost_dfm];
IF OBJECT_ID(N'[ext].[sydax_kreditortransaktion_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_kreditortransaktion_dfm];
IF OBJECT_ID(N'[ext].[sydax_finanspostering_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_finanspostering_dfm];
IF OBJECT_ID(N'[ext].[sydax_finanspostering_kreditortrans_relation_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sydax_finanspostering_kreditortrans_relation_dfm];
GO

-- ILS (18)
IF OBJECT_ID(N'[ext].[ils_vareart_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_vareart_dfm];
IF OBJECT_ID(N'[ext].[ils_varegrp_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_varegrp_dfm];
IF OBJECT_ID(N'[ext].[ils_varetype_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_varetype_dfm];
IF OBJECT_ID(N'[ext].[ils_moms_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_moms_dfm];
IF OBJECT_ID(N'[ext].[ils_lev_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_lev_dfm];
IF OBJECT_ID(N'[ext].[ils_sgh_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_sgh_dfm];
IF OBJECT_ID(N'[ext].[ils_lgr_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_lgr_dfm];
IF OBJECT_ID(N'[ext].[ils_rekv_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_rekv_dfm];
IF OBJECT_ID(N'[ext].[ils_rekvgrp_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_rekvgrp_dfm];
IF OBJECT_ID(N'[ext].[ils_lafth_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_lafth_dfm];
IF OBJECT_ID(N'[ext].[ils_zz_oper_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_zz_oper_dfm];
IF OBJECT_ID(N'[ext].[ils_laftdpris_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_laftdpris_dfm];
IF OBJECT_ID(N'[ext].[ils_laftdstam_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_laftdstam_dfm];
IF OBJECT_ID(N'[ext].[ils_laftd_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_laftd_dfm];
IF OBJECT_ID(N'[ext].[ils_udlev_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_udlev_dfm];
IF OBJECT_ID(N'[ext].[ils_lgrvare_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_lgrvare_dfm];
IF OBJECT_ID(N'[ext].[ils_pmdm_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_pmdm_dfm];
IF OBJECT_ID(N'[ext].[ils_indkfak_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[ils_indkfak_dfm];
GO

-- MySupply (5)
IF OBJECT_ID(N'[ext].[mysupply_statistic_invoice_header_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[mysupply_statistic_invoice_header_dfm];
IF OBJECT_ID(N'[ext].[mysupply_metadata_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[mysupply_metadata_dfm];
IF OBJECT_ID(N'[ext].[mysupply_statistic_invoice_line_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[mysupply_statistic_invoice_line_dfm];
IF OBJECT_ID(N'[ext].[mysupply_statistic_invoice_header_allowance_charge_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[mysupply_statistic_invoice_header_allowance_charge_dfm];
IF OBJECT_ID(N'[ext].[mysupply_statistic_invoice_line_allowance_charge_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[mysupply_statistic_invoice_line_allowance_charge_dfm];
GO
