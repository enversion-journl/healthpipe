-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Rename ILS lagerbeholdning ext/arc tables: source-postfix -> source-prefix
--
-- lgrbeh_ils    -> ils_lgrbeh   (ext + arc)
-- afdlgrbeh_ils -> ils_afdlgrbeh (ext + arc)
--
-- Brings the last legacy postfix holdouts for warehouse/department stock in
-- line with the established arc.ils_* / arc.mysupply_* prefix convention.
-- Idempotent: each rename is guarded; re-running is a no-op. Preserves the
-- insert-only arc snapshot history via sp_rename (no data copy).

BEGIN TRANSACTION;

-- 1. Rename the four physical tables (old exists AND new does not) ------------
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ext' AND TABLE_NAME = 'lgrbeh_ils')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ext' AND TABLE_NAME = 'ils_lgrbeh')
    EXEC sp_rename N'ext.lgrbeh_ils', N'ils_lgrbeh';

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'lgrbeh_ils')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lgrbeh')
    EXEC sp_rename N'arc.lgrbeh_ils', N'ils_lgrbeh';

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ext' AND TABLE_NAME = 'afdlgrbeh_ils')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ext' AND TABLE_NAME = 'ils_afdlgrbeh')
    EXEC sp_rename N'ext.afdlgrbeh_ils', N'ils_afdlgrbeh';

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'afdlgrbeh_ils')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_afdlgrbeh')
    EXEC sp_rename N'arc.afdlgrbeh_ils', N'ils_afdlgrbeh';

-- 2. Rename the two arc indexes (qualified by the NEW table names) ------------
-- Runs after the table renames above, so in an existing environment the arc
-- table already carries its new name and OBJECT_ID resolves. In a fresh
-- environment (migration runs before 00_archive.sql) the table does not exist
-- yet, OBJECT_ID is NULL, and the guard simply no-ops — 00_archive.sql then
-- creates the index under the new name directly. Both paths are correct.
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_arc_lgrbeh_ils_DW_Timestamp_Update' AND object_id = OBJECT_ID(N'[arc].[ils_lgrbeh]'))
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_arc_ils_lgrbeh_DW_Timestamp_Update' AND object_id = OBJECT_ID(N'[arc].[ils_lgrbeh]'))
    EXEC sp_rename N'arc.ils_lgrbeh.IX_arc_lgrbeh_ils_DW_Timestamp_Update', N'IX_arc_ils_lgrbeh_DW_Timestamp_Update', N'INDEX';

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_arc_afdlgrbeh_ils_DW_Timestamp_Update' AND object_id = OBJECT_ID(N'[arc].[ils_afdlgrbeh]'))
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_arc_ils_afdlgrbeh_DW_Timestamp_Update' AND object_id = OBJECT_ID(N'[arc].[ils_afdlgrbeh]'))
    EXEC sp_rename N'arc.ils_afdlgrbeh.IX_arc_afdlgrbeh_ils_DW_Timestamp_Update', N'IX_arc_ils_afdlgrbeh_DW_Timestamp_Update', N'INDEX';

-- 3. Defensive DFM-tracking cleanup ------------------------------------------
-- These are full-load tables (no checkpoint), but the run log may carry rows.
-- Exact-match updates (NOT REPLACE) so 'lgrbeh_ils' never corrupts the
-- 'afdlgrbeh_ils' substring. Idempotent.
IF OBJECT_ID(N'[meta].[dfm_state]', N'U') IS NOT NULL
BEGIN
    UPDATE [meta].[dfm_state] SET [table_name] = 'ils_afdlgrbeh'      WHERE [table_name] = 'afdlgrbeh_ils';
    UPDATE [meta].[dfm_state] SET [table_name] = 'ils_lgrbeh'         WHERE [table_name] = 'lgrbeh_ils';
    UPDATE [meta].[dfm_state] SET [table_name] = 'ext.ils_afdlgrbeh'  WHERE [table_name] = 'ext.afdlgrbeh_ils';
    UPDATE [meta].[dfm_state] SET [table_name] = 'ext.ils_lgrbeh'     WHERE [table_name] = 'ext.lgrbeh_ils';
    UPDATE [meta].[dfm_state] SET [table_name] = 'arc.ils_afdlgrbeh'  WHERE [table_name] = 'arc.afdlgrbeh_ils';
    UPDATE [meta].[dfm_state] SET [table_name] = 'arc.ils_lgrbeh'     WHERE [table_name] = 'arc.lgrbeh_ils';
END

IF OBJECT_ID(N'[meta].[dfm_run_log]', N'U') IS NOT NULL
BEGIN
    UPDATE [meta].[dfm_run_log] SET [table_name] = 'ils_afdlgrbeh'     WHERE [table_name] = 'afdlgrbeh_ils';
    UPDATE [meta].[dfm_run_log] SET [table_name] = 'ils_lgrbeh'        WHERE [table_name] = 'lgrbeh_ils';
    UPDATE [meta].[dfm_run_log] SET [table_name] = 'ext.ils_afdlgrbeh' WHERE [table_name] = 'ext.afdlgrbeh_ils';
    UPDATE [meta].[dfm_run_log] SET [table_name] = 'ext.ils_lgrbeh'    WHERE [table_name] = 'ext.lgrbeh_ils';
    UPDATE [meta].[dfm_run_log] SET [table_name] = 'arc.ils_afdlgrbeh' WHERE [table_name] = 'arc.afdlgrbeh_ils';
    UPDATE [meta].[dfm_run_log] SET [table_name] = 'arc.ils_lgrbeh'    WHERE [table_name] = 'arc.lgrbeh_ils';
END

COMMIT;
