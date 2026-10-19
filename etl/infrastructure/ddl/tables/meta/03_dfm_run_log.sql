-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DFM Run Log
-- Used by DfmManager in core.dfm_manager
--
--   meta.DFMRunLog — one row per asset run. Full audit trail including
--                       mode, checkpoint before/after, error details.
--                       Every extraction from outside the platform and every
--                       flow start that reads its source also record the source (source_user,
--                       source_host, source_table). The columns are NULL when
--                       the source has no user or host, e.g. anonymous HTTP.
--
--   status values: 'running'   — run in progress
--                  'success'   — completed normally
--                  'failed'    — failed; error_message holds the exception
--                  'abandoned' — was stuck in 'running' past the staleness
--                                threshold (process killed before
--                                complete/fail); set by
--                                DfmManager.sweep_stale_runs()
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'DFMRunLog'
)
BEGIN
    CREATE TABLE [meta].[DFMRunLog] (
        [id]                 BIGINT IDENTITY(1,1)  NOT NULL,
        [scope]              NVARCHAR(50)   NOT NULL
            CONSTRAINT [DF_DFMRunLog_scope] DEFAULT ('staging'),
        [target_type]        NVARCHAR(50)   NOT NULL,
        [schema_name]        NVARCHAR(128)  NOT NULL,
        [table_name]         NVARCHAR(128)  NOT NULL,
        [mode]               NVARCHAR(50)   NOT NULL,
        [status]             NVARCHAR(50)   NOT NULL,
        [checkpoint_before]  NVARCHAR(50)   NULL,
        [checkpoint_after]   NVARCHAR(50)   NULL,
        [rows_affected]      INT            NULL,
        [error_message]      NVARCHAR(MAX)  NULL,
        [dagster_run_id]     NVARCHAR(128)  NULL,
        [run_started_at]     DATETIME2      NOT NULL,
        [run_completed_at]   DATETIME2      NULL,
        [source_user]        NVARCHAR(128)  NULL,
        [source_host]        NVARCHAR(255)  NULL,
        [source_table]       NVARCHAR(256)  NULL,
        CONSTRAINT [PK_DFMRunLog]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

-- Filtered index supporting sweep_stale_runs(), which runs at the start of
-- every DFM run: WHERE [status] = 'running' AND [run_started_at] < @threshold.
-- Filtered on 'running' so it stays tiny regardless of total log size.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_DFMRunLog_running'
      AND object_id = OBJECT_ID(N'[meta].[DFMRunLog]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_DFMRunLog_running]
    ON [meta].[DFMRunLog] ([run_started_at])
    WHERE [status] = 'running';
END
GO
