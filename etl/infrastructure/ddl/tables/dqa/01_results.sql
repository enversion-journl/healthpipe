-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- DQA Results store: one row per check execution, for the cross-run /
-- cross-environment overview (dqa_summary rollup + dashboards).
--
-- Written by dqa/result.py on every check run (best-effort). This file defines
-- the table + the lookup index; the dqa.v_latest convenience view lives in
-- views/dqa/01_v_latest.sql. Owned by the staging_infra_dqa asset. Idempotent.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dqa')
    EXEC ('CREATE SCHEMA [dqa]');
GO

IF OBJECT_ID(N'[dqa].[Results]', N'U') IS NULL
    CREATE TABLE [dqa].[Results] (
        [result_id]       BIGINT IDENTITY(1,1) PRIMARY KEY,
        [run_id]          NVARCHAR(64)  NULL,
        [checked_at]      DATETIME2     NOT NULL CONSTRAINT [DF_Results_checked_at] DEFAULT SYSUTCDATETIME(),
        [environment]     NVARCHAR(128) NULL,
        [track]           NVARCHAR(16)  NULL,
        [subject]         NVARCHAR(128) NOT NULL,
        [check_type]      NVARCHAR(64)  NOT NULL,
        [name_suffix]     NVARCHAR(64)  NULL,
        [severity]        NVARCHAR(16)  NOT NULL,
        [cadence]         NVARCHAR(16)  NOT NULL,
        [passed]          BIT           NOT NULL,
        [checked_count]   BIGINT        NULL,
        [violation_count] BIGINT        NULL,
        [examples_json]   NVARCHAR(MAX) NULL,
        [detail_json]     NVARCHAR(MAX) NULL
    );
GO

-- Widen [environment] on pre-existing tables (feature-branch postfixes exceed 32
-- chars; dqa_environment() truncates to 128 to match). COL_LENGTH is bytes:
-- NVARCHAR(32) -> 64, NVARCHAR(128) -> 256. Requires dropping the covering index
-- first (it includes [environment]); recreated below. Idempotent.
IF COL_LENGTH(N'[dqa].[Results]', 'environment') < 256
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_dqa_Results_latest'
          AND object_id = OBJECT_ID(N'[dqa].[Results]')
    )
        DROP INDEX [IX_dqa_Results_latest] ON [dqa].[Results];
    ALTER TABLE [dqa].[Results] ALTER COLUMN [environment] NVARCHAR(128) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_dqa_Results_latest'
      AND object_id = OBJECT_ID(N'[dqa].[Results]')
)
    CREATE NONCLUSTERED INDEX [IX_dqa_Results_latest]
        ON [dqa].[Results] ([environment], [subject], [check_type], [name_suffix], [checked_at] DESC);
GO
