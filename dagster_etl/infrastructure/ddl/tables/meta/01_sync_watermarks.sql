-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Sync Watermarks Table
-- Tracks last successful sync timestamp per table for delta (watermark) sync.
-- Used by SyncStateManager in DSO.load.sync_state
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'sync_watermarks'
)
BEGIN
    CREATE TABLE [meta].[sync_watermarks] (
        schema_name           NVARCHAR(128)   NOT NULL,
        table_name            NVARCHAR(128)   NOT NULL,
        last_sync_timestamp   DATETIME2(0)    NOT NULL,
        last_sync_rows        INT             NULL,
        last_sync_duration_ms INT             NULL,
        last_sync_status      NVARCHAR(50)    NULL,
        created_at            DATETIME2(0)    NOT NULL DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        updated_at            DATETIME2(0)    NOT NULL DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        CONSTRAINT PK_sync_watermarks PRIMARY KEY (schema_name, table_name)
    );
END
GO
