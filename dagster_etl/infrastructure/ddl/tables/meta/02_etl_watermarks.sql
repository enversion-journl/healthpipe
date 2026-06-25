-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ETL Watermarks
-- Used by EtlWatermarkManager in core.etl_watermark_manager
--
--   meta.etl_watermark_state — current high-water mark per entity
--                              (read by extract assets, advanced by archive
--                              assets after a successful MERGE).
--   meta.etl_watermark_log   — run lifecycle log (start → success / failed).
--
-- watermark_value / watermark_before / watermark_after are NVARCHAR(50) so
-- they can hold either a stringified int (audit-MAX delta) or an ISO datetime
-- (timestamp delta).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- meta.etl_watermark_state — current high-water mark per entity
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'etl_watermark_state'
)
BEGIN
    CREATE TABLE [meta].[etl_watermark_state] (
        [entity_type]      NVARCHAR(50)   NOT NULL,
        [schema_name]      NVARCHAR(128)  NOT NULL,
        [table_name]       NVARCHAR(128)  NOT NULL,
        [watermark_value]  NVARCHAR(50)   NOT NULL,
        [updated_at]       DATETIME2      NOT NULL
            DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        CONSTRAINT [PK_etl_watermark_state]
            PRIMARY KEY CLUSTERED ([entity_type], [schema_name], [table_name])
    );
END
GO


-- ----------------------------------------------------------------------------
-- meta.etl_watermark_log — run lifecycle log
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'etl_watermark_log'
)
BEGIN
    CREATE TABLE [meta].[etl_watermark_log] (
        [id]                BIGINT IDENTITY(1,1) NOT NULL,
        [entity_type]       NVARCHAR(50)   NOT NULL,
        [schema_name]       NVARCHAR(128)  NOT NULL,
        [table_name]        NVARCHAR(128)  NOT NULL,
        [run_started_at]    DATETIME2      NOT NULL,
        [run_completed_at]  DATETIME2      NULL,
        [watermark_before]  NVARCHAR(50)   NULL,
        [watermark_after]   NVARCHAR(50)   NULL,
        [rows_affected]     INT            NULL,
        [status]            NVARCHAR(50)   NOT NULL,
        [dagster_run_id]    NVARCHAR(128)  NULL,
        CONSTRAINT [PK_etl_watermark_log]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO
