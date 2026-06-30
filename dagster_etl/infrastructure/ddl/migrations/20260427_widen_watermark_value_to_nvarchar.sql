-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Widen watermark columns from DATETIME2 to NVARCHAR(50)
--
-- Motivation: the audit-MAX delta strategy for KMD UdData stores INT
-- watermark values. Existing consumers store ISO datetime strings. Widening
-- to NVARCHAR(50) lets a single column hold both, with implicit conversion
-- preserving existing rows.
--
-- Affected tables/columns:
--   meta.etl_watermark_state.watermark_value   DATETIME2 -> NVARCHAR(50)
--   meta.etl_watermark_log.watermark_before    DATETIME2 -> NVARCHAR(50)
--   meta.etl_watermark_log.watermark_after     DATETIME2 -> NVARCHAR(50)
-- ============================================================================

-- ---- meta.etl_watermark_state.watermark_value ----

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'meta'
      AND TABLE_NAME   = 'etl_watermark_state'
      AND COLUMN_NAME  = 'watermark_value'
      AND DATA_TYPE    = 'datetime2'
)
BEGIN
    ALTER TABLE [meta].[etl_watermark_state]
        ALTER COLUMN [watermark_value] NVARCHAR(50) NOT NULL;
    PRINT 'Widened meta.etl_watermark_state.watermark_value to NVARCHAR(50)';
END
GO

-- ---- meta.etl_watermark_log.watermark_before ----

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'meta'
      AND TABLE_NAME   = 'etl_watermark_log'
      AND COLUMN_NAME  = 'watermark_before'
      AND DATA_TYPE    = 'datetime2'
)
BEGIN
    ALTER TABLE [meta].[etl_watermark_log]
        ALTER COLUMN [watermark_before] NVARCHAR(50) NULL;
    PRINT 'Widened meta.etl_watermark_log.watermark_before to NVARCHAR(50)';
END
GO

-- ---- meta.etl_watermark_log.watermark_after ----

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'meta'
      AND TABLE_NAME   = 'etl_watermark_log'
      AND COLUMN_NAME  = 'watermark_after'
      AND DATA_TYPE    = 'datetime2'
)
BEGIN
    ALTER TABLE [meta].[etl_watermark_log]
        ALTER COLUMN [watermark_after] NVARCHAR(50) NULL;
    PRINT 'Widened meta.etl_watermark_log.watermark_after to NVARCHAR(50)';
END
GO
