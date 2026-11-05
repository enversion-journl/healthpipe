-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_absence  (1:1 from IFDB.ABSENCE)
--
-- Grain: one row per encounter per absence start. TYPE_CODE is not needed in
-- the key (measured: 78.951 rows, 78.951 distinct on (OBJECT_ID,
-- PERIOD_START), and the same count when TYPE_CODE is added).
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_absence'
)
BEGIN
    CREATE TABLE [arc].[ifdb_absence] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [PERIOD_START]                   NVARCHAR(100)   NOT NULL,
        [PERIOD_END]                     NVARCHAR(100)   NULL,
        [EXPECTED_RETURN]                NVARCHAR(100)   NULL,
        [TYPE_CODE]                      NVARCHAR(512)   NULL,
        [TYPE_TEXT]                      NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_absence_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_absence]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [PERIOD_START])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_absence_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_absence]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_absence_dw_timestamp_update]
        ON [arc].[ifdb_absence] ([dw_timestamp_update] DESC);
END
GO
