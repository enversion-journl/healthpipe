-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_location  (1:1 from IFDB.LOCATION — 11 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_location'
)
BEGIN
    CREATE TABLE [arc].[ifdb_location] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [LOCATION_CODE]                  NVARCHAR(512)   NULL,
        [LOCATION_NAME]                  NVARCHAR(512)   NULL,
        [LOCATION_ORGCODE]               NVARCHAR(512)   NULL,
        [LOCATION_SORCODE]               NVARCHAR(512)   NULL,
        [PERIOD_START]                   NVARCHAR(100)   NOT NULL,
        [PERIOD_END]                     NVARCHAR(100)   NULL,
        [DAYRATE_CODE]                   NVARCHAR(512)   NULL,
        [DAYRATE_TEXT]                   NVARCHAR(512)   NULL,
        [OUTPATIENT_VISITRATE_CODE]      NVARCHAR(512)   NULL,
        [OUTPATIENT_VISITRATE_TEXT]      NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_ifdb_location_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_location]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [PERIOD_START])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_location_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_location]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_location_dw_timestamp_update]
        ON [arc].[ifdb_location] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_location_LOCATION_CODE'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_location]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_location_LOCATION_CODE]
        ON [arc].[ifdb_location] ([LOCATION_CODE]);
END
GO
