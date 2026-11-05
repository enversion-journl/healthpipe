-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_encounter_bed_space_history  (1:1 from IFDB.ENCOUNTER_BED_SPACE_HISTORY)
--
-- Grain: one row per encounter per registration start. BED_ID is deliberately
-- NOT part of the key: the source is unique on (OBJECT_ID,
-- PERIOD_START_REGISTRATION) alone (measured: 1.312.474 rows, 1.312.474
-- distinct), because an encounter is never in two beds at the same instant.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_encounter_bed_space_history'
)
BEGIN
    CREATE TABLE [arc].[ifdb_encounter_bed_space_history] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [PERIOD_START_REGISTRATION]      NVARCHAR(100)   NOT NULL,
        [PERIOD_END_REGISTRATION]        NVARCHAR(100)   NULL,
        [BED_ID]                         NVARCHAR(512)   NULL,
        [BED]                            NVARCHAR(512)   NULL,
        [ROOM]                           NVARCHAR(512)   NULL,
        [LOCATION_CODE]                  NVARCHAR(512)   NULL,
        [LOCATION_NAME]                  NVARCHAR(512)   NULL,
        [LOCATION_ORGCODE]               NVARCHAR(512)   NULL,
        [LOCATION_SORCODE]               NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_encounter_bed_space_history_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_encounter_bed_space_history]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [PERIOD_START_REGISTRATION])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_ebsh_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_encounter_bed_space_history]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_ebsh_dw_timestamp_update]
        ON [arc].[ifdb_encounter_bed_space_history] ([dw_timestamp_update] DESC);
END
GO

-- The bed Location transform reads distinct BED_ID from this table, and the
-- Encounter transform reads it per encounter batch. Both are covered by the
-- clustered PK on OBJECT_ID; BED_ID needs its own index for the first.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_ebsh_BED_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_encounter_bed_space_history]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_ebsh_BED_ID]
        ON [arc].[ifdb_encounter_bed_space_history] ([BED_ID])
        INCLUDE ([BED], [ROOM], [LOCATION_SORCODE], [PERIOD_START_REGISTRATION]);
END
GO
