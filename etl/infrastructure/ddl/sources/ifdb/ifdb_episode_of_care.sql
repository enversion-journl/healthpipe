-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_episode_of_care  (1:1 from IFDB.EPISODE_OF_CARE — 17 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_episode_of_care'
)
BEGIN
    CREATE TABLE [arc].[ifdb_episode_of_care] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [PATIENT_ID]                     NVARCHAR(50)    NULL,
        [STATUS]                         NVARCHAR(512)   NULL,
        [PERIOD_START]                   NVARCHAR(100)   NULL,
        [PERIOD_END]                     NVARCHAR(100)   NULL,
        [MANAGING_ORGANIZATION_CODE]     NVARCHAR(512)   NULL,
        [MANAGING_ORGANIZATION_NAME]     NVARCHAR(512)   NULL,
        [MANAGING_ORGANIZATION_SORCODE]  NVARCHAR(512)   NULL,
        [CARE_MANAGER_ID]                NVARCHAR(512)   NULL,
        [CARE_MANAGER_NAME]              NVARCHAR(512)   NULL,
        [PREVIOUS_EOC_ID]                NVARCHAR(50)    NULL,
        [PREVIOUS_EOC_REF_TYPE_CODE]     NVARCHAR(512)   NULL,
        [HEALTHISSUE_ID]                 NVARCHAR(50)    NULL,
        [DISCHARGE_DISPOSITION_CODE]     NVARCHAR(512)   NULL,
        [DELETED]                        NVARCHAR(22)    NULL,
        [EPJI_MODIFICATION_DATETIME]     NVARCHAR(100)   NULL,
        [IFDB_MODIFICATION_DATETIME]     NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_ifdb_episode_of_care_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_episode_of_care]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_episode_of_care_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_episode_of_care]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_episode_of_care_dw_timestamp_update]
        ON [arc].[ifdb_episode_of_care] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_episode_of_care_IFDB_MODIFICATION_DATETIME'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_episode_of_care]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_episode_of_care_IFDB_MODIFICATION_DATETIME]
        ON [arc].[ifdb_episode_of_care] ([IFDB_MODIFICATION_DATETIME] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_episode_of_care_patient_id_ifdb'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_episode_of_care]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_episode_of_care_patient_id_ifdb]
        ON [arc].[ifdb_episode_of_care] ([PATIENT_ID]);
END
GO
