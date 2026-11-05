-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_care_discharge_report  (1:1 from IFDB.CARE_DISCHARGE_REPORT — 11 cols)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_care_discharge_report'
)
BEGIN
    CREATE TABLE [arc].[ifdb_care_discharge_report] (
        [OBJECT_ID]                       NVARCHAR(50)    NOT NULL,
        [PATIENT_ID]                      NVARCHAR(50)    NULL,
        [NOTIFICATION_IDENTIFIER]         NVARCHAR(50)    NULL,
        [SENDER_YDER_NAVN]                NVARCHAR(512)   NULL,
        [SENDER_YDER_NUMMER]              NVARCHAR(50)    NULL,
        [SENDER_AFDELING_KODE]            NVARCHAR(512)   NULL,
        [SENDER_AFDELING_KODETEKST]       NVARCHAR(100)   NULL,
        [MODIFICATION_TIME]               NVARCHAR(100)   NULL,
        [UPDATE_TIME]                     NVARCHAR(100)   NULL,
        [DELETED]                         NVARCHAR(22)    NULL,
        [SENDER_AFDELING_SORKODE]         NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_care_discharge_report_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_care_discharge_report_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_care_discharge_report_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_care_discharge_report]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_care_discharge_report_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_care_discharge_report] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_care_discharge_report_ifdb_UPDATE_TIME'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_care_discharge_report]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_care_discharge_report_ifdb_UPDATE_TIME]
        ON [arc].[ifdb_care_discharge_report] ([UPDATE_TIME] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_care_discharge_report_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_care_discharge_report]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_care_discharge_report_ifdb_PATIENT_ID]
        ON [arc].[ifdb_care_discharge_report] ([PATIENT_ID]);
END
GO
