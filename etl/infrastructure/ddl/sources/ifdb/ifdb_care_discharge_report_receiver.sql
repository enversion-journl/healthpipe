-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_care_discharge_report_receiver
--   (1:1 from IFDB.CARE_DISCHARGE_REPORT_RECEIVER — 10 source columns)
--   Composite UPSERT key (OBJECT_ID, RECEIVER_AFDELING_KODE, CCRECEIVER) —
--   all 100% populated. Full-load + soft-delete.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_care_discharge_report_receiver'
)
BEGIN
    CREATE TABLE [arc].[ifdb_care_discharge_report_receiver] (
        [OBJECT_ID]                       NVARCHAR(50)    NOT NULL,
        [RECEIVER_AFDELING_KODE]          NVARCHAR(512)   NOT NULL,
        [CCRECEIVER]                      NVARCHAR(22)    NOT NULL,
        [STATE_KODE]                      NVARCHAR(512)   NULL,
        [STATE_KODETEXT]                  NVARCHAR(100)   NULL,
        [TRANSMISSION_TIME]               NVARCHAR(100)   NULL,
        [RECEIVER_YDER_NAVN]              NVARCHAR(512)   NULL,
        [RECEIVER_YDER_NUMMER]            NVARCHAR(50)    NULL,
        [RECEIVER_AFDELING_KODETEKST]     NVARCHAR(100)   NULL,
        [RECEIVER_AFDELING_SORKODE]       NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_care_discharge_report_receiver_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_care_discharge_report_receiver_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [RECEIVER_AFDELING_KODE], [CCRECEIVER])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_care_discharge_report_receiver_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_care_discharge_report_receiver]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_care_discharge_report_receiver_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_care_discharge_report_receiver] ([dw_timestamp_update] DESC);
END
GO
