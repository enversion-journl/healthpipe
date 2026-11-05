-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_rehabilitation_plan_receiver
--   (1:1 from IFDB.REHABILITATION_PLAN_RECEIVER — 9 source columns + modtager_noegle)
--   No PK in source. modtager_noegle = COALESCE(RECEIVER_YDER_NUMMER,
--   RECEIVER_AFDELING_KODE, RECEIVER_AFDELING_SORKODE), derived in the extract. A
--   receiver carries a yder number or a department code, never both, and otherwise
--   only a SOR code; the three come from different code systems, so
--   (OBJECT_ID, modtager_noegle) is the UPSERT key. Full-load + soft-delete.
--   Index rule: ONLY on dw_timestamp_update.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_rehabilitation_plan_receiver'
)
BEGIN
    CREATE TABLE [arc].[ifdb_rehabilitation_plan_receiver] (
        [OBJECT_ID]                       NVARCHAR(50)    NOT NULL,
        -- 255 = fhir.ServiceRequest_Recipient.recipient_key, which carries modtager_noegle.
        [modtager_noegle]                 NVARCHAR(255)   NOT NULL,
        [RECEIVER_YDER_NUMMER]            NVARCHAR(50)    NULL,
        [RECEIVER_AFDELING_KODE]          NVARCHAR(512)   NULL,
        [TRANSMISSION_TIME]               NVARCHAR(100)   NOT NULL,
        [RECEIVER_YDER_NAVN]              NVARCHAR(512)   NULL,
        [RECEIVER_AFDELING_SORKODE]       NVARCHAR(512)   NULL,
        [RECEIVER_AFDELING_KODETEKST]     NVARCHAR(200)   NULL,
        [STATE_KODE]                      NVARCHAR(512)   NULL,
        [STATE_KODETEXT]                  NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_rehabilitation_plan_receiver_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_rehabilitation_plan_receiver_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [modtager_noegle])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_rehabilitation_plan_receiver_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_rehabilitation_plan_receiver_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_rehabilitation_plan_receiver] ([dw_timestamp_update] DESC);
END
GO
