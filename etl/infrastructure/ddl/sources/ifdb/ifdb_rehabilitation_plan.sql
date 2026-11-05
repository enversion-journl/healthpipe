-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_rehabilitation_plan  (1:1 from IFDB.REHABILITATION_PLAN — 13 cols)
--   Genoptræningsplan (G-GOP) → ServiceRequest. Delta on UPDATE_TIME
--   (upper bound MODIFICATION_TIME). Faithful archive (also the
--   100%-NULL SENDER_YDER_* columns). Soft-delete via DELETED.
--   Index rule: index ONLY on dw_timestamp_update (the DWH delta
--   column) — NOT on the IFDB source-delta column UPDATE_TIME (deliberate
--   deviation from arc.ifdb_encounter / arc.ifdb_epikrise). PATIENT_ID index allowed.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_rehabilitation_plan'
)
BEGIN
    CREATE TABLE [arc].[ifdb_rehabilitation_plan] (
        [OBJECT_ID]                       NVARCHAR(50)    NOT NULL,
        [PATIENT_ID]                      NVARCHAR(50)    NULL,
        [TYPE_KODETEKST]                  NVARCHAR(200)   NULL,
        [PLAN_VERSION]                    NVARCHAR(50)    NULL,
        [NOTIFICATION_IDENTIFIER]         NVARCHAR(50)    NULL,
        [SENDER_YDER_NAVN]                NVARCHAR(512)   NULL,
        [SENDER_YDER_NUMMER]              NVARCHAR(50)    NULL,
        [SENDER_AFDELING_KODE]            NVARCHAR(512)   NULL,
        [SENDER_AFDELING_KODETEKST]       NVARCHAR(100)   NULL,
        [SENDER_AFDELING_SORKODE]         NVARCHAR(512)   NULL,
        [DELETED]                         NVARCHAR(22)    NULL,
        [MODIFICATION_TIME]               NVARCHAR(100)   NULL,
        [UPDATE_TIME]                     NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_rehabilitation_plan_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_rehabilitation_plan_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_rehabilitation_plan_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_rehabilitation_plan_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_rehabilitation_plan] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_rehabilitation_plan_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_rehabilitation_plan_ifdb_PATIENT_ID]
        ON [arc].[ifdb_rehabilitation_plan] ([PATIENT_ID]);
END
GO
