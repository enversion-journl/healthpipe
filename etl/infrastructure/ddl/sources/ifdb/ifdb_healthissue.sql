-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_healthissue  (1:1 from IFDB.HEALTHISSUE — 15 source columns)
--   Lookup archive for the EpisodeOfCare healthissue_code enrichment. Delta on
--   IFDB_MODIFICATION_DATETIME (upper bound EPJI_MODIFICATION_DATETIME).
--   Index rule: index ONLY on dw_timestamp_update — the code lookup is on
--   OBJECT_ID (the clustered PK), and NOT on the IFDB source-delta column
--   (deliberate deviation from arc.ifdb_encounter / arc.ifdb_epikrise).
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_healthissue'
)
BEGIN
    CREATE TABLE [arc].[ifdb_healthissue] (
        [OBJECT_ID]                       NVARCHAR(50)    NOT NULL,
        [PATIENT_ID]                      NVARCHAR(100)   NULL,
        [HEALTHISSUE_CODE]                NVARCHAR(512)   NULL,
        [HEALTHISSUE_TEXT]                NVARCHAR(512)   NULL,
        [CODE_ELABORATION]                NVARCHAR(512)   NULL,
        [CLINICAL_STATUS]                 NVARCHAR(512)   NULL,
        [PERIOD_START]                    NVARCHAR(100)   NULL,
        [PERIOD_END]                      NVARCHAR(100)   NULL,
        [HEALTHISSUE_MANAGER_PERSID]      NVARCHAR(512)   NULL,
        [HEALTHISSUE_MANAGER_PERSNAME]    NVARCHAR(512)   NULL,
        [DELETED]                         NVARCHAR(22)    NULL,
        [EPJI_CREATED_DATETIME]           NVARCHAR(100)   NULL,
        [EPJI_MODIFICATION_DATETIME]      NVARCHAR(100)   NULL,
        [IFDB_MODIFICATION_DATETIME]      NVARCHAR(100)   NULL,
        [HEALTHISSUE_EXTERNAL_CODE]       NVARCHAR(50)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_healthissue_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_healthissue_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_healthissue_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_healthissue]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_healthissue_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_healthissue] ([dw_timestamp_update] DESC);
END
GO
