-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_ris_ydelse  (1:1 from IFDB.RIS_YDELSE — 27 source columns incl. XML CLOB)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_ris_ydelse'
)
BEGIN
    CREATE TABLE [arc].[ifdb_ris_ydelse] (
        [OBJEKT_ID]                NVARCHAR(50)    NOT NULL,
        [MODTIDSPUNKT]             NVARCHAR(100)   NULL,
        [SLETTET]                  NVARCHAR(22)    NULL,
        [PATIENT_ID]               NVARCHAR(50)    NULL,
        [ENCOUNTER_ID]             NVARCHAR(50)    NULL,
        [EPISODE_OF_CARE_ID]       NVARCHAR(50)    NULL,
        [TILSTEDEVAERELSE_ID]      NVARCHAR(50)    NULL,
        [XML]                      NVARCHAR(MAX)   NULL,
        [STAMAFD_KODE]             NVARCHAR(512)   NULL,
        [STAMAFD_NAVN]             NVARCHAR(512)   NULL,
        [STAMAFD_ORGKODE]          NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_KODE]    NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_NAVN]    NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_ORGKODE] NVARCHAR(512)   NULL,
        [UDFOERT_TIDSPUNKT]        NVARCHAR(100)   NULL,
        [OPDATERET_TID]            NVARCHAR(100)   NULL,
        [BESKED_ID]                NVARCHAR(512)   NULL,
        [FILNAVN]                  NVARCHAR(512)   NULL,
        [STATE]                    NVARCHAR(512)   NULL,
        [TYPE]                     NVARCHAR(512)   NULL,
        [RTEE]                     NVARCHAR(100)   NULL,
        [IGNORED]                  NVARCHAR(22)    NULL,
        [PERIOD_START]             NVARCHAR(100)   NULL,
        [PERIOD_END]               NVARCHAR(100)   NULL,
        [UDFOERT_ADMENHED_SORCODE] NVARCHAR(512)   NULL,
        [STAMAFD_SORCODE]          NVARCHAR(512)   NULL,
        [LPR3_IGNORED]             NVARCHAR(22)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)    NOT NULL,
        [dw_is_deleted]            BIT             NOT NULL CONSTRAINT [DF_ifdb_ris_ydelse_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ris_ydelse_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ris_ydelse_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_ris_ydelse]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ris_ydelse_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_ris_ydelse] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ris_ydelse_ifdb_OPDATERET_TID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_ris_ydelse]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ris_ydelse_ifdb_OPDATERET_TID]
        ON [arc].[ifdb_ris_ydelse] ([OPDATERET_TID] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ris_ydelse_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_ris_ydelse]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ris_ydelse_ifdb_PATIENT_ID]
        ON [arc].[ifdb_ris_ydelse] ([PATIENT_ID]);
END
GO
