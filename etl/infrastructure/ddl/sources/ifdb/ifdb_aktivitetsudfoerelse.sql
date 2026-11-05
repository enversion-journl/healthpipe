-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_aktivitetsudfoerelse  (1:1 from IFDB.AKTIVITETSUDFOERELSE — 31 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_aktivitetsudfoerelse'
)
BEGIN
    CREATE TABLE [arc].[ifdb_aktivitetsudfoerelse] (
        [OBJEKT_ID]                NVARCHAR(50)    NOT NULL,
        [OPDATERET_TID]            NVARCHAR(100)   NULL,
        [SLETTET]                  NVARCHAR(22)    NULL,
        [MODTIDSPUNKT]             NVARCHAR(100)   NULL,
        [PATIENT_ID]               NVARCHAR(50)    NULL,
        [UDFOERT_PERSID]           NVARCHAR(512)   NULL,
        [UDFOERT_PERSNAVN]         NVARCHAR(512)   NULL,
        [UDFOERT_BRUGERROLLE]      NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_KODE]    NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_NAVN]    NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_ORGKODE] NVARCHAR(512)   NULL,
        [UDFOERT_TIDSPUNKT]        NVARCHAR(100)   NULL,
        [KLADDE]                   NVARCHAR(22)    NULL,
        [AKTIVITET_ID]             NVARCHAR(50)    NULL,
        [OPRET_PERSID]             NVARCHAR(512)   NULL,
        [OPRET_PERSNAVN]           NVARCHAR(512)   NULL,
        [OPRET_BRUGERROLLE]        NVARCHAR(512)   NULL,
        [OPRET_ADMENHED_KODE]      NVARCHAR(512)   NULL,
        [OPRET_ADMENHED_NAVN]      NVARCHAR(512)   NULL,
        [OPRET_ADMENHED_ORGKODE]   NVARCHAR(512)   NULL,
        [OPRET_TIDSPUNKT]          NVARCHAR(100)   NULL,
        [AENDRET_PERSID]           NVARCHAR(512)   NULL,
        [AENDRET_PERSNAVN]         NVARCHAR(512)   NULL,
        [AENDRET_BRUGERROLLE]      NVARCHAR(512)   NULL,
        [AENDRET_ADMENHED_KODE]    NVARCHAR(512)   NULL,
        [AENDRET_ADMENHED_NAVN]    NVARCHAR(512)   NULL,
        [AENDRET_ADMENHED_ORGKODE] NVARCHAR(512)   NULL,
        [AENDRET_TIDSPUNKT]        NVARCHAR(100)   NULL,
        [FORTR_NIVEAU]             NVARCHAR(512)   NULL,
        [FORTR_GRUPPE_ID]          NVARCHAR(512)   NULL,
        [FORTR_GRUPPE_NAVN]        NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)    NOT NULL,
        [dw_is_deleted]            BIT             NOT NULL CONSTRAINT [DF_ifdb_aktivitetsudfoerelse_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_aktivitetsudfoerelse_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_aktivitetsudfoerelse_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_aktivitetsudfoerelse]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_aktivitetsudfoerelse_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_aktivitetsudfoerelse] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_aktivitetsudfoerelse_ifdb_OPDATERET_TID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_aktivitetsudfoerelse]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_aktivitetsudfoerelse_ifdb_OPDATERET_TID]
        ON [arc].[ifdb_aktivitetsudfoerelse] ([OPDATERET_TID] DESC);
END
GO
