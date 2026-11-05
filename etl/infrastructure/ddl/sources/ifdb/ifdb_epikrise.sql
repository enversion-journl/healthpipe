-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB (Epikrise + Udskrivningsrapport)
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
-- All source values land as strings (mirrors the Oracle→MSSQL stringify path).
--
-- epikrise_ifdb:                    delta on OPDATERET_TID; UPSERT on OBJEKT_ID.
-- epikrise_modtager_ifdb:           inner-join-delta via EPIKRISE; UPSERT on OBJEKT_ID.
-- epikrise_tekst_ifdb:              inner-join-delta via EPIKRISE (archive only, CLOB).
-- care_discharge_report_ifdb:       delta on UPDATE_TIME; UPSERT on OBJECT_ID.
-- care_discharge_report_receiver_ifdb:
--                                   full-load; UPSERT on
--                                   (OBJECT_ID, RECEIVER_AFDELING_KODE, CCRECEIVER)
--                                   with soft-delete detection.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_epikrise  (1:1 from IFDB.EPIKRISE — 47 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_epikrise'
)
BEGIN
    CREATE TABLE [arc].[ifdb_epikrise] (
        [OBJEKT_ID]                       NVARCHAR(50)    NOT NULL,
        [MODTIDSPUNKT]                    NVARCHAR(100)   NULL,
        [SLETTET]                         NVARCHAR(22)    NULL,
        [PATIENT_ID]                      NVARCHAR(50)    NULL,
        [TILSTEDEVAERELSE_ID]             NVARCHAR(50)    NULL,
        [EPIKRISETYPE_KODE]               NVARCHAR(512)   NULL,
        [EPIKRISETYPE_KODETEKST]          NVARCHAR(100)   NULL,
        [EPIKRISESTATUS_KODE]             NVARCHAR(512)   NULL,
        [EPIKRISESTATUS_KODETEKST]        NVARCHAR(100)   NULL,
        [FOERSTE_AFSENDELSESTIDSPUNKT]    NVARCHAR(100)   NULL,
        [AFSENDER_ADMENHED_KODE]          NVARCHAR(512)   NULL,
        [AFSENDER_ADMENHED_NAVN]          NVARCHAR(512)   NULL,
        [AFSENDER_ADMENHED_ORGKODE]       NVARCHAR(512)   NULL,
        [KORREKTION_AF_ID]                NVARCHAR(50)    NULL,
        [UNDERSKRIVER_PERSID]             NVARCHAR(512)   NULL,
        [UNDERSKRIVER_PERSNAVN]           NVARCHAR(512)   NULL,
        [UNDERSKRIVER_BRUGERROLLE]        NVARCHAR(512)   NULL,
        [UNDERSKRIVER_ADMENHED_KODE]      NVARCHAR(512)   NULL,
        [UNDERSKRIVER_ADMENHED_NAVN]      NVARCHAR(512)   NULL,
        [UNDERSKRIVER_ADMENHED_ORGKODE]   NVARCHAR(512)   NULL,
        [UNDERSKRIVER_TIDSPUNKT]          NVARCHAR(100)   NULL,
        [GODKENDER_PERSID]                NVARCHAR(512)   NULL,
        [GODKENDER_PERSNAVN]              NVARCHAR(512)   NULL,
        [GODKENDER_BRUGERROLLE]           NVARCHAR(512)   NULL,
        [GODKENDER_ADMENHED_KODE]         NVARCHAR(512)   NULL,
        [GODKENDER_ADMENHED_NAVN]         NVARCHAR(512)   NULL,
        [GODKENDER_ADMENHED_ORGKODE]      NVARCHAR(512)   NULL,
        [GODKENDER_TIDSPUNKT]             NVARCHAR(100)   NULL,
        [REGISTRANT_PERSID]               NVARCHAR(512)   NULL,
        [REGISTRANT_PERSNAVN]             NVARCHAR(512)   NULL,
        [REGISTRANT_BRUGERROLLE]          NVARCHAR(512)   NULL,
        [REGISTRANT_ADMENHED_KODE]        NVARCHAR(512)   NULL,
        [REGISTRANT_ADMENHED_NAVN]        NVARCHAR(512)   NULL,
        [REGISTRANT_ADMENHED_ORGKODE]     NVARCHAR(512)   NULL,
        [REGISTRANT_TIDSPUNKT]            NVARCHAR(100)   NULL,
        [OPDATERET_TID]                   NVARCHAR(100)   NULL,
        [ENCOUNTER_ID]                    NVARCHAR(50)    NULL,
        [PRIORITET]                       NVARCHAR(100)   NULL,
        [ANBEFALET_OPFOLGNING]            NVARCHAR(1750)  NULL,
        [AFSENDER_ADMENHED_SORKODE]       NVARCHAR(512)   NULL,
        [UNDERSKRIVER_ADMENHED_SORKODE]   NVARCHAR(512)   NULL,
        [GODKENDER_ADMENHED_SORKODE]      NVARCHAR(512)   NULL,
        [REGISTRANT_ADMENHED_SORKODE]     NVARCHAR(512)   NULL,
        [UNDERSKRIVER_BRUGERROLLE_NAME]   NVARCHAR(512)   NULL,
        [GODKENDER_BRUGERROLLE_NAME]      NVARCHAR(512)   NULL,
        [REGISTRANT_BRUGERROLLE_NAME]     NVARCHAR(512)   NULL,
        [EPISODE_OF_CARE_ID]              NVARCHAR(50)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_epikrise_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_epikrise_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_epikrise] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_ifdb_OPDATERET_TID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_ifdb_OPDATERET_TID]
        ON [arc].[ifdb_epikrise] ([OPDATERET_TID] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_ifdb_PATIENT_ID]
        ON [arc].[ifdb_epikrise] ([PATIENT_ID]);
END
GO
