-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB (Encounter + Location + Epikrise + Udskrivningsrapport)
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
--
-- ifdb_encounter: delta-loaded via IFDB_MODIFICATION_DATETIME; UPSERT on OBJECT_ID.
-- ifdb_location:  full-load; UPSERT on (OBJECT_ID, PERIOD_START) with soft-delete.
--
-- The Epikrise + Udskrivningsrapport archive tables (US-FHIR-07) are appended
-- in their own banner section at the bottom of this file.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_encounter  (1:1 from IFDB.ENCOUNTER — 34 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_encounter'
)
BEGIN
    CREATE TABLE [arc].[ifdb_encounter] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [EPISODE_OF_CARE_ID]             NVARCHAR(50)    NULL,
        [PATIENT_ID]                     NVARCHAR(50)    NULL,
        [PAYMENT_INFORMATION_ID]         NVARCHAR(100)   NULL,
        [STATUS_CODE]                    NVARCHAR(512)   NULL,
        [STATUS_TEXT]                     NVARCHAR(100)   NULL,
        [CLASS_CODE]                     NVARCHAR(512)   NULL,
        [CLASS_TEXT]                      NVARCHAR(100)   NULL,
        [PRIORITY_CODE]                  NVARCHAR(512)   NULL,
        [PRIORITY_TEXT]                   NVARCHAR(100)   NULL,
        [CLINICAL_SPECIALITY_CODE]       NVARCHAR(512)   NULL,
        [CLINICAL_SPECIALITY_TEXT]        NVARCHAR(512)   NULL,
        [EPJI_CREATED_DATETIME]          NVARCHAR(100)   NULL,
        [EPJI_MODIFICATION_DATETIME]     NVARCHAR(100)   NULL,
        [IFDB_MODIFICATION_DATETIME]     NVARCHAR(100)   NULL,
        [BED]                            NVARCHAR(512)   NULL,
        [TEAM]                           NVARCHAR(512)   NULL,
        [BROUGHT_IN_BY]                  NVARCHAR(512)   NULL,
        [SERVICE_PROVIDER_CODE]          NVARCHAR(512)   NULL,
        [SERVICE_PROVIDER_NAME]          NVARCHAR(512)   NULL,
        [SERVICE_PROVIDER_ORGCODE]       NVARCHAR(512)   NULL,
        [SERVICE_PROVIDER_SORCODE]       NVARCHAR(512)   NULL,
        [PERIOD_START]                   NVARCHAR(100)   NULL,
        [PERIOD_END]                     NVARCHAR(100)   NULL,
        [WARD_CODE]                      NVARCHAR(512)   NULL,
        [WARD_NAME]                      NVARCHAR(512)   NULL,
        [WARD_ORGCODE]                   NVARCHAR(512)   NULL,
        [WARD_SORCODE]                   NVARCHAR(512)   NULL,
        [MUNICIPALITY_CARE]              NVARCHAR(512)   NULL,
        [EXPECTED_DISCHARGE]             NVARCHAR(100)   NULL,
        [DELETED]                        NVARCHAR(22)    NULL,
        [REFERRAL_ID]                    NVARCHAR(50)    NULL,
        [ROOM]                           NVARCHAR(512)   NULL,
        [TREATMENT_START]                NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_ifdb_encounter_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_encounter]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_encounter_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_encounter]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_encounter_dw_timestamp_update]
        ON [arc].[ifdb_encounter] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_encounter_IFDB_MODIFICATION_DATETIME'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_encounter]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_encounter_IFDB_MODIFICATION_DATETIME]
        ON [arc].[ifdb_encounter] ([IFDB_MODIFICATION_DATETIME] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_encounter_patient_id_ifdb'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_encounter]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_encounter_patient_id_ifdb]
        ON [arc].[ifdb_encounter] ([PATIENT_ID]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_location  (1:1 from IFDB.LOCATION — 11 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_location'
)
BEGIN
    CREATE TABLE [arc].[ifdb_location] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [LOCATION_CODE]                  NVARCHAR(512)   NULL,
        [LOCATION_NAME]                  NVARCHAR(512)   NULL,
        [LOCATION_ORGCODE]               NVARCHAR(512)   NULL,
        [LOCATION_SORCODE]               NVARCHAR(512)   NULL,
        [PERIOD_START]                   NVARCHAR(100)   NOT NULL,
        [PERIOD_END]                     NVARCHAR(100)   NULL,
        [DAYRATE_CODE]                   NVARCHAR(512)   NULL,
        [DAYRATE_TEXT]                   NVARCHAR(512)   NULL,
        [OUTPATIENT_VISITRATE_CODE]      NVARCHAR(512)   NULL,
        [OUTPATIENT_VISITRATE_TEXT]      NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_ifdb_location_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_location]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [PERIOD_START])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_location_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_location]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_location_dw_timestamp_update]
        ON [arc].[ifdb_location] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_location_LOCATION_CODE'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_location]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_location_LOCATION_CODE]
        ON [arc].[ifdb_location] ([LOCATION_CODE]);
END
GO


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


-- ----------------------------------------------------------------------------
-- arc.ifdb_epikrise_modtager  (1:1 from IFDB.EPIKRISE_MODTAGER — 5 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_epikrise_modtager'
)
BEGIN
    CREATE TABLE [arc].[ifdb_epikrise_modtager] (
        [OBJEKT_ID]                       NVARCHAR(50)    NOT NULL,
        [EPIKRISE_ID]                     NVARCHAR(50)    NULL,
        [MODTAGERTYPE]                    NVARCHAR(50)    NULL,
        [NAVN]                            NVARCHAR(512)   NULL,
        [ADRESSE]                         NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_epikrise_modtager_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_epikrise_modtager_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_modtager_ifdb_EPIKRISE_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise_modtager]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_modtager_ifdb_EPIKRISE_ID]
        ON [arc].[ifdb_epikrise_modtager] ([EPIKRISE_ID]);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_modtager_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise_modtager]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_modtager_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_epikrise_modtager] ([dw_timestamp_update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_epikrise_tekst  (1:1 from IFDB.EPIKRISE_TEKST — 4 source columns;
--                           archive only, TEKST is CLOB → NVARCHAR(MAX))
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_epikrise_tekst'
)
BEGIN
    CREATE TABLE [arc].[ifdb_epikrise_tekst] (
        [OBJEKT_ID]                       NVARCHAR(50)    NOT NULL,
        [EPIKRISE_ID]                     NVARCHAR(50)    NULL,
        [OVERSKRIFT]                      NVARCHAR(512)   NULL,
        [TEKST]                           NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_epikrise_tekst_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_epikrise_tekst_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_tekst_ifdb_EPIKRISE_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise_tekst]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_tekst_ifdb_EPIKRISE_ID]
        ON [arc].[ifdb_epikrise_tekst] ([EPIKRISE_ID]);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_tekst_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise_tekst]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_tekst_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_epikrise_tekst] ([dw_timestamp_update] DESC);
END
GO


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


-- ----------------------------------------------------------------------------
-- arc.ifdb_rehabilitation_plan_receiver
--   (1:1 from IFDB.REHABILITATION_PLAN_RECEIVER — 9 source columns)
--   No PK in source; composite UPSERT key (OBJECT_ID, RECEIVER_YDER_NUMMER,
--   RECEIVER_AFDELING_KODE, TRANSMISSION_TIME) is MERGE-safe because the extract
--   NVL-normalizes the two nullable identity columns to the "__NULL__" sentinel
--   token — Oracle treats '' as NULL, so an empty-string sentinel is impossible.
--   The 84 identity-less duplicate groups collapse deliberately. Full-load +
--   soft-delete. Index rule: ONLY on dw_timestamp_update.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_rehabilitation_plan_receiver'
)
BEGIN
    CREATE TABLE [arc].[ifdb_rehabilitation_plan_receiver] (
        [OBJECT_ID]                       NVARCHAR(50)    NOT NULL,
        [RECEIVER_YDER_NUMMER]            NVARCHAR(50)    NOT NULL,
        [RECEIVER_AFDELING_KODE]          NVARCHAR(512)   NOT NULL,
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
            PRIMARY KEY CLUSTERED (
                [OBJECT_ID], [RECEIVER_YDER_NUMMER], [RECEIVER_AFDELING_KODE], [TRANSMISSION_TIME]
            )
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
