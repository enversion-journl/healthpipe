-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB (Patient + EpisodeOfCare)
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
--
-- ifdb_patient:          delta-loaded via MODTIDSPUNKT; UPSERT on PATIENT_ID.
-- ifdb_episode_of_care:  delta-loaded via IFDB_MODIFICATION_DATETIME; UPSERT on OBJECT_ID.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_patient  (1:1 from IFDB.PATIENT_OPLYSNINGER_COLUMNA — 25 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_patient'
)
BEGIN
    CREATE TABLE [arc].[ifdb_patient] (
        [PATIENT_ID]                     NVARCHAR(50)    NOT NULL,
        [CPR]                            NVARCHAR(20)    NULL,
        [FORNAVNE]                       NVARCHAR(512)   NULL,
        [EFTERNAVN]                      NVARCHAR(512)   NULL,
        [VEJ]                            NVARCHAR(512)   NULL,
        [POSTNR]                         NVARCHAR(20)    NULL,
        [STEDNAVN]                       NVARCHAR(512)   NULL,
        [KOMMUNEKODE]                    NVARCHAR(100)   NULL,
        [KOEN_KODE]                      NVARCHAR(100)   NULL,
        [KOEN_KODETEKST]                 NVARCHAR(512)   NULL,
        [FOEDSELSDATO]                   NVARCHAR(100)   NULL,
        [LAEGENAVN]                      NVARCHAR(512)   NULL,
        [LAEGENUMMER]                    NVARCHAR(100)   NULL,
        [PERSON_STATUSKODE]              NVARCHAR(512)   NULL,
        [PERSON_STATUSKODETEKST]         NVARCHAR(512)   NULL,
        [HAR_ADRESSEBESKYTTELSE]         NVARCHAR(100)   NULL,
        [OENSKER_DISKRETION]             NVARCHAR(100)   NULL,
        [BEMAERKNINGER]                  NVARCHAR(MAX)   NULL,
        [PERSON_DISTRIKTKODE]            NVARCHAR(512)   NULL,
        [PERSON_DISTRIKTKODETEKST]       NVARCHAR(512)   NULL,
        [PERSON_SOCIALDISTRIKTKODE]      NVARCHAR(512)   NULL,
        [PERSON_SOCIALDISTRIKTKODETEKST] NVARCHAR(512)   NULL,
        [OPDATERET_TID]                  NVARCHAR(100)   NULL,
        [SLETTET]                        NVARCHAR(22)    NULL,
        [MODTIDSPUNKT]                   NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_ifdb_patient_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_patient]
            PRIMARY KEY CLUSTERED ([PATIENT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_patient_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_patient]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_patient_dw_timestamp_update]
        ON [arc].[ifdb_patient] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_patient_MODTIDSPUNKT'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_patient]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_patient_MODTIDSPUNKT]
        ON [arc].[ifdb_patient] ([MODTIDSPUNKT] DESC);
END
GO

-- Supports the CPR-dedup query in Patient_IFDB_dfm._build_patient_query, which
-- joins windowed CPRs back to the whole archive (so an active patient is never
-- clobbered by a touched-but-deleted duplicate). Also speeds the CPR fallback
-- in prepare_resources_for_upsert. Without it the join is a full table scan.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_patient_CPR'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_patient]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_patient_CPR]
        ON [arc].[ifdb_patient] ([CPR]);
END
GO


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
