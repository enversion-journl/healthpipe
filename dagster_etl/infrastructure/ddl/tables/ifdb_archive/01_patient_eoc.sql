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
-- patient_ifdb:          delta-loaded via MODTIDSPUNKT; UPSERT on PATIENT_ID.
-- episode_of_care_ifdb:  delta-loaded via IFDB_MODIFICATION_DATETIME; UPSERT on OBJECT_ID.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.patient_ifdb  (1:1 from IFDB.PATIENT_OPLYSNINGER_COLUMNA — 25 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'patient_ifdb'
)
BEGIN
    CREATE TABLE [arc].[patient_ifdb] (
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
        [DW_Timestamp_Insert]            DATETIME2(0)    NOT NULL,
        [DW_Timestamp_Update]            DATETIME2(0)    NOT NULL,
        [DW_IsDeleted]                   BIT             NOT NULL DEFAULT (0),

        CONSTRAINT [PK_arc_patient_ifdb]
            PRIMARY KEY CLUSTERED ([PATIENT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_patient_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[patient_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_patient_ifdb_DW_Timestamp_Update]
        ON [arc].[patient_ifdb] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_patient_ifdb_MODTIDSPUNKT'
      AND object_id = OBJECT_ID(N'[arc].[patient_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_patient_ifdb_MODTIDSPUNKT]
        ON [arc].[patient_ifdb] ([MODTIDSPUNKT] DESC);
END
GO

-- Supports the CPR-dedup query in Patient_IFDB_dfm._build_patient_query, which
-- joins windowed CPRs back to the whole archive (so an active patient is never
-- clobbered by a touched-but-deleted duplicate). Also speeds the CPR fallback
-- in prepare_resources_for_upsert. Without it the join is a full table scan.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_patient_ifdb_CPR'
      AND object_id = OBJECT_ID(N'[arc].[patient_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_patient_ifdb_CPR]
        ON [arc].[patient_ifdb] ([CPR]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.episode_of_care_ifdb  (1:1 from IFDB.EPISODE_OF_CARE — 17 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'episode_of_care_ifdb'
)
BEGIN
    CREATE TABLE [arc].[episode_of_care_ifdb] (
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
        [DW_Timestamp_Insert]            DATETIME2(0)    NOT NULL,
        [DW_Timestamp_Update]            DATETIME2(0)    NOT NULL,
        [DW_IsDeleted]                   BIT             NOT NULL DEFAULT (0),

        CONSTRAINT [PK_arc_episode_of_care_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_episode_of_care_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[episode_of_care_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_episode_of_care_ifdb_DW_Timestamp_Update]
        ON [arc].[episode_of_care_ifdb] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_episode_of_care_ifdb_IFDB_MODIFICATION_DATETIME'
      AND object_id = OBJECT_ID(N'[arc].[episode_of_care_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_episode_of_care_ifdb_IFDB_MODIFICATION_DATETIME]
        ON [arc].[episode_of_care_ifdb] ([IFDB_MODIFICATION_DATETIME] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_episode_of_care_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[episode_of_care_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_episode_of_care_ifdb_PATIENT_ID]
        ON [arc].[episode_of_care_ifdb] ([PATIENT_ID]);
END
GO
