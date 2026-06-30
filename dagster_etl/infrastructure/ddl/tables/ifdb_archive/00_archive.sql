-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB (Encounter + Location)
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
--
-- encounter_ifdb: delta-loaded via IFDB_MODIFICATION_DATETIME; UPSERT on OBJECT_ID.
-- location_ifdb:  full-load; UPSERT on (OBJECT_ID, PERIOD_START) with soft-delete.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.encounter_ifdb  (1:1 from IFDB.ENCOUNTER — 34 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'encounter_ifdb'
)
BEGIN
    CREATE TABLE [arc].[encounter_ifdb] (
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
        [DW_Timestamp_Insert]            DATETIME2(0)    NOT NULL,
        [DW_Timestamp_Update]            DATETIME2(0)    NOT NULL,
        [DW_IsDeleted]                   BIT             NOT NULL DEFAULT (0),

        CONSTRAINT [PK_arc_encounter_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_encounter_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[encounter_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_encounter_ifdb_DW_Timestamp_Update]
        ON [arc].[encounter_ifdb] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_encounter_ifdb_IFDB_MODIFICATION_DATETIME'
      AND object_id = OBJECT_ID(N'[arc].[encounter_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_encounter_ifdb_IFDB_MODIFICATION_DATETIME]
        ON [arc].[encounter_ifdb] ([IFDB_MODIFICATION_DATETIME] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_encounter_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[encounter_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_encounter_ifdb_PATIENT_ID]
        ON [arc].[encounter_ifdb] ([PATIENT_ID]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.location_ifdb  (1:1 from IFDB.LOCATION — 11 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'location_ifdb'
)
BEGIN
    CREATE TABLE [arc].[location_ifdb] (
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
        [DW_Timestamp_Insert]            DATETIME2(0)    NOT NULL,
        [DW_Timestamp_Update]            DATETIME2(0)    NOT NULL,
        [DW_IsDeleted]                   BIT             NOT NULL DEFAULT (0),

        CONSTRAINT [PK_arc_location_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [PERIOD_START])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_location_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[location_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_location_ifdb_DW_Timestamp_Update]
        ON [arc].[location_ifdb] ([DW_Timestamp_Update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_location_ifdb_LOCATION_CODE'
      AND object_id = OBJECT_ID(N'[arc].[location_ifdb]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_location_ifdb_LOCATION_CODE]
        ON [arc].[location_ifdb] ([LOCATION_CODE]);
END
GO
