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
