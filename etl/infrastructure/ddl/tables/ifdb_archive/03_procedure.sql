-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB Procedure (ADMINISTRATIVE_PROCEDURE + RIS_YDELSE)
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
--
-- ifdb_administrative_procedure:  delta via IFDB_MODIFICATION_DATETIME; UPSERT on OBJECT_ID.
-- ifdb_adm_procedure_suppl_codes: full-load; UPSERT on (PARENT_OBJECT_ID, IDX).
-- ifdb_ris_ydelse:                delta via OPDATERET_TID; UPSERT on OBJEKT_ID.
--                                 XML CLOB stored verbatim (NVARCHAR(MAX), never indexed).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_administrative_procedure  (1:1 from IFDB.ADMINISTRATIVE_PROCEDURE — 29 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_administrative_procedure'
)
BEGIN
    CREATE TABLE [arc].[ifdb_administrative_procedure] (
        [OBJECT_ID]                  NVARCHAR(50)    NOT NULL,
        [PATIENT_ID]                 NVARCHAR(50)    NULL,
        [ENCOUNTER_ID]               NVARCHAR(50)    NULL,
        [EPISODE_OF_CARE_ID]         NVARCHAR(50)    NULL,
        [PERIOD_START]               NVARCHAR(100)   NULL,
        [PERIOD_END]                 NVARCHAR(100)   NULL,
        [PROCEDURE_CODE]             NVARCHAR(512)   NULL,
        [PROCEDURE_TEXT]             NVARCHAR(512)   NULL,
        [STATUS_CODE]                NVARCHAR(512)   NULL,
        [STATUS_TEXT]                NVARCHAR(512)   NULL,
        [INDICATION_OF_SIDE_CODE]    NVARCHAR(512)   NULL,
        [INDICATION_OF_SIDE_TEXT]    NVARCHAR(512)   NULL,
        [USED_CONTRAST_CODE]         NVARCHAR(512)   NULL,
        [USED_CONTRAST_TEXT]         NVARCHAR(512)   NULL,
        [STAFF_CATEGORY_CODE]        NVARCHAR(512)   NULL,
        [STAFF_CATEGORY_TEXT]        NVARCHAR(512)   NULL,
        [LOCATION_CODE]              NVARCHAR(512)   NULL,
        [LOCATION_NAME]              NVARCHAR(512)   NULL,
        [LOCATION_ORGCODE]           NVARCHAR(512)   NULL,
        [LOCATION_SORCODE]           NVARCHAR(512)   NULL,
        [EPJI_CREATED_DATETIME]      NVARCHAR(100)   NULL,
        [EPJI_MODIFICATION_DATETIME] NVARCHAR(100)   NULL,
        [IFDB_MODIFICATION_DATETIME] NVARCHAR(100)   NULL,
        [DELETED]                    NVARCHAR(22)    NULL,
        [USER_ID]                    NVARCHAR(512)   NULL,
        [USER_NAME]                  NVARCHAR(512)   NULL,
        [USER_ROLE]                  NVARCHAR(512)   NULL,
        [LPR3_IGNORED]               NVARCHAR(22)    NULL,
        [USER_ROLE_NAME]             NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]        DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]        DATETIME2(0)    NOT NULL,
        [dw_is_deleted]              BIT             NOT NULL CONSTRAINT [DF_ifdb_administrative_procedure_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_administrative_procedure_ifdb]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_administrative_procedure_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_administrative_procedure]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_administrative_procedure_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_administrative_procedure] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_administrative_procedure_ifdb_IFDB_MODIFICATION_DATETIME'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_administrative_procedure]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_administrative_procedure_ifdb_IFDB_MODIFICATION_DATETIME]
        ON [arc].[ifdb_administrative_procedure] ([IFDB_MODIFICATION_DATETIME] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_administrative_procedure_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_administrative_procedure]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_administrative_procedure_ifdb_PATIENT_ID]
        ON [arc].[ifdb_administrative_procedure] ([PATIENT_ID]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_adm_procedure_suppl_codes  (1:1 from IFDB.ADM_PROCEDURE_SUPPL_CODES — 4 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_adm_procedure_suppl_codes'
)
BEGIN
    CREATE TABLE [arc].[ifdb_adm_procedure_suppl_codes] (
        [PARENT_OBJECT_ID]    NVARCHAR(50)    NOT NULL,
        [IDX]                 NVARCHAR(50)    NOT NULL,
        [SUPPL_CODE]          NVARCHAR(512)   NULL,
        [SUPPL_TEXT]          NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert] DATETIME2(0)    NOT NULL,
        [dw_timestamp_update] DATETIME2(0)    NOT NULL,
        [dw_is_deleted]       BIT             NOT NULL CONSTRAINT [DF_ifdb_adm_procedure_suppl_codes_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_adm_procedure_suppl_codes_ifdb]
            PRIMARY KEY CLUSTERED ([PARENT_OBJECT_ID], [IDX])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_adm_procedure_suppl_codes_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_adm_procedure_suppl_codes]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_adm_procedure_suppl_codes_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_adm_procedure_suppl_codes] ([dw_timestamp_update] DESC);
END
GO


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
