-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB Condition (DIAGNOSE + PAS_DIAGNOSE incl. ref/child tables)
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
--
-- ifdb_diagnose:                   delta-loaded via MODTIDSPUNKT; UPSERT on OBJEKT_ID.
-- ifdb_diagnose_tillaegskode:      full-load; UPSERT on (DIAGNOSE_ID, TILLAEGSKODE_KODE).
-- ifdb_pas_diagnose:               delta-loaded via OPDATERET_TID; UPSERT on OBJEKT_ID.
-- ifdb_pas_diagnose_encounter_ref: full-load link table; UPSERT on (DIAGNOSE_ID, ENCOUNTER_ID).
-- ifdb_pas_diagnose_tillaegskode:  full-load; UPSERT on (PAS_DIAGNOSE_ID, TILLAEGSKODE_KODE).
-- ifdb_pas_diagnose_henvisning_ref: full-load link table; UPSERT on (DIAGNOSE_ID, HENVISNING_ID).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_diagnose  (1:1 from IFDB.DIAGNOSE — 27 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_diagnose'
)
BEGIN
    CREATE TABLE [arc].[ifdb_diagnose] (
        [OBJEKT_ID]               NVARCHAR(50)    NOT NULL,
        [MODTIDSPUNKT]            NVARCHAR(100)   NULL,
        [SLETTET]                 NVARCHAR(22)    NULL,
        [PATIENT_ID]              NVARCHAR(50)    NULL,
        [BESLUT_PERSID]           NVARCHAR(512)   NULL,
        [BESLUT_PERSNAVN]         NVARCHAR(512)   NULL,
        [BESLUT_BRUGERROLLE]      NVARCHAR(512)   NULL,
        [BESLUT_ADMENHED_KODE]    NVARCHAR(512)   NULL,
        [BESLUT_ADMENHED_NAVN]    NVARCHAR(512)   NULL,
        [BESLUT_ADMENHED_ORGKODE] NVARCHAR(512)   NULL,
        [BESLUT_TIDSPUNKT]        NVARCHAR(100)   NULL,
        [AFSLUT_PERSID]           NVARCHAR(512)   NULL,
        [AFSLUT_PERSNAVN]         NVARCHAR(512)   NULL,
        [AFSLUT_BRUGERROLLE]      NVARCHAR(512)   NULL,
        [AFSLUT_ADMENHED_KODE]    NVARCHAR(512)   NULL,
        [AFSLUT_ADMENHED_NAVN]    NVARCHAR(512)   NULL,
        [AFSLUT_ADMENHED_ORGKODE] NVARCHAR(512)   NULL,
        [AFSLUT_TIDSPUNKT]        NVARCHAR(100)   NULL,
        [OPRET_PERSID]            NVARCHAR(512)   NULL,
        [OPRET_PERSNAVN]          NVARCHAR(512)   NULL,
        [OPRET_BRUGERROLLE]       NVARCHAR(512)   NULL,
        [OPRET_ADMENHED_KODE]     NVARCHAR(512)   NULL,
        [OPRET_ADMENHED_NAVN]     NVARCHAR(512)   NULL,
        [OPRET_ADMENHED_ORGKODE]  NVARCHAR(512)   NULL,
        [OPRET_TIDSPUNKT]         NVARCHAR(100)   NULL,
        [DIAGNOSE_KODE]           NVARCHAR(512)   NULL,
        [DIAGNOSE_KODETEKST]      NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]     DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]     DATETIME2(0)    NOT NULL,
        [dw_is_deleted]           BIT             NOT NULL CONSTRAINT [DF_ifdb_diagnose_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_diagnose_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_diagnose_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_diagnose]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_diagnose_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_diagnose] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_diagnose_ifdb_MODTIDSPUNKT'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_diagnose]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_diagnose_ifdb_MODTIDSPUNKT]
        ON [arc].[ifdb_diagnose] ([MODTIDSPUNKT] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_diagnose_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_diagnose]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_diagnose_ifdb_PATIENT_ID]
        ON [arc].[ifdb_diagnose] ([PATIENT_ID]);
END
GO
