-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_pas_diagnose  (1:1 from IFDB.PAS_DIAGNOSE — 16 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_pas_diagnose'
)
BEGIN
    CREATE TABLE [arc].[ifdb_pas_diagnose] (
        [OBJEKT_ID]                      NVARCHAR(50)    NOT NULL,
        [MODTIDSPUNKT]                   NVARCHAR(100)   NULL,
        [SLETTET]                        NVARCHAR(22)    NULL,
        [PATIENT_ID]                     NVARCHAR(50)    NULL,
        [DIAGNOSETYPE]                   NVARCHAR(100)   NULL,
        [DIAGNOSE_KODE]                  NVARCHAR(512)   NULL,
        [DIAGNOSE_KODETEKST]             NVARCHAR(512)   NULL,
        [OPDATERET_TID]                  NVARCHAR(100)   NULL,
        [LATER_DISPROVED]                NVARCHAR(22)    NULL,
        [MODIFICERET_PERSID]             NVARCHAR(512)   NULL,
        [MODIFICERET_PERSNAVN]           NVARCHAR(512)   NULL,
        [MODIFICERET_BRUGERROLLE]        NVARCHAR(512)   NULL,
        [MODIFICERET_ADMENHED_KODE]      NVARCHAR(512)   NULL,
        [MODIFICERET_ADMENHED_KODETEKST] NVARCHAR(512)   NULL,
        [MODIFICERET_ADMENHED_ORGKODE]   NVARCHAR(512)   NULL,
        [MODIFICERET_ADMENHED_SORKODE]   NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_pas_diagnose_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_pas_diagnose_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_pas_diagnose_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_pas_diagnose]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_pas_diagnose_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_pas_diagnose] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_pas_diagnose_ifdb_OPDATERET_TID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_pas_diagnose]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_pas_diagnose_ifdb_OPDATERET_TID]
        ON [arc].[ifdb_pas_diagnose] ([OPDATERET_TID] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_pas_diagnose_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_pas_diagnose]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_pas_diagnose_ifdb_PATIENT_ID]
        ON [arc].[ifdb_pas_diagnose] ([PATIENT_ID]);
END
GO
