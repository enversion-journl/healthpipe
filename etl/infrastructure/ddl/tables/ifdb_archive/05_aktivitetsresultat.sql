-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB Aktivitetsresultat / SFI
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
--
-- ifdb_aktivitetsresultat:            delta via OPDATERET_TID; UPSERT on OBJEKT_ID.
--                                     XML CLOB stored verbatim (NVARCHAR(MAX), never indexed).
-- ifdb_aktivitetsudfoerelse:          delta via OPDATERET_TID; UPSERT on OBJEKT_ID.
-- ifdb_aktivitetsudfoerelse_speciale: full-load child; UPSERT on
--                                     (AKTIVITETSUDFOERELSE_ID, SPECIALE_KODE).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_aktivitetsresultat  (1:1 from IFDB.AKTIVITETSRESULTAT — 34 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_aktivitetsresultat'
)
BEGIN
    CREATE TABLE [arc].[ifdb_aktivitetsresultat] (
        [OBJEKT_ID]                NVARCHAR(50)    NOT NULL,
        [MODTIDSPUNKT]             NVARCHAR(100)   NULL,
        [SLETTET]                  NVARCHAR(22)    NULL,
        [PATIENT_ID]               NVARCHAR(50)    NULL,
        [UDFOERELSE_ID]            NVARCHAR(50)    NULL,
        [AKTIVITETTYPE_KODE]       NVARCHAR(512)   NULL,
        [AKTIVITETTYPE_KODETEKST]  NVARCHAR(512)   NULL,
        [RESULTATTYPE_KODE]        NVARCHAR(512)   NULL,
        [RESULTATTYPE_KODETEKST]   NVARCHAR(512)   NULL,
        [OVERSKRIFT]               NVARCHAR(MAX)   NULL,
        [HIERARKISK_PLACERING]     NVARCHAR(512)   NULL,
        [DOMAENENAVN]              NVARCHAR(512)   NULL,
        [MODELLERINGSNAVN]         NVARCHAR(512)   NULL,
        [MODELLERINGSVERSION]      NVARCHAR(100)   NULL,
        [XML]                      NVARCHAR(MAX)   NULL,
        [TILSTEDEVAERELSE_ID]      NVARCHAR(50)    NULL,
        [UDFOERT_PERSID]           NVARCHAR(512)   NULL,
        [UDFOERT_PERSNAVN]         NVARCHAR(512)   NULL,
        [UDFOERT_BRUGERROLLE]      NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_KODE]    NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_NAVN]    NVARCHAR(512)   NULL,
        [UDFOERT_ADMENHED_ORGKODE] NVARCHAR(512)   NULL,
        [UDFOERT_TIDSPUNKT]        NVARCHAR(100)   NULL,
        [KLADDE]                   NVARCHAR(22)    NULL,
        [OPDATERET_TID]            NVARCHAR(100)   NULL,
        [ENCOUNTER_ID]             NVARCHAR(50)    NULL,
        [EPISODE_OF_CARE_ID]       NVARCHAR(50)    NULL,
        [PERIOD_START]             NVARCHAR(100)   NULL,
        [PERIOD_END]               NVARCHAR(100)   NULL,
        [UDFOERT_ADMENHED_SORCODE] NVARCHAR(512)   NULL,
        [LPR3_IGNORED]             NVARCHAR(22)    NULL,
        [UDFOERT_BRUGERROLLE_NAME] NVARCHAR(512)   NULL,
        [RESULT_TYPE]              NVARCHAR(512)   NULL,
        [RESULT_MOID]              NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)    NOT NULL,
        [dw_is_deleted]            BIT             NOT NULL CONSTRAINT [DF_ifdb_aktivitetsresultat_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_aktivitetsresultat_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_aktivitetsresultat_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_aktivitetsresultat]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_aktivitetsresultat_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_aktivitetsresultat] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_aktivitetsresultat_ifdb_OPDATERET_TID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_aktivitetsresultat]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_aktivitetsresultat_ifdb_OPDATERET_TID]
        ON [arc].[ifdb_aktivitetsresultat] ([OPDATERET_TID] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_aktivitetsresultat_ifdb_PATIENT_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_aktivitetsresultat]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_aktivitetsresultat_ifdb_PATIENT_ID]
        ON [arc].[ifdb_aktivitetsresultat] ([PATIENT_ID]);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_aktivitetsresultat_ifdb_UDFOERELSE_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_aktivitetsresultat]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_aktivitetsresultat_ifdb_UDFOERELSE_ID]
        ON [arc].[ifdb_aktivitetsresultat] ([UDFOERELSE_ID]);
END
GO


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


-- ----------------------------------------------------------------------------
-- arc.ifdb_aktivitetsudfoerelse_speciale
-- (1:1 from IFDB.AKTIVITETSUDFOERELSE_SPECIALE — 6 source columns; no Oracle PK)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_aktivitetsudfoerelse_speciale'
)
BEGIN
    CREATE TABLE [arc].[ifdb_aktivitetsudfoerelse_speciale] (
        [AKTIVITETSUDFOERELSE_ID]  NVARCHAR(50)    NOT NULL,
        [SPECIALE_HOSPITAL_KODE]   NVARCHAR(512)   NULL,
        [SPECIALE_HOSPITAL_NAVN]   NVARCHAR(512)   NULL,
        [SPECIALE_HOSPITAL_ORGKODE] NVARCHAR(512)  NULL,
        [SPECIALE_KODE]            NVARCHAR(512)   NOT NULL,
        [SPECIALE_KODETEKST]       NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)    NOT NULL,
        [dw_is_deleted]            BIT             NOT NULL CONSTRAINT [DF_ifdb_aktivitetsudfoerelse_speciale_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_aktivitetsudfoerelse_speciale_ifdb]
            PRIMARY KEY CLUSTERED ([AKTIVITETSUDFOERELSE_ID], [SPECIALE_KODE])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_aktivitetsudfoerelse_speciale_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_aktivitetsudfoerelse_speciale]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_aktivitetsudfoerelse_speciale_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_aktivitetsudfoerelse_speciale] ([dw_timestamp_update] DESC);
END
GO
