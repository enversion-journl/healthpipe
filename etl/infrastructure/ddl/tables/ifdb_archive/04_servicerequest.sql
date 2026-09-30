-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB ServiceRequest (HENVISNING + enrichment tables)
--
-- 1:1 reflection of IFDB Oracle source tables plus DWH audit columns.
--
-- RIS_YDELSE — the other ServiceRequest source — lives in 03_procedure.sql
-- (shared with the Procedure pipeline).
--
-- ifdb_henvisning:             delta via OPDATERET_TID; UPSERT on OBJEKT_ID;
--                              soft-delete driven by SLETTET.
-- ifdb_henvisning_historik:    delta via BESLUT_TIDSPUNKT; UPSERT on
--                              (HENVISNING_ID, ORDNING). Read per batch to pick
--                              the latest status-history row per henvisning.
-- ifdb_referralinformation:    delta via REFERRAL_DATE; UPSERT on OBJECT_ID,
--                              which is the episode-of-care id the transform
--                              joins on (not a surrogate key).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_henvisning  (1:1 from IFDB.HENVISNING — 23 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_henvisning'
)
BEGIN
    CREATE TABLE [arc].[ifdb_henvisning] (
        [OBJEKT_ID]                     NVARCHAR(50)    NOT NULL,
        [MODTIDSPUNKT]                  NVARCHAR(100)   NULL,
        [SLETTET]                       NVARCHAR(22)    NULL,
        [PATIENT_ID]                    NVARCHAR(50)    NULL,
        [EPISODE_OF_CARE_ID]            NVARCHAR(50)    NULL,
        [HENVISNINGSDATO]               NVARCHAR(100)   NULL,
        [AFSENDELSESDATO]               NVARCHAR(100)   NULL,
        [SLUTDATO]                      NVARCHAR(100)   NULL,
        [MODTAGET]                      NVARCHAR(100)   NULL,
        [STATUS_KODE]                   NVARCHAR(512)   NULL,
        [STATUS_KODETEKST]              NVARCHAR(512)   NULL,
        [TYPE_KODE]                     NVARCHAR(512)   NULL,
        [TYPE_KODETEKST]                NVARCHAR(512)   NULL,
        [INDKALDELSESPRIORIT_KODE]      NVARCHAR(512)   NULL,
        [INDKALDELSESPRIORIT_KODETEKST] NVARCHAR(512)   NULL,
        [HENVISNINGSMAADE_KODE]         NVARCHAR(512)   NULL,
        [HENVISNINGSMAADE_KODETEKST]    NVARCHAR(512)   NULL,
        [REFERRAL_PURPOSE]              NVARCHAR(1750)  NULL,
        [HENVIST_TIL_AFDELING_SORKODE]  NVARCHAR(512)   NULL,
        [HENVIST_FRA_AFDELING_SORKODE]  NVARCHAR(512)   NULL,
        [AFSENDER_AFDELING_SORKODE]     NVARCHAR(512)   NULL,
        [BEHANDLER_AFDELING_SORKODE]    NVARCHAR(512)   NULL,
        [OPDATERET_TID]                 NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]           DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]           DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                 BIT             NOT NULL CONSTRAINT [DF_ifdb_henvisning_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_henvisning_ifdb] PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_henvisning_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_henvisning]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_henvisning_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_henvisning] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_henvisning_ifdb_OPDATERET_TID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_henvisning]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_henvisning_ifdb_OPDATERET_TID]
        ON [arc].[ifdb_henvisning] ([OPDATERET_TID]);
END
GO

-- EPISODE_OF_CARE_ID drives the referral-information lookup per batch.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_henvisning_ifdb_EPISODE_OF_CARE_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_henvisning]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_henvisning_ifdb_EPISODE_OF_CARE_ID]
        ON [arc].[ifdb_henvisning] ([EPISODE_OF_CARE_ID]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_henvisning_historik  (1:1 from IFDB.HENVISNING_HISTORIK — 16 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_henvisning_historik'
)
BEGIN
    CREATE TABLE [arc].[ifdb_henvisning_historik] (
        [HENVISNING_ID]         NVARCHAR(50)    NOT NULL,
        [ORDNING]               NVARCHAR(50)    NOT NULL,
        [HANDLING]              NVARCHAR(512)   NULL,
        [BESLUT_TIDSPUNKT]      NVARCHAR(100)   NULL,
        [FRA_KODE]              NVARCHAR(512)   NULL,
        [FRA_KODETEKST]         NVARCHAR(512)   NULL,
        [STATUS_KODE]           NVARCHAR(512)   NULL,
        [STATUS_KODETEKST]      NVARCHAR(512)   NULL,
        [TIL_AFDELING_SORKODE]  NVARCHAR(512)   NULL,
        [MODTAGETDATO]          NVARCHAR(100)   NULL,
        [VISITATIONSDATO]       NVARCHAR(100)   NULL,
        [VENTEGRUPPE]           NVARCHAR(512)   NULL,
        [EKSPERTOMRAADE]        NVARCHAR(512)   NULL,
        [TREATMENTRIGHT]        NVARCHAR(512)   NULL,
        [FAELLESVISITATION]     NVARCHAR(512)   NULL,
        [DIAGNOSE_ID]           NVARCHAR(50)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)    NOT NULL,
        [dw_is_deleted]         BIT             NOT NULL CONSTRAINT [DF_ifdb_henvisning_historik_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_henvisning_historik_ifdb]
            PRIMARY KEY CLUSTERED ([HENVISNING_ID], [ORDNING])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_henvisning_historik_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_henvisning_historik]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_henvisning_historik_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_henvisning_historik] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_henvisning_historik_ifdb_BESLUT_TIDSPUNKT'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_henvisning_historik]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_henvisning_historik_ifdb_BESLUT_TIDSPUNKT]
        ON [arc].[ifdb_henvisning_historik] ([BESLUT_TIDSPUNKT]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_referralinformation  (1:1 from IFDB.REFERRALINFORMATION — 14 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_referralinformation'
)
BEGIN
    CREATE TABLE [arc].[ifdb_referralinformation] (
        [OBJECT_ID]             NVARCHAR(50)    NOT NULL,
        [REFERRAL_ORG_CODE]     NVARCHAR(512)   NULL,
        [REFERRAL_ORG_NAME]     NVARCHAR(512)   NULL,
        [REFERRAL_ORG_ORGCODE]  NVARCHAR(512)   NULL,
        [REFERRAL_ORG_SORCODE]  NVARCHAR(512)   NULL,
        [PRACTITIONER_NUMBER]   NVARCHAR(100)   NULL,
        [PRACTITIONER_NAME]     NVARCHAR(512)   NULL,
        [REFERRAL_TYPE_CODE]    NVARCHAR(512)   NULL,
        [REFERRAL_TYPE_TEXT]    NVARCHAR(512)   NULL,
        [REFERRAL_REASON_CODE]  NVARCHAR(512)   NULL,
        [REFERRAL_REASON_TEXT]  NVARCHAR(1750)  NULL,
        [FREECHOICE_CODE]       NVARCHAR(512)   NULL,
        [FREECHOICE_TEXT]       NVARCHAR(512)   NULL,
        [REFERRAL_DATE]         NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)    NOT NULL,
        [dw_is_deleted]         BIT             NOT NULL CONSTRAINT [DF_ifdb_referralinformation_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_referralinformation_ifdb] PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_referralinformation_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_referralinformation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_referralinformation_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_referralinformation] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_referralinformation_ifdb_REFERRAL_DATE'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_referralinformation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_referralinformation_ifdb_REFERRAL_DATE]
        ON [arc].[ifdb_referralinformation] ([REFERRAL_DATE]);
END
GO
