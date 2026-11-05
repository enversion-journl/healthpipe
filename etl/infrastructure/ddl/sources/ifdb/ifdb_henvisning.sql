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
-- ifdb_henvisning_historik:    delta via HENVISNING.OPDATERET_TID; UPSERT on
--                              (HENVISNING_ID, ORDNING). Read per batch to pick
--                              the current history row by ORDNING and reopening rules.
-- ifdb_referralinformation:    delta via REFERRAL_DATE; UPSERT on OBJECT_ID,
--                              which is the episode-of-care id the transform
--                              joins on (not a surrogate key).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_henvisning  (1:1 from IFDB.HENVISNING — 53 source columns)
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

        [HENVIST_TIL_YDER_NAVN] NVARCHAR(512) NULL,
        [HENVIST_TIL_YDER_NUMMER] NVARCHAR(512) NULL,
        [HENVIST_TIL_AFDELING_KODE] NVARCHAR(512) NULL,
        [HENVIST_TIL_AFDELING_KODETEKST] NVARCHAR(512) NULL,
        [HENVIST_TIL_MUNI_KODE] NVARCHAR(512) NULL,
        [HENVIST_TIL_MUNI_KODETEKST] NVARCHAR(512) NULL,
        [HENVIST_TIL_MUNI_LOCATION] NVARCHAR(512) NULL,
        [HENVIST_FRA_YDER_NAVN] NVARCHAR(512) NULL,
        [HENVIST_FRA_YDER_NUMMER] NVARCHAR(512) NULL,
        [HENVIST_FRA_AFDELING_KODE] NVARCHAR(512) NULL,
        [HENVIST_FRA_AFDELING_KODETEKST] NVARCHAR(512) NULL,
        [HENVIST_FRA_INSTITUTION_NAVN] NVARCHAR(512) NULL,
        [HENVIST_FRA_INSTITUTION_POSTNR] NVARCHAR(512) NULL,
        [HENVIST_FRA_INSTITUTION_VEJ] NVARCHAR(512) NULL,
        [HENVIST_FRA_INSTITUTION_TLF_NR] NVARCHAR(512) NULL,
        [AFSLUTNINGSAARSAG_KODE] NVARCHAR(512) NULL,
        [AFSLUTNINGSAARSAG_KODETEKST] NVARCHAR(512) NULL,
        [AFSENDER_YDER_NAVN] NVARCHAR(512) NULL,
        [AFSENDER_YDER_NUMMER] NVARCHAR(512) NULL,
        [AFSENDER_AFDELING_KODE] NVARCHAR(512) NULL,
        [AFSENDER_AFDELING_KODETEKST] NVARCHAR(512) NULL,
        [AFSENDER_INSTITUTION_NAVN] NVARCHAR(512) NULL,
        [AFSENDER_INSTITUTION_POSTNR] NVARCHAR(512) NULL,
        [AFSENDER_INSTITUTION_VEJ] NVARCHAR(512) NULL,
        [AFSENDER_INSTITUTION_TLF_NR] NVARCHAR(512) NULL,
        [BEHANDLER_YDER_NAVN] NVARCHAR(512) NULL,
        [BEHANDLER_YDER_NUMMER] NVARCHAR(512) NULL,
        [BEHANDLER_AFDELING_KODE] NVARCHAR(512) NULL,
        [BEHANDLER_AFDELING_KODETEKST] NVARCHAR(512) NULL,
        [SENT_ELECTRONICAL] NVARCHAR(512) NULL,

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
