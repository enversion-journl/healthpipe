-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
