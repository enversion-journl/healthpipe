-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.bom_ifdb_aftaleressourcedeltager  (inner-join-delta child — 6 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'bom_ifdb_aftaleressourcedeltager'
)
BEGIN
    CREATE TABLE [arc].[bom_ifdb_aftaleressourcedeltager] (
        [OID]                            NVARCHAR(50)    NOT NULL,
        [AFTALEOID]                      NVARCHAR(50)    NULL,
        [KRAV_PERSONALE_ROLLEKODE]       NVARCHAR(512)   NULL,
        [TRAEK_RESSOURCEOID]             NVARCHAR(50)    NULL,
        [TRAEK_START_DATOTID]            NVARCHAR(100)   NULL,
        [TRAEK_SLUT_DATOTID]             NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_bom_ifdb_aftaleressourcedeltager_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_bom_ifdb_aftaleressourcedeltager]
            PRIMARY KEY CLUSTERED ([OID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_aftaleressourcedeltager_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_aftaleressourcedeltager]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_aftaleressourcedeltager_DW_Timestamp_Update]
        ON [arc].[bom_ifdb_aftaleressourcedeltager] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_aftaleressourcedeltager_AFTALEOID'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_aftaleressourcedeltager]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_aftaleressourcedeltager_AFTALEOID]
        ON [arc].[bom_ifdb_aftaleressourcedeltager] ([AFTALEOID]);
END
GO
