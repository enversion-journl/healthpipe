-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: BOM_IFDB (ORGENHED + SORENHED → Location_SOR)
--
-- Flattened ORGENHED⋈SORENHED join plus DWH audit columns.
-- Full-load with detect_deletes (soft-delete via NOT MATCHED BY SOURCE).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.bom_ifdb_orgenhed_sor  (ORGENHED⋈SORENHED — 12 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'bom_ifdb_orgenhed_sor'
)
BEGIN
    CREATE TABLE [arc].[bom_ifdb_orgenhed_sor] (
        [OID]                            NVARCHAR(50)    NOT NULL,
        [GUID]                           NVARCHAR(100)   NULL,
        [ORG_TYPE]                       NVARCHAR(512)   NULL,
        [AKTIV]                          NVARCHAR(100)   NULL,
        [GYLDIG_FRA_DATOTID]             NVARCHAR(100)   NULL,
        [GYLDIG_TIL_DATOTID]             NVARCHAR(100)   NULL,
        [SORENHEDOID]                    NVARCHAR(50)    NULL,
        [PARENT_ORGENHEDOID]             NVARCHAR(50)    NULL,
        [LASTMODIFIED]                   NVARCHAR(100)   NULL,
        [SORKODE]                        NVARCHAR(512)   NULL,
        [SHAKKODE]                       NVARCHAR(512)   NULL,
        [SORENHED_OID]                   NVARCHAR(50)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_bom_ifdb_orgenhed_sor_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_bom_ifdb_orgenhed_sor]
            PRIMARY KEY CLUSTERED ([OID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_orgenhed_sor_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_orgenhed_sor]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_orgenhed_sor_DW_Timestamp_Update]
        ON [arc].[bom_ifdb_orgenhed_sor] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_orgenhed_sor_SORKODE'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_orgenhed_sor]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_orgenhed_sor_SORKODE]
        ON [arc].[bom_ifdb_orgenhed_sor] ([SORKODE]);
END
GO
