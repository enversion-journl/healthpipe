-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.bom_ifdb_patientinfo  (delta on LASTMODIFIED — 7 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'bom_ifdb_patientinfo'
)
BEGIN
    CREATE TABLE [arc].[bom_ifdb_patientinfo] (
        [PATIENTINFOOID]                 NVARCHAR(50)    NOT NULL,
        [CPRNR]                          NVARCHAR(20)    NULL,
        [FORNAVN]                        NVARCHAR(512)   NULL,
        [EFTERNAVN]                      NVARCHAR(512)   NULL,
        [FOEDSELSDATO]                   NVARCHAR(100)   NULL,
        [KOEN]                           NVARCHAR(100)   NULL,
        [LASTMODIFIED]                   NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                   BIT             NOT NULL CONSTRAINT [DF_bom_ifdb_patientinfo_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_bom_ifdb_patientinfo]
            PRIMARY KEY CLUSTERED ([PATIENTINFOOID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_patientinfo_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_patientinfo]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_patientinfo_DW_Timestamp_Update]
        ON [arc].[bom_ifdb_patientinfo] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_patientinfo_CPRNR'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_patientinfo]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_patientinfo_CPRNR]
        ON [arc].[bom_ifdb_patientinfo] ([CPRNR]);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_bom_ifdb_patientinfo_LASTMODIFIED'
      AND object_id = OBJECT_ID(N'[arc].[bom_ifdb_patientinfo]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_bom_ifdb_patientinfo_LASTMODIFIED]
        ON [arc].[bom_ifdb_patientinfo] ([LASTMODIFIED]);
END
GO
