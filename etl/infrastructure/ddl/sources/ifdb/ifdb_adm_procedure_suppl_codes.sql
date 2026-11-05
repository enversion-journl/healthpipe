-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
