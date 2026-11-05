-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_pas_diagnose_henvisning_ref  (1:1 from IFDB.PAS_DIAGNOSE_HENVISNING_REF — 2 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_pas_diagnose_henvisning_ref'
)
BEGIN
    CREATE TABLE [arc].[ifdb_pas_diagnose_henvisning_ref] (
        [DIAGNOSE_ID]         NVARCHAR(50)    NOT NULL,
        [HENVISNING_ID]       NVARCHAR(50)    NOT NULL,

        -- DWH audit columns
        [dw_timestamp_insert] DATETIME2(0)    NOT NULL,
        [dw_timestamp_update] DATETIME2(0)    NOT NULL,
        [dw_is_deleted]       BIT             NOT NULL CONSTRAINT [DF_ifdb_pas_diagnose_henvisning_ref_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_pas_diagnose_henvisning_ref_ifdb]
            PRIMARY KEY CLUSTERED ([DIAGNOSE_ID], [HENVISNING_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_pas_diagnose_henvisning_ref_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_pas_diagnose_henvisning_ref]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_pas_diagnose_henvisning_ref_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_pas_diagnose_henvisning_ref] ([dw_timestamp_update] DESC);
END
GO
