-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_epikrise_tekst  (1:1 from IFDB.EPIKRISE_TEKST — 4 source columns;
--                           archive only, TEKST is CLOB → NVARCHAR(MAX))
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_epikrise_tekst'
)
BEGIN
    CREATE TABLE [arc].[ifdb_epikrise_tekst] (
        [OBJEKT_ID]                       NVARCHAR(50)    NOT NULL,
        [EPIKRISE_ID]                     NVARCHAR(50)    NULL,
        [OVERSKRIFT]                      NVARCHAR(512)   NULL,
        [TEKST]                           NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]             DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_ifdb_epikrise_tekst_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_epikrise_tekst_ifdb]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_tekst_ifdb_EPIKRISE_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise_tekst]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_tekst_ifdb_EPIKRISE_ID]
        ON [arc].[ifdb_epikrise_tekst] ([EPIKRISE_ID]);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epikrise_tekst_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_epikrise_tekst]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epikrise_tekst_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_epikrise_tekst] ([dw_timestamp_update] DESC);
END
GO
