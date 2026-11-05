-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
