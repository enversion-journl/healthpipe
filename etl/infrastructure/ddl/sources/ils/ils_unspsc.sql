-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_unspsc  (1:1 from ILS dbo.unspsc)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_unspsc'
)
BEGIN
    CREATE TABLE [arc].[ils_unspsc] (
        -- Source columns (1:1 from dbo.unspsc)
        [prrowid]               VARCHAR(36)    NOT NULL,
        [unspsc]                NVARCHAR(16)   NULL,
        [betegn]                NVARCHAR(255)  NULL,
        [un_name]               NVARCHAR(255)  NULL,
        [sysdato]               DATETIME2      NULL,
        [bruger]                NVARCHAR(6)    NULL,
        [seqnr]                 INT            NULL,
        [aktiv]                 BIT            NULL,
        [version]               NVARCHAR(16)   NULL,
        [Pro2SrcPDB]            NVARCHAR(12)   NULL,
        [pro2created]           DATETIME2      NULL,
        [pro2modified]          DATETIME2      NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_ils_unspsc] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_unspsc_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_unspsc]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_unspsc_sysdato]
        ON [arc].[ils_unspsc] ([sysdato] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_unspsc_unspsc'
      AND object_id = OBJECT_ID(N'[arc].[ils_unspsc]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_unspsc_unspsc]
        ON [arc].[ils_unspsc] ([unspsc])
        WHERE [unspsc] IS NOT NULL;
END
GO
