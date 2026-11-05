-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.laftdpris_ils  (1:1 from ILS dbo.laftdpris) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_laftdpris'
)
BEGIN
    CREATE TABLE [arc].[ils_laftdpris] (
        -- Source columns (1:1 from dbo.laftdpris)
        [prrowid]      varchar(36)    NOT NULL,
        [lnr]          varchar(20)    NULL,
        [aftale]       varchar(24)    NULL,
        [vnr]          varchar(24)    NULL,
        [start_]       datetime       NULL,
        [startklk]     int            NULL,
        [slut]         datetime       NULL,
        [slutklk]      int            NULL,
        [pris##1]      decimal(38,4)  NULL,
        [pris##2]      decimal(38,4)  NULL,
        [pris##3]      decimal(38,4)  NULL,
        [pris##4]      decimal(38,4)  NULL,
        [pakstr##1]    decimal(38,3)  NULL,
        [pakstr##2]    decimal(38,3)  NULL,
        [pakstr##3]    decimal(38,3)  NULL,
        [pakstr##4]    decimal(38,3)  NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [aktiv]        bit            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_laftdpris_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_laftdpris_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_laftdpris] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdpris_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdpris]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdpris_dw_timestamp_update]
        ON [arc].[ils_laftdpris] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdpris_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdpris]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdpris_sysdato]
        ON [arc].[ils_laftdpris] ([sysdato] DESC);
END
GO
