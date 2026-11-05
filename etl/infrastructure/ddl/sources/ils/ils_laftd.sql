-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.laftd_ils  (1:1 from ILS dbo.laftd) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_laftd'
)
BEGIN
    CREATE TABLE [arc].[ils_laftd] (
        -- Source columns (1:1 from dbo.laftd)
        [prrowid]       varchar(36)    NOT NULL,
        [lnr]           varchar(20)    NULL,
        [aftale]        varchar(24)    NULL,
        [vnr]           varchar(24)    NULL,
        [bem]           varchar(80)    NULL,
        [maerke]        varchar(10)    NULL,
        [ordmin]        decimal(18,3)  NULL,
        [start_]        datetime       NULL,
        [slut]          datetime       NULL,
        [pris##1]       decimal(38,4)  NULL,
        [pris##2]       decimal(38,4)  NULL,
        [pris##3]       decimal(38,4)  NULL,
        [pakstr##1]     int            NULL,
        [pakstr##2]     int            NULL,
        [pakstr##3]     int            NULL,
        [lvnr##1]       varchar(126)   NULL,
        [lvnr##2]       varchar(126)   NULL,
        [lvnr##3]       varchar(126)   NULL,
        [levplan]       varchar(4)     NULL,
        [distrvnr##1]   varchar(84)    NULL,
        [distrvnr##2]   varchar(84)    NULL,
        [distrvnr##3]   varchar(84)    NULL,
        [lnavn]         varchar(80)    NULL,
        [valuta]        varchar(8)     NULL,
        [kurs]          decimal(19,4)  NULL,
        [afvigpct]      decimal(17,2)  NULL,
        [regulpct]      decimal(17,2)  NULL,
        [andelpct]      decimal(17,2)  NULL,
        [faelvnr]       varchar(24)    NULL,
        [udbud]         varchar(24)    NULL,
        [sysdato]       datetime       NULL,
        [bruger]        varchar(6)     NULL,
        [seqnr]         int            NULL,
        [afttyp]        int            NULL,
        [sw_alt]        bit            NULL,
        [aktiv]         bit            NULL,
        [sw_sek]        bit            NULL,
        [slutvnr]       datetime       NULL,
        [lvnr2]         varchar(40)    NULL,
        [oprdto]        datetime       NULL,
        [Pro2SrcPDB]    varchar(12)    NULL,
        [pro2created]   datetime       NULL,
        [pro2modified]  datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_laftd_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_laftd_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_laftd] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftd_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftd]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftd_dw_timestamp_update]
        ON [arc].[ils_laftd] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftd_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftd]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftd_sysdato]
        ON [arc].[ils_laftd] ([sysdato] DESC);
END
GO
