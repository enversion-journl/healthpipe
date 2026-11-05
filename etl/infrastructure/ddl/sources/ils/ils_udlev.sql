-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.udlev_ils  (1:1 from ILS dbo.udlev) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_udlev'
)
BEGIN
    CREATE TABLE [arc].[ils_udlev] (
        -- Source columns (1:1 from dbo.udlev)
        [prrowid]          varchar(36)    NOT NULL,
        [sgh]              varchar(8)     NULL,
        [lgr]              varchar(8)     NULL,
        [sgh_rnr]          varchar(8)     NULL,
        [rnr]              varchar(16)    NULL,
        [vnr]              varchar(24)    NULL,
        [rvis]             int            NULL,
        [udldto]           datetime       NULL,
        [udlklk]           int            NULL,
        [flgsednr]         int            NULL,
        [udlant]           decimal(18,3)  NULL,
        [udlbel]           decimal(19,4)  NULL,
        [indkbel]          decimal(19,4)  NULL,
        [rabatbel]         decimal(19,4)  NULL,
        [brik]             int            NULL,
        [lgrlok]           varchar(16)    NULL,
        [batchnr]          varchar(24)    NULL,
        [udlobdto]         datetime       NULL,
        [lnr]              varchar(20)    NULL,
        [ordre]            int            NULL,
        [ordlinie]         int            NULL,
        [postdto]          datetime       NULL,
        [ompoststat]       varchar(4)     NULL,
        [sw_direkte]       bit            NULL,
        [sw_paksdl]        bit            NULL,
        [sw_flgsdl]        bit            NULL,
        [lagerantal]       decimal(18,3)  NULL,
        [projnr]           varchar(20)    NULL,
        [kto]              varchar(28)    NULL,
        [udlevenh]         varchar(40)    NULL,
        [bruger]           varchar(6)     NULL,
        [seqnr]            int            NULL,
        [sysdato]          datetime       NULL,
        [formaal]          varchar(24)    NULL,
        [sw_fly]           bit            NULL,
        [lotnr]            varchar(24)    NULL,
        [refnr]            int            NULL,
        [reflinie]         varchar(40)    NULL,
        [udtraknr1]        int            NULL,
        [udtraknr2]        int            NULL,
        [udtraknr3]        int            NULL,
        [sw_haster]        bit            NULL,
        [prioritet]        varchar(10)    NULL,
        [tekst1]           varchar(60)    NULL,
        [tekst2]           varchar(60)    NULL,
        [aflevoper]        varchar(20)    NULL,
        [aflevdto]         datetime       NULL,
        [aflevklk]         int            NULL,
        [aflevtxt]         varchar(80)    NULL,
        [aflevstreg]       varchar(40)    NULL,
        [id]               int            NULL,
        [rvisvareid]       int            NULL,
        [plukvareid]       int            NULL,
        [salgsordreint]    int            NULL,
        [salgsordre]       varchar(40)    NULL,
        [salgsfaknr]       varchar(20)    NULL,
        [vognnummer]       varchar(80)    NULL,
        [bem]              varchar(100)   NULL,
        [lvnr##1]          varchar(168)   NULL,
        [lvnr##2]          varchar(168)   NULL,
        [lvnr##3]          varchar(168)   NULL,
        [lvnr##4]          varchar(168)   NULL,
        [lgrbeh_prio]      int            NULL,
        [sw_erstat]        bit            NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,
        [kundeid]          int            NULL,
        [kundesortimentid] int            NULL,
        [aarsag]           varchar(16)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_udlev_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_udlev_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_udlev] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_udlev_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_udlev]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_udlev_dw_timestamp_update]
        ON [arc].[ils_udlev] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_udlev_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_udlev]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_udlev_sysdato]
        ON [arc].[ils_udlev] ([sysdato] DESC);
END
GO
