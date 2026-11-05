-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.indkfak_ils  (1:1 from ILS dbo.indkfak) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_indkfak'
)
BEGIN
    CREATE TABLE [arc].[ils_indkfak] (
        -- Source columns (1:1 from dbo.indkfak)
        [prrowid]          varchar(36)    NOT NULL,
        [sgh]              varchar(8)     NULL,
        [lgr]              varchar(8)     NULL,
        [vnr]              varchar(24)    NULL,
        [sgh_rnr]          varchar(8)     NULL,
        [rnr]              varchar(16)    NULL,
        [fakant]           decimal(18,3)  NULL,
        [fakbel]           decimal(17,2)  NULL,
        [prismod]          decimal(17,2)  NULL,
        [varerabbel]       decimal(17,2)  NULL,
        [varerabpct]       decimal(17,2)  NULL,
        [ordre]            int            NULL,
        [faknr]            varchar(30)    NULL,
        [batchnr]          varchar(24)    NULL,
        [lnr]              varchar(20)    NULL,
        [aftale]           varchar(12)    NULL,
        [levdto]           datetime       NULL,
        [fakdto]           datetime       NULL,
        [bgfdto]           datetime       NULL,
        [klkslet]          int            NULL,
        [lagerantal]       decimal(18,3)  NULL,
        [bilag]            int            NULL,
        [edi_status]       varchar(4)     NULL,
        [linie]            int            NULL,
        [mednr]            varchar(30)    NULL,
        [levvarenr]        varchar(40)    NULL,
        [levvarenavn]      varchar(80)    NULL,
        [enhedsstorrelse]  varchar(30)    NULL,
        [kontantrabatdato] datetime       NULL,
        [levtype]          varchar(16)    NULL,
        [levid]            varchar(16)    NULL,
        [slutlev]          bit            NULL,
        [slutfakt]         bit            NULL,
        [projnr]           varchar(20)    NULL,
        [faklinienr]       int            NULL,
        [fakant_org]       decimal(18,3)  NULL,
        [fakbel_org]       decimal(17,2)  NULL,
        [match1]           varchar(60)    NULL,
        [match2]           varchar(60)    NULL,
        [fakturaenh]       varchar(40)    NULL,
        [sysdato]          datetime       NULL,
        [seqnr]            int            NULL,
        [bruger]           varchar(6)     NULL,
        [formaal]          varchar(24)    NULL,
        [xmlantal]         decimal(17,2)  NULL,
        [xmlenhed]         varchar(40)    NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_indkfak_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_indkfak_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_indkfak] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_indkfak_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_indkfak]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_indkfak_dw_timestamp_update]
        ON [arc].[ils_indkfak] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_indkfak_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_indkfak]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_indkfak_sysdato]
        ON [arc].[ils_indkfak] ([sysdato] DESC);
END
GO
