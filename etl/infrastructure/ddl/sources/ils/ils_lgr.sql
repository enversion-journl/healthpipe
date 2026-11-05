-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_lgr  (1:1 from ILS dbo.lgr)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lgr'
)
BEGIN
    CREATE TABLE [arc].[ils_lgr] (
        -- Source columns (1:1 from dbo.lgr)
        [prrowid]          varchar(36)    NOT NULL,
        [sgh]              varchar(8)     NULL,
        [lgr]              varchar(8)     NULL,
        [altlgr]           varchar(16)    NULL,
        [navn]             varchar(80)    NULL,
        [adr]              varchar(80)    NULL,
        [land]             varchar(6)     NULL,
        [postdi]           varchar(10)    NULL,
        [kontakt]          varchar(80)    NULL,
        [tlf]              varchar(32)    NULL,
        [fax]              varchar(32)    NULL,
        [giro]             varchar(16)    NULL,
        [deflevadr]        varchar(6)     NULL,
        [deflevdage]       int            NULL,
        [udlprikd]         varchar(4)     NULL,
        [opgprikd]         varchar(4)     NULL,
        [lgrejer]          varchar(16)    NULL,
        [rekvsvind]        varchar(16)    NULL,
        [rekvindk]         varchar(16)    NULL,
        [rekvsalg]         varchar(16)    NULL,
        [rekvavan]         varchar(16)    NULL,
        [rvis]             int            NULL,
        [flgsednr]         int            NULL,
        [batchnr]          varchar(24)    NULL,
        [bhtbibnavn]       varchar(16)    NULL,
        [edi_filsti]       varchar(104)   NULL,
        [ordhtxt##1]       varchar(186)   NULL,
        [ordhtxt##2]       varchar(186)   NULL,
        [ordhtxt##3]       varchar(186)   NULL,
        [flgstxt##1]       varchar(186)   NULL,
        [flgstxt##2]       varchar(186)   NULL,
        [flgstxt##3]       varchar(186)   NULL,
        [ordhatt]          varchar(60)    NULL,
        [sw_udlev]         bit            NULL,
        [sw_restord]       bit            NULL,
        [sw_genbemax]      bit            NULL,
        [sw_bht_udlev]     bit            NULL,
        [flerlok]          bit            NULL,
        [batchfm]          varchar(4)     NULL,
        [fiktiv]           bit            NULL,
        [aktiv]            bit            NULL,
        [ktolvgld]         varchar(20)    NULL,
        [ktovkob]          varchar(20)    NULL,
        [ktoporto]         varchar(20)    NULL,
        [ktofragt]         varchar(20)    NULL,
        [ktokasrb]         varchar(20)    NULL,
        [ktogebyr]         varchar(20)    NULL,
        [ktoforko]         varchar(200)   NULL,
        [ktoforsa]         varchar(200)   NULL,
        [ktoforal]         varchar(200)   NULL,
        [ktoformo]         varchar(200)   NULL,
        [ktoforsa2]        varchar(200)   NULL,
        [ktoforomp]        varchar(200)   NULL,
        [ktoresta]         varchar(20)    NULL,
        [ktoforsk]         varchar(20)    NULL,
        [ktodiff]          varchar(20)    NULL,
        [ekspnr]           int            NULL,
        [ekspnrgl]         int            NULL,
        [ekspndto]         datetime       NULL,
        [filsti]           varchar(104)   NULL,
        [filkvit]          varchar(16)    NULL,
        [filovf]           varchar(16)    NULL,
        [filudt]           varchar(16)    NULL,
        [fillaas]          varchar(16)    NULL,
        [oekosys]          varchar(6)     NULL,
        [oekoid1]          varchar(20)    NULL,
        [oekoid2]          varchar(20)    NULL,
        [oekoid3]          varchar(20)    NULL,
        [ompstdtokd]       varchar(6)     NULL,
        [sw_skabo]         bit            NULL,
        [wilsonomk##1]     decimal(38,2)  NULL,
        [wilsonomk##2]     decimal(38,2)  NULL,
        [wilsonomk##3]     decimal(38,2)  NULL,
        [wilsonpct##1]     decimal(38,2)  NULL,
        [wilsonpct##2]     decimal(38,2)  NULL,
        [wilsonpct##3]     decimal(38,2)  NULL,
        [indsenr]          varchar(20)    NULL,
        [indtlf]           varchar(32)    NULL,
        [indfax]           varchar(32)    NULL,
        [dunsnr]           varchar(40)    NULL,
        [ean_lokation]     varchar(34)    NULL,
        [bruger]           varchar(6)     NULL,
        [seqnr]            int            NULL,
        [sysdato]          datetime       NULL,
        [cvr_nr]           varchar(28)    NULL,
        [e_mail]           varchar(80)    NULL,
        [cvr_udland]       varchar(28)    NULL,
        [ktomoms]          varchar(20)    NULL,
        [fragtvnr]         varchar(24)    NULL,
        [gebyrvnr]         varchar(24)    NULL,
        [portovnr]         varchar(24)    NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,
        [ktovejafg]        varchar(20)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lgr_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lgr_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_lgr] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lgr_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lgr]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lgr_dw_timestamp_update]
        ON [arc].[ils_lgr] ([dw_timestamp_update] DESC);
END
GO
