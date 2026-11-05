-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_rekv  (1:1 from ILS dbo.rekv)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_rekv'
)
BEGIN
    CREATE TABLE [arc].[ils_rekv] (
        -- Source columns (1:1 from dbo.rekv)
        [prrowid]              varchar(36)    NOT NULL,
        [sgh]                  varchar(8)     NULL,
        [rnr]                  varchar(16)    NULL,
        [altrnr]               varchar(16)    NULL,
        [navn]                 varchar(80)    NULL,
        [adr]                  varchar(80)    NULL,
        [adr2]                 varchar(80)    NULL,
        [land]                 varchar(6)     NULL,
        [postdi]               varchar(10)    NULL,
        [kontakt]              varchar(80)    NULL,
        [tlf]                  varchar(32)    NULL,
        [tlflok]               varchar(8)     NULL,
        [telefax]              varchar(32)    NULL,
        [e_mail]               varchar(80)    NULL,
        [ktosted]              varchar(34)    NULL,
        [rgrp]                 varchar(4)     NULL,
        [rtyp]                 varchar(4)     NULL,
        [budgsted]             varchar(8)     NULL,
        [levadr]               varchar(6)     NULL,
        [udlnr]                int            NULL,
        [rute]                 varchar(16)    NULL,
        [udldag]               varchar(14)    NULL,
        [udlprikd]             varchar(4)     NULL,
        [bem]                  varchar(80)    NULL,
        [edi_lokation]         varchar(34)    NULL,
        [sw_rvis]              bit            NULL,
        [sw_ktoforsa2]         bit            NULL,
        [sw_lgrstyr]           bit            NULL,
        [fiktiv]               bit            NULL,
        [aktiv]                bit            NULL,
        [primsgh]              varchar(8)     NULL,
        [primlgr]              varchar(8)     NULL,
        [sw_ekstern]           bit            NULL,
        [sw_divonline]         bit            NULL,
        [sw_ekstern_faktura]   bit            NULL,
        [bestiller]            varchar(34)    NULL,
        [dunsnr]               varchar(40)    NULL,
        [godk_dir]             bit            NULL,
        [txt1]                 varchar(80)    NULL,
        [txt2]                 varchar(80)    NULL,
        [txt3]                 varchar(80)    NULL,
        [profil]               varchar(80)    NULL,
        [bruger]               varchar(6)     NULL,
        [seqnr]                int            NULL,
        [sysdato]              datetime       NULL,
        [sw_auto]              bit            NULL,
        [afsnit]               varchar(24)    NULL,
        [placerring]           varchar(60)    NULL,
        [fysplacering]         varchar(40)    NULL,
        [sw_fly]               bit            NULL,
        [sysklk]               int            NULL,
        [aflevstreg]           varchar(40)    NULL,
        [transportor]          varchar(20)    NULL,
        [altlevadr]            varchar(6)     NULL,
        [sw_transportdok]      bit            NULL,
        [servicekodesk]        varchar(4)     NULL,
        [servicekodela]        varchar(4)     NULL,
        [vognpladsid]          int            NULL,
        [ekskl_ordadvis]       bit            NULL,
        [oprbruger]            varchar(6)     NULL,
        [oprdto]               datetime       NULL,
        [sor]                  varchar(40)    NULL,
        [salgskonto]           varchar(20)    NULL,
        [glndepot]             varchar(26)    NULL,
        [glndest]              varchar(26)    NULL,
        [glnrorpost]           varchar(26)    NULL,
        [glnrordepot]          varchar(26)    NULL,
        [ansv_led]             varchar(80)    NULL,
        [ansv_budg]            varchar(80)    NULL,
        [ansv_godk]            varchar(80)    NULL,
        [sw_modt_afd]          bit            NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_rekv_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_rekv_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_rekv] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_rekv_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_rekv]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_rekv_dw_timestamp_update]
        ON [arc].[ils_rekv] ([dw_timestamp_update] DESC);
END
GO
