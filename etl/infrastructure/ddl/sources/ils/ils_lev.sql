-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_lev  (1:1 from ILS dbo.lev)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lev'
)
BEGIN
    CREATE TABLE [arc].[ils_lev] (
        -- Source columns (1:1 from dbo.lev)
        [prrowid]              varchar(36)    NOT NULL,
        [lnr]                  varchar(20)    NULL,
        [navn]                 varchar(80)    NULL,
        [adr]                  varchar(80)    NULL,
        [adr2]                 varchar(80)    NULL,
        [land]                 varchar(6)     NULL,
        [postdi]               varchar(10)    NULL,
        [kontakt]              varchar(80)    NULL,
        [salger]               varchar(80)    NULL,
        [tlf]                  varchar(32)    NULL,
        [mobil]                varchar(32)    NULL,
        [email]                varchar(80)    NULL,
        [wwwside]              varchar(80)    NULL,
        [fax]                  varchar(32)    NULL,
        [giro]                 varchar(16)    NULL,
        [betform]              varchar(4)     NULL,
        [bet]                  varchar(6)     NULL,
        [bet2]                 varchar(6)     NULL,
        [kasbet]               varchar(6)     NULL,
        [kasrabpct]            decimal(17,2)  NULL,
        [levbet]               varchar(4)     NULL,
        [gebyrbel]             decimal(17,2)  NULL,
        [graensebel]           decimal(17,2)  NULL,
        [kundenr]              varchar(32)    NULL,
        [lkto]                 varchar(28)    NULL,
        [afstyp]               varchar(2)     NULL,
        [deflevdage]           int            NULL,
        [defgikfik]            varchar(66)    NULL,
        [nrkd]                 int            NULL,
        [bem]                  varchar(80)    NULL,
        [se_nr]                varchar(28)    NULL,
        [ydmodt]               varchar(20)    NULL,
        [edi_lokation]         varchar(34)    NULL,
        [edi_postks]           varchar(34)    NULL,
        [edi_pw]               varchar(28)    NULL,
        [sw_ean]               bit            NULL,
        [sw_edi_ord_fak_1_1]   bit            NULL,
        [sw_oekopost]          bit            NULL,
        [sw_synk]              bit            NULL,
        [ansvar]               varchar(20)    NULL,
        [aktiv]                bit            NULL,
        [sysdato]              datetime       NULL,
        [bruger]               varchar(6)     NULL,
        [seqnr]                int            NULL,
        [fastdag]              varchar(14)    NULL,
        [regnum]               varchar(8)     NULL,
        [bankkto]              varchar(20)    NULL,
        [sw_fik]               bit            NULL,
        [billedbi]             varchar(100)   NULL,
        [levdag]               varchar(14)    NULL,
        [doip]                 varchar(6)     NULL,
        [dunsnr]               varchar(40)    NULL,
        [e_vnr]                int            NULL,
        [levklk]               int            NULL,
        [txt1]                 varchar(80)    NULL,
        [txt2]                 varchar(80)    NULL,
        [txt3]                 varchar(80)    NULL,
        [txt4]                 varchar(20)    NULL,
        [cvr_nr]               varchar(28)    NULL,
        [type]                 varchar(10)    NULL,
        [ordredag]             varchar(14)    NULL,
        [brugerid]             varchar(20)    NULL,
        [pw]                   varchar(40)    NULL,
        [ordweb]               bit            NULL,
        [oprdto]               datetime       NULL,
        [oprbruger]            varchar(8)     NULL,
        [oprtid]               int            NULL,
        [sysklk]               int            NULL,
        [sw_andenlev]          bit            NULL,
        [ordremail]            varchar(120)   NULL,
        [ordresprog]           varchar(6)     NULL,
        [oioprofil]            varchar(24)    NULL,
        [sw_oioord_txt]        bit            NULL,
        [sw_oio_basisenh]      bit            NULL,
        [faktyratype]          varchar(20)    NULL,
        [sw_ws_lvnr]           bit            NULL,
        [ws_divvnr]            varchar(24)    NULL,
        [laasenhed]            bit            NULL,
        [intrastat]            varchar(20)    NULL,
        [sw_omvmoms]           bit            NULL,
        [osafd]                varchar(20)    NULL,
        [typenr]               varchar(20)    NULL,
        [nr]                   varchar(40)    NULL,
        [sw_oioord_txt_lin]    bit            NULL,
        [orddeadlineklk]       int            NULL,
        [fragtbel]             decimal(17,2)  NULL,
        [fragtgraensebel]      decimal(17,2)  NULL,
        [fakturamail]          varchar(120)   NULL,
        [synonym_]             varchar(120)   NULL,
        [rykkermail]           varchar(120)   NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lev_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lev_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_lev] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lev_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lev]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lev_dw_timestamp_update]
        ON [arc].[ils_lev] ([dw_timestamp_update] DESC);
END
GO
