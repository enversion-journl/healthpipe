-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_zz_oper  (1:1 from ILS dbo.zz_oper)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_zz_oper'
)
BEGIN
    CREATE TABLE [arc].[ils_zz_oper] (
        -- Source columns (1:1 from dbo.zz_oper)
        [prrowid]      varchar(36)    NOT NULL,
        [bd_oprid]     varchar(20)    NULL,
        [bd_init]      varchar(6)     NULL,
        [bd_navn]      varchar(60)    NULL,
        [bd_titel]     varchar(100)   NULL,
        [bd_image]     varchar(16)    NULL,
        [bd_primafd]   varchar(16)    NULL,
        [bd_aktiv]     bit            NULL,
        [bd_pwdupd]    datetime       NULL,
        [bd_pwdlim]    datetime       NULL,
        [bd_pwdchgrqd] bit            NULL,
        [bd_sesant]    int            NULL,
        [bd_usrtyp]    varchar(16)    NULL,
        [bd_signdate]  datetime       NULL,
        [bd_signtime]  varchar(10)    NULL,
        [bd_signid##1] varchar(54)    NULL,
        [bd_signid##2] varchar(54)    NULL,
        [bd_signid##3] varchar(54)    NULL,
        [bd_fradat]    datetime       NULL,
        [bd_tildat]    datetime       NULL,
        [bd_kopiaf]    varchar(20)    NULL,
        [bd_dmngrp]    varchar(32)    NULL,
        [bd_supermail] varchar(16)    NULL,
        [bd_mail]      varchar(16)    NULL,
        [bd_tlf]       varchar(16)    NULL,
        [bd_adr]       varchar(48)    NULL,
        [bd_div]       varchar(16)    NULL,
        [bd_sysdiv]    varchar(16)    NULL,
        [win_fgd]      int            NULL,
        [win_bgd]      int            NULL,
        [edi_fgd]      int            NULL,
        [edi_bgd]      int            NULL,
        [eob_fgd]      int            NULL,
        [eob_bgd]      int            NULL,
        [eob_led]      int            NULL,
        [epr_fgd]      int            NULL,
        [epr_bgd]      int            NULL,
        [inf_fgd]      int            NULL,
        [inf_bgd]      int            NULL,
        [vlg_fgd]      int            NULL,
        [vlg_bgd]      int            NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_zz_oper_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_ils_zz_oper] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_zz_oper_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_zz_oper]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_zz_oper_dw_timestamp_update]
        ON [arc].[ils_zz_oper] ([dw_timestamp_update] DESC);
END
GO
