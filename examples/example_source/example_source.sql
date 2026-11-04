-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Creates the example_source database with made-up data in the shape of the ILS
-- user tables dbo.zz_oper and dbo.sgh. All names, codes and IDs are fake.
-- The script drops and creates the two tables, so you can run it again.

IF DB_ID(N'example_source') IS NULL
    CREATE DATABASE [example_source];
GO

USE [example_source];
GO

DROP TABLE IF EXISTS [dbo].[sgh];
DROP TABLE IF EXISTS [dbo].[zz_oper];
GO

CREATE TABLE [dbo].[sgh] (
    [prrowid]      varchar(36)    NOT NULL PRIMARY KEY,
    [sgh]          varchar(8)     NULL,
    [navn]         varchar(80)    NULL,
    [adr]          varchar(80)    NULL,
    [land]         varchar(6)     NULL,
    [postdi]       varchar(10)    NULL,
    [giro]         varchar(16)    NULL,
    [tlf]          varchar(32)    NULL,
    [fax]          varchar(32)    NULL,
    [afvigpct]     decimal(17,2)  NULL,
    [afvigpct2]    decimal(17,2)  NULL,
    [flerlagr]     bit            NULL,
    [antgnkob]     int            NULL,
    [aktiv]        bit            NULL,
    [fastomkbel]   decimal(17,2)  NULL,
    [personpris]   decimal(19,4)  NULL,
    [sterillgr]    varchar(8)     NULL,
    [usterillgr]   varchar(8)     NULL,
    [lnr]          varchar(20)    NULL,
    [levoekolog]   int            NULL,
    [edi_lokation] varchar(34)    NULL,
    [bruger]       varchar(6)     NULL,
    [seqnr]        int            NULL,
    [sysdato]      datetime       NULL,
    [dunsnr]       varchar(40)    NULL,
    [vat_eu]       varchar(20)    NULL,
    [vat]          varchar(20)    NULL,
    [Pro2SrcPDB]   varchar(12)    NULL,
    [pro2created]  datetime       NULL,
    [pro2modified] datetime       NULL
);

CREATE TABLE [dbo].[zz_oper] (
    [prrowid]      varchar(36)    NOT NULL PRIMARY KEY,
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
    [pro2modified] datetime       NULL
);
GO

INSERT INTO [dbo].[sgh] ([prrowid], [sgh], [navn], [aktiv], [bruger], [seqnr], [sysdato], [pro2created], [pro2modified])
VALUES
    ('EX-SGH-0001', 'EX01', 'Example Hospital North', 1, 'EXADM', 1, '2026-01-05', '2026-01-05', '2026-01-05'),
    ('EX-SGH-0002', 'EX02', 'Example Hospital South', 1, 'EXADM', 2, '2026-01-05', '2026-01-05', '2026-01-05');

INSERT INTO [dbo].[zz_oper] ([prrowid], [bd_oprid], [bd_init], [bd_navn], [bd_titel], [bd_primafd], [bd_aktiv], [bd_usrtyp], [bruger], [seqnr], [sysdato], [pro2created], [pro2modified])
VALUES
    ('EX-OPER-0001', 'EXUSR01', 'EXA', 'Example User One', 'Example buyer', 'EX01', 1, 'EXAMPLE', 'EXADM', 1, '2026-01-05', '2026-01-05', '2026-01-05'),
    ('EX-OPER-0002', 'EXUSR02', 'EXB', 'Example User Two', 'Example buyer', 'EX02', 1, 'EXAMPLE', 'EXADM', 2, '2026-01-06', '2026-01-06', '2026-01-06'),
    ('EX-OPER-0003', 'EXUSR03', 'EXC', 'Example User Three', 'Example store keeper', 'EX01', 0, 'EXAMPLE', 'EXADM', 3, '2026-01-07', '2026-01-07', '2026-01-07'),
    ('EX-OPER-0004', 'XX-EXU04A', 'EXD', 'Example User Four', 'Example store keeper', 'EX02', 1, 'EXAMPLE', 'EXADM', 4, '2026-01-08', '2026-01-08', '2026-01-08'),
    ('EX-OPER-0005', 'EXUSR05', 'EXE', 'Example User Five', 'Example planner', 'EX99', 1, 'EXAMPLE', 'EXADM', 5, '2026-01-09', '2026-01-09', '2026-01-09'),
    ('EX-OPER-0006', 'EXUSR06', NULL, 'Example User Six', 'Example planner', 'EX01', 1, 'EXAMPLE', 'EXADM', 6, '2026-01-10', '2026-01-10', '2026-01-10');
GO
