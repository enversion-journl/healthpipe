-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.pmdm_ils  (1:1 from ILS dbo.pmdm) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_pmdm'
)
BEGIN
    CREATE TABLE [arc].[ils_pmdm] (
        -- Source columns (1:1 from dbo.pmdm)
        [prrowid]              varchar(36)    NOT NULL,
        [vnr]                  varchar(24)    NULL,
        [pmdmid]               int            NULL,
        [aktiv]                bit            NULL,
        [navn]                 varchar(80)    NULL,
        [navn2]                varchar(80)    NULL,
        [navneng]              varchar(80)    NULL,
        [navneng2]             varchar(80)    NULL,
        [varetype]             varchar(12)    NULL,
        [varegrp]              varchar(8)     NULL,
        [vareart]              varchar(12)    NULL,
        [basisenh]             varchar(10)    NULL,
        [varestr]              varchar(40)    NULL,
        [momskd]               varchar(4)     NULL,
        [unspsc]               varchar(16)    NULL,
        [unspscint]            varchar(16)    NULL,
        [weblink]              varchar(MAX)   NULL,
        [billede]              varchar(MAX)   NULL,
        [steril]               varchar(24)    NULL,
        [udbkat]               varchar(40)    NULL,
        [sw_tdoc]              bit            NULL,
        [ejlukkes]             bit            NULL,
        [paktyp]               varchar(6)     NULL,
        [adr_kode]             varchar(12)    NULL,
        [un_nr]                varchar(14)    NULL,
        [sik_fak]              decimal(17,2)  NULL,
        [sw_brikker]           bit            NULL,
        [sw_kons]              bit            NULL,
        [sw_afdlgrstyr]        bit            NULL,
        [miljo1]               varchar(16)    NULL,
        [miljo2]               varchar(16)    NULL,
        [miljo3]               varchar(16)    NULL,
        [miljo4]               varchar(16)    NULL,
        [miljo5]               varchar(16)    NULL,
        [miljo6]               varchar(16)    NULL,
        [miljo7]               varchar(16)    NULL,
        [miljo8]               varchar(16)    NULL,
        [miljo9]               varchar(16)    NULL,
        [miljo10]              varchar(16)    NULL,
        [enheduommindste]      varchar(40)    NULL,
        [enheduominder]        varchar(40)    NULL,
        [enheduomyder]         varchar(40)    NULL,
        [enheduompalle]        varchar(40)    NULL,
        [gtinmindste]          varchar(80)    NULL,
        [gtininder]            varchar(80)    NULL,
        [gtinpalle]            varchar(80)    NULL,
        [hojdemindste]         decimal(17,2)  NULL,
        [hojdeinder]           decimal(17,2)  NULL,
        [hojdeyder]            decimal(17,2)  NULL,
        [hojdepalle]           decimal(17,2)  NULL,
        [langdemindste]        decimal(17,2)  NULL,
        [langdeinder]          decimal(17,2)  NULL,
        [langdeyder]           decimal(17,2)  NULL,
        [langdepalle]          decimal(17,2)  NULL,
        [vagtmindste]          decimal(17,2)  NULL,
        [vagtinder]            decimal(17,2)  NULL,
        [vagtyder]             decimal(17,2)  NULL,
        [vagtpalle]            decimal(17,2)  NULL,
        [lnr]                  varchar(20)    NULL,
        [lvnr]                 varchar(24)    NULL,
        [lvnrnavn]             varchar(80)    NULL,
        [lvnr2]                varchar(40)    NULL,
        [prisgnsl]             decimal(17,2)  NULL,
        [ordreuom]             varchar(40)    NULL,
        [fakturauom]           varchar(40)    NULL,
        [mangdeenhed]          varchar(80)    NULL,
        [mindstekob]           decimal(17,2)  NULL,
        [faktor]               decimal(17,2)  NULL,
        [prodnavn]             varchar(80)    NULL,
        [prodvarenavn]         varchar(80)    NULL,
        [prodvarenummer]       varchar(80)    NULL,
        [leverandorid]         varchar(40)    NULL,
        [farenummer]           int            NULL,
        [datablad]             varchar(80)    NULL,
        [oprindelsesland]      varchar(80)    NULL,
        [flergangsvare]        varchar(40)    NULL,
        [positionsnummer]      varchar(40)    NULL,
        [leverandornavn]       varchar(80)    NULL,
        [startdatotid]         datetime       NULL,
        [slutdatotid]          datetime       NULL,
        [begrundelse]          varchar(80)    NULL,
        [kontaktperson]        varchar(80)    NULL,
        [sidstopdpmdm]         datetime       NULL,
        [sidstopdils]          datetime       NULL,
        [afsendt]              bit            NULL,
        [sysdato]              datetime       NULL,
        [seqnr]                int            NULL,
        [bruger]               varchar(10)    NULL,
        [txt]                  varchar(16)    NULL,
        [txt2]                 varchar(16)    NULL,
        [txt3]                 varchar(16)    NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,
        [pmdmid2]              varchar(MAX)   NULL,
        [gtinyder]             varchar(80)    NULL,
        [breddemindste]        decimal(17,3)  NULL,
        [breddeinder]          decimal(17,3)  NULL,
        [breddeyder]           decimal(17,3)  NULL,
        [breddepalle]          decimal(17,3)  NULL,
        [levnavn]              varchar(80)    NULL,
        [levvirksomhedsnr]     varchar(40)    NULL,
        [sw_indberet]          bit            NULL,
        [konverttilvnr]        varchar(24)    NULL,
        [konvertbruger]        varchar(16)    NULL,
        [indlaest]             bit            NULL,
        [sw_ikkeeu]            bit            NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_pmdm_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_pmdm_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_pmdm] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_pmdm_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_pmdm]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_pmdm_dw_timestamp_update]
        ON [arc].[ils_pmdm] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_pmdm_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_pmdm]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_pmdm_sysdato]
        ON [arc].[ils_pmdm] ([sysdato] DESC);
END
GO
