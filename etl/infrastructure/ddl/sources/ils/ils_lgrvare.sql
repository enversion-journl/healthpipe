-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.lgrvare_ils  (1:1 from ILS dbo.lgrvare) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lgrvare'
)
BEGIN
    CREATE TABLE [arc].[ils_lgrvare] (
        -- Source columns (1:1 from dbo.lgrvare)
        [prrowid]              varchar(36)    NOT NULL,
        [sgh]                  varchar(8)     NULL,
        [lgr]                  varchar(8)     NULL,
        [vnr]                  varchar(24)    NULL,
        [lnr]                  varchar(20)    NULL,
        [lvnr]                 varchar(40)    NULL,
        [lvnr2]                varchar(40)    NULL,
        [lvnrstrip]            varchar(40)    NULL,
        [branche]              varchar(10)    NULL,
        [deflgrlok]            varchar(16)    NULL,
        [lagersted]            varchar(20)    NULL,
        [enhedtxt]             varchar(40)    NULL,
        [lagerenh]             varchar(10)    NULL,
        [varekode]             varchar(4)     NULL,
        [ktogrp]               varchar(20)    NULL,
        [ktoart]               varchar(20)    NULL,
        [budgart]              varchar(12)    NULL,
        [prisformel]           varchar(4)     NULL,
        [genpkt]               decimal(18,3)  NULL,
        [behmax]               decimal(18,3)  NULL,
        [behmin]               decimal(18,3)  NULL,
        [udlmax]               decimal(18,3)  NULL,
        [udlmin]               decimal(18,3)  NULL,
        [udlmult]              int            NULL,
        [ordmin]               decimal(18,3)  NULL,
        [ordvejl]              decimal(18,3)  NULL,
        [ordopt]               decimal(18,3)  NULL,
        [prisgnsl]             decimal(19,4)  NULL,
        [prisgnsx]             decimal(19,4)  NULL,
        [prisgros]             decimal(19,4)  NULL,
        [prisindk]             decimal(19,4)  NULL,
        [prislist]             decimal(19,4)  NULL,
        [prissalg]             decimal(19,4)  NULL,
        [prisstd]              decimal(19,4)  NULL,
        [prisvejl]             decimal(19,4)  NULL,
        [pakstr]               int            NULL,
        [antdeci]              int            NULL,
        [holddage]             int            NULL,
        [frekvens]             int            NULL,
        [opmetode]             varchar(8)     NULL,
        [optemp]               int            NULL,
        [sidstdto]             datetime       NULL,
        [sidstant]             decimal(18,3)  NULL,
        [gnsmdforb]            decimal(17,2)  NULL,
        [ansvar]               varchar(20)    NULL,
        [sw_flergangs_vare]    bit            NULL,
        [sw_restord]           bit            NULL,
        [sw_lgrstyr]           bit            NULL,
        [sw_batch]             bit            NULL,
        [sw_udlkontr]          bit            NULL,
        [sw_recept]            bit            NULL,
        [fritxt##1]            varchar(164)   NULL,
        [fritxt##2]            varchar(164)   NULL,
        [aktiv]                bit            NULL,
        [pakint]               int            NULL,
        [enhtxtint]            varchar(40)    NULL,
        [txt]                  varchar(80)    NULL,
        [lnavn]                varchar(80)    NULL,
        [oprdto]               datetime       NULL,
        [oprklk]               int            NULL,
        [oprini]               varchar(8)     NULL,
        [fakturaenh]           varchar(40)    NULL,
        [ordreenh]             varchar(40)    NULL,
        [udlevenh]             varchar(40)    NULL,
        [varemodenh]           varchar(40)    NULL,
        [dec1]                 decimal(18,3)  NULL,
        [dec2]                 decimal(18,3)  NULL,
        [dec3]                 decimal(18,3)  NULL,
        [dec4]                 decimal(18,3)  NULL,
        [bruger]               varchar(6)     NULL,
        [sysdato]              datetime       NULL,
        [seqnr]                int            NULL,
        [txt1]                 varchar(80)    NULL,
        [sw_web]               bit            NULL,
        [pakning1]             varchar(20)    NULL,
        [pakning2]             varchar(20)    NULL,
        [pakning3]             varchar(20)    NULL,
        [pakning4]             varchar(20)    NULL,
        [pvnr]                 varchar(50)    NULL,
        [varetype2]            varchar(12)    NULL,
        [sw_lotnr]             bit            NULL,
        [sw_kons]              bit            NULL,
        [sysklk]               int            NULL,
        [paktyp]               varchar(6)     NULL,
        [aftale]               varchar(24)    NULL,
        [indhojde]             decimal(17,3)  NULL,
        [indbrede]             decimal(17,3)  NULL,
        [indlangde]            decimal(17,3)  NULL,
        [indvagt]              decimal(17,3)  NULL,
        [yderhojde]            decimal(17,3)  NULL,
        [yderbrede]            decimal(17,3)  NULL,
        [yderlangde]           decimal(17,3)  NULL,
        [ydervagt]             decimal(17,3)  NULL,
        [pallehojde]           decimal(17,3)  NULL,
        [pallebrede]           decimal(17,3)  NULL,
        [pallelangde]          decimal(17,3)  NULL,
        [pallevagt]            decimal(17,3)  NULL,
        [deflevdage]           int            NULL,
        [bufferlok]            varchar(16)    NULL,
        [sw_beregngenbest]     bit            NULL,
        [kategori]             varchar(40)    NULL,
        [levvarenavn]          varchar(80)    NULL,
        [Pro2SrcPDB]           varchar(12)    NULL,
        [pro2created]          datetime       NULL,
        [pro2modified]         datetime       NULL,
        [udlob_obs]            int            NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lgrvare_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lgrvare_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_lgrvare] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lgrvare_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lgrvare]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lgrvare_dw_timestamp_update]
        ON [arc].[ils_lgrvare] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lgrvare_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_lgrvare]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lgrvare_sysdato]
        ON [arc].[ils_lgrvare] ([sysdato] DESC);
END
GO
