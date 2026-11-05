-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_vare  (1:1 from ILS dbo.vare — full schema mirror)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_vare'
)
BEGIN
    CREATE TABLE [arc].[ils_vare] (
        [prrowid]               VARCHAR(36)    NOT NULL,
        [vnr]                   NVARCHAR(24)   NULL,
        [navn]                  NVARCHAR(80)   NULL,
        [navn2]                 NVARCHAR(80)   NULL,
        [varestr]               NVARCHAR(40)   NULL,
        [navn_conc]             NVARCHAR(100)  NULL,
        [basisenh]              NVARCHAR(10)   NULL,
        [faelvnr]               NVARCHAR(24)   NULL,
        [faelnavn]              NVARCHAR(80)   NULL,
        [faelnavn2]             NVARCHAR(80)   NULL,
        [ean_vnr]               NVARCHAR(36)   NULL,
        [vareart]               NVARCHAR(12)   NULL,
        [varegrp]               NVARCHAR(8)    NULL,
        [varetype]              NVARCHAR(12)   NULL,
        [varelis]               NVARCHAR(12)   NULL,
        [momskd]                NVARCHAR(4)    NULL,
        [afgiftkd]              NVARCHAR(4)    NULL,
        [miljo]                 NVARCHAR(16)   NULL,
        [ansvar]                NVARCHAR(20)   NULL,
        [adr_kode]              NVARCHAR(12)   NULL,
        [un_nr]                 NVARCHAR(12)   NULL,
        [sik_fak]               DECIMAL(17,2)  NULL,
        [holddage]              INT            NULL,
        [billed]                NVARCHAR(104)  NULL,
        [sw_edivare]            BIT            NULL,
        [sw_udlkontr]           BIT            NULL,
        [sw_batch]              BIT            NULL,
        [sw_ovf_gs]             BIT            NULL,
        [sw_opret_gs]           BIT            NULL,
        [sw_ovf_dk200]          BIT            NULL,
        [sw_opret_dk2000]       BIT            NULL,
        [sw_synk]               BIT            NULL,
        [sw_usyn_gs]            BIT            NULL,
        [sw_gs]                 BIT            NULL,
        [aktiv]                 BIT            NULL,
        [oprdto]                DATETIME2      NULL,
        [oprtid]                INT            NULL,
        [oprbruger]             NVARCHAR(6)    NULL,
        [billedli]              NVARCHAR(140)  NULL,
        [sw_web]                BIT            NULL,
        [unspc]                 NVARCHAR(16)   NULL,
        [sampak]                BIT            NULL,
        [txt1]                  NVARCHAR(80)   NULL,
        [txt2]                  NVARCHAR(80)   NULL,
        [bruger]                NVARCHAR(6)    NULL,
        [sysdato]               DATETIME2      NULL,
        [seqnr]                 INT            NULL,
        [varetype2]             NVARCHAR(12)   NULL,
        [skat]                  NVARCHAR(10)   NULL,
        [synonym_]              NVARCHAR(120)  NULL,
        [kategori]              NVARCHAR(40)   NULL,
        [sysklk]                INT            NULL,
        [varecpv]               NVARCHAR(20)   NULL,
        [pak1]                  NVARCHAR(40)   NULL,
        [pak2]                  NVARCHAR(40)   NULL,
        [pak3]                  NVARCHAR(40)   NULL,
        [markering]             NVARCHAR(80)   NULL,
        [sw_steril]             BIT            NULL,
        [navneng]               NVARCHAR(80)   NULL,
        [navneng2]              NVARCHAR(80)   NULL,
        [ejlukkes]              BIT            NULL,
        [sw_farlig]             BIT            NULL,
        [farligfarve]           NVARCHAR(10)   NULL,
        [pvnr]                  NVARCHAR(60)   NULL,
        [pvarenavn]             NVARCHAR(160)  NULL,
        [pvarestr]              NVARCHAR(140)  NULL,
        [pnavn]                 NVARCHAR(120)  NULL,
        [implantatside]         NVARCHAR(2)    NULL,
        [extordreenh]           NVARCHAR(40)   NULL,
        [unspsc2]               NVARCHAR(16)   NULL,
        [paktyp]                NVARCHAR(6)    NULL,
        [intrastat]             NVARCHAR(40)   NULL,
        [unspscint]             NVARCHAR(16)   NULL,
        [udbkat]                NVARCHAR(40)   NULL,
        [udbegnet]              NVARCHAR(6)    NULL,
        [appafh]                BIT            NULL,
        [steril]                NVARCHAR(24)   NULL,
        [fraafttype]            NVARCHAR(40)   NULL,
        [sw_indberet]           BIT            NULL,
        [Pro2SrcPDB]            NVARCHAR(12)   NULL,
        [pro2created]           DATETIME2      NULL,
        [pro2modified]          DATETIME2      NULL,
        [pmdmid]                INT            NULL,
        [sw_ikkeeu]             BIT            NULL,
        [sw_sygehusapotek]      BIT            NULL,
        [sw_kritiskvareliste]   BIT            NULL,
        [sw_positivliste]       BIT            NULL,
        [sw_hjemmepatient]      BIT            NULL,
        [sw_obstekst]           BIT            NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_ils_vare] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_vare_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_vare]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_vare_sysdato]
        ON [arc].[ils_vare] ([sysdato] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_vare_vnr'
      AND object_id = OBJECT_ID(N'[arc].[ils_vare]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_vare_vnr]
        ON [arc].[ils_vare] ([vnr])
        WHERE [vnr] IS NOT NULL;
END
GO
