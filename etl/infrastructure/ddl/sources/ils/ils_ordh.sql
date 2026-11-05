-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_ordh  (1:1 from ILS dbo.ordh) — sysdato delta
-- Order headers. adr1-adr5 (alternative delivery address) can hold a home
-- patient's name and address; they are archived 1:1 but not carried further.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_ordh'
)
BEGIN
    CREATE TABLE [arc].[ils_ordh] (
        -- Source columns (1:1 from dbo.ordh)
        [prrowid]         varchar(36)    NOT NULL,
        [sgh]             varchar(8)     NULL,
        [lgr]             varchar(8)     NULL,
        [ordre]           int            NULL,
        [lnr]             varchar(20)    NULL,
        [orddto]          datetime       NULL,
        [ordstat]         varchar(4)     NULL,
        [afstyp]          varchar(2)     NULL,
        [edi_status]      varchar(4)     NULL,
        [edi_ref_ordre]   int            NULL,
        [ordtxt]          varchar(80)    NULL,
        [levadr]          varchar(6)     NULL,
        [bet]             varchar(6)     NULL,
        [sw_org_mgl]      bit            NULL,
        [sw_kopi_mgl]     bit            NULL,
        [bd_oprid]        varchar(20)    NULL,
        [sysdato]         datetime       NULL,
        [bruger]          varchar(6)     NULL,
        [seqnr]           int            NULL,
        [ean_lokation]    varchar(34)    NULL,
        [refnr]           int            NULL,
        [fakadr]          varchar(40)    NULL,
        [txt1]            varchar(80)    NULL,
        [txt2]            varchar(80)    NULL,
        [sw_tilbageholdt] bit            NULL,
        [sw_stoppet]      bit            NULL,
        [bemstoppet]      varchar(80)    NULL,
        [tilbdato]        datetime       NULL,
        [tilbbruger]      varchar(10)    NULL,
        [levref]          varchar(200)   NULL,
        [indkober]        varchar(200)   NULL,
        [dokkode]         varchar(16)    NULL,
        [txt3]            varchar(60)    NULL,
        [txt4]            varchar(60)    NULL,
        [accept]          bit            NULL,
        [ac_bruger]       varchar(6)     NULL,
        [ac_dato]         datetime       NULL,
        [ac_kl]           int            NULL,
        [ac_txt]          varchar(60)    NULL,
        [adr1]            varchar(80)    NULL,
        [adr2]            varchar(80)    NULL,
        [adr3]            varchar(80)    NULL,
        [adr4]            varchar(80)    NULL,
        [adr5]            varchar(80)    NULL,
        [konsig]          varchar(8)     NULL,
        [xml_filnavn]     varchar(100)   NULL,
        [fragtbel]        decimal(17,2)  NULL,
        [gebyrbel]        decimal(17,2)  NULL,
        [sw_stopryk]      bit            NULL,
        [ordretype]       varchar(6)     NULL,
        [Pro2SrcPDB]      varchar(12)    NULL,
        [pro2created]     datetime       NULL,
        [pro2modified]    datetime       NULL,
        [sendt_wms]       bit            NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_ordh_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_ordh_dw_timestamp_update] DEFAULT SYSDATETIME(),

        CONSTRAINT [PK_arc_ils_ordh] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

-- Serves the pre_stg delta read (dw_timestamp_update > checkpoint).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_ordh_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_ordh]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_ordh_dw_timestamp_update]
        ON [arc].[ils_ordh] ([dw_timestamp_update] DESC);
END
GO

-- Serves checkpoint_query MAX([sysdato]).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_ordh_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_ordh]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_ordh_sysdato]
        ON [arc].[ils_ordh] ([sysdato] DESC);
END
GO
