-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_lafth  (1:1 from ILS dbo.lafth)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lafth'
)
BEGIN
    CREATE TABLE [arc].[ils_lafth] (
        -- Source columns (1:1 from dbo.lafth)
        [prrowid]          varchar(36)    NOT NULL,
        [lnr]              varchar(20)    NULL,
        [aftale]           varchar(24)    NULL,
        [aftdto]           datetime       NULL,
        [txt]              varchar(80)    NULL,
        [start_]           datetime       NULL,
        [slut]             datetime       NULL,
        [belfra##1]        decimal(38,2)  NULL,
        [belfra##2]        decimal(38,2)  NULL,
        [belfra##3]        decimal(38,2)  NULL,
        [beltil##1]        decimal(38,2)  NULL,
        [beltil##2]        decimal(38,2)  NULL,
        [beltil##3]        decimal(38,2)  NULL,
        [rabkr##1]         decimal(38,2)  NULL,
        [rabkr##2]         decimal(38,2)  NULL,
        [rabkr##3]         decimal(38,2)  NULL,
        [rabatpct##1]      decimal(38,2)  NULL,
        [rabatpct##2]      decimal(38,2)  NULL,
        [rabatpct##3]      decimal(38,2)  NULL,
        [bem]              varchar(80)    NULL,
        [bonusgr##1]       decimal(38,2)  NULL,
        [bonusgr##2]       decimal(38,2)  NULL,
        [bonusgr##3]       decimal(38,2)  NULL,
        [bonusbel##1]      decimal(38,2)  NULL,
        [bonusbel##2]      decimal(38,2)  NULL,
        [bonusbel##3]      decimal(38,2)  NULL,
        [bonuspct##1]      decimal(38,2)  NULL,
        [bonuspct##2]      decimal(38,2)  NULL,
        [bonuspct##3]      decimal(38,2)  NULL,
        [bonusbem]         varchar(80)    NULL,
        [altlnr]           varchar(20)    NULL,
        [aktiv]            bit            NULL,
        [udbud]            varchar(24)    NULL,
        [kontakt]          varchar(80)    NULL,
        [ansvar]           varchar(20)    NULL,
        [eksansvar]        varchar(80)    NULL,
        [sysdato]          datetime       NULL,
        [bruger]           varchar(6)     NULL,
        [seqnr]            int            NULL,
        [afttype]          varchar(20)    NULL,
        [fragtbel]         decimal(17,2)  NULL,
        [sw_fragt]         bit            NULL,
        [gebyrbel]         decimal(17,2)  NULL,
        [sw_gebyr]         bit            NULL,
        [graensebel]       decimal(17,2)  NULL,
        [fragtgraensebel]  decimal(17,2)  NULL,
        [deflevdage]       int            NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lafth_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lafth_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_lafth] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lafth_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lafth]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lafth_dw_timestamp_update]
        ON [arc].[ils_lafth] ([dw_timestamp_update] DESC);
END
GO
