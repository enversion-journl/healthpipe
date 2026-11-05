-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_sgh  (1:1 from ILS dbo.sgh)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_sgh'
)
BEGIN
    CREATE TABLE [arc].[ils_sgh] (
        -- Source columns (1:1 from dbo.sgh)
        [prrowid]      varchar(36)    NOT NULL,
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
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_sgh_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_sgh_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_sgh] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_sgh_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_sgh]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_sgh_dw_timestamp_update]
        ON [arc].[ils_sgh] ([dw_timestamp_update] DESC);
END
GO
