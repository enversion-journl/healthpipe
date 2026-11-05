-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.laftdstam_ils  (1:1 from ILS dbo.laftdstam) — sysdato delta
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_laftdstam'
)
BEGIN
    CREATE TABLE [arc].[ils_laftdstam] (
        -- Source columns (1:1 from dbo.laftdstam)
        [prrowid]      varchar(36)    NOT NULL,
        [lnr]          varchar(20)    NULL,
        [aftale]       varchar(24)    NULL,
        [vnr]          varchar(24)    NULL,
        [start_]       datetime       NULL,
        [startklk]     int            NULL,
        [slut]         datetime       NULL,
        [slutklk]      int            NULL,
        [levvarenavn]  varchar(80)    NULL,
        [distnr]       varchar(20)    NULL,
        [altlnr]       varchar(20)    NULL,
        [lvnr2]        varchar(40)    NULL,
        [vnavn1]       varchar(80)    NULL,
        [vnavn2]       varchar(80)    NULL,
        [billed]       varchar(MAX)   NULL,
        [billedli]     varchar(MAX)   NULL,
        [lvnr##1]      varchar(126)   NULL,
        [lvnr##2]      varchar(126)   NULL,
        [lvnr##3]      varchar(126)   NULL,
        [lvnr##4]      varchar(126)   NULL,
        [inder]        varchar(40)    NULL,
        [yder]         varchar(40)    NULL,
        [palle]        varchar(40)    NULL,
        [mindste]      varchar(40)    NULL,
        [ordreenh]     varchar(40)    NULL,
        [varemodenh]   varchar(40)    NULL,
        [udlevenh]     varchar(40)    NULL,
        [fakturaenh]   varchar(40)    NULL,
        [steril]       varchar(24)    NULL,
        [intrastat]    varchar(40)    NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [aktiv]        bit            NULL,
        [varestr]      varchar(40)    NULL,
        [unspc]        varchar(16)    NULL,
        [extordreenh]  varchar(40)    NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,
        [pmdmid]       int            NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_laftdstam_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_laftdstam_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_laftdstam] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdstam_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdstam]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdstam_dw_timestamp_update]
        ON [arc].[ils_laftdstam] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_laftdstam_sysdato'
      AND object_id = OBJECT_ID(N'[arc].[ils_laftdstam]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdstam_sysdato]
        ON [arc].[ils_laftdstam] ([sysdato] DESC);
END
GO
