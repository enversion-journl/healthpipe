-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_projekt  (1:1 from ILS dbo.projekt)
-- Lookup for projektnavn on dim.Vareudlevering (join on projnr only, SAS parity).
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_projekt'
)
BEGIN
    CREATE TABLE [arc].[ils_projekt] (
        [prrowid]      varchar(36)    NOT NULL,
        [sgh]          varchar(8)     NULL,
        [projnr]       varchar(20)    NULL,
        [navn]         varchar(80)    NULL,
        [ktosted]      varchar(20)    NULL,
        [projgr]       varchar(16)    NULL,
        [sw_niveau]    bit            NULL,
        [startdato]    datetime       NULL,
        [slutdato]     datetime       NULL,
        [rekvirent]    varchar(200)   NULL,
        [aktiv]        bit            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_projekt_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_projekt_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_projekt] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_projekt_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_projekt]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_projekt_dw_timestamp_update]
        ON [arc].[ils_projekt] ([dw_timestamp_update] DESC);
END
GO
