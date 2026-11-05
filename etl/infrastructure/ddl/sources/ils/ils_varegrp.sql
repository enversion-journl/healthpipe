-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_varegrp  (1:1 from ILS dbo.varegrp)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_varegrp'
)
BEGIN
    CREATE TABLE [arc].[ils_varegrp] (
        -- Source columns (1:1 from dbo.varegrp)
        [prrowid]      varchar(36)    NOT NULL,
        [varegrp]      varchar(8)     NULL,
        [navn]         varchar(80)    NULL,
        [ktogrp]       varchar(20)    NULL,
        [sw_synk]      bit            NULL,
        [aktiv]        bit            NULL,
        [sysklk]       int            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_varegrp_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_varegrp_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_varegrp] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_varegrp_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_varegrp]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_varegrp_dw_timestamp_update]
        ON [arc].[ils_varegrp] ([dw_timestamp_update] DESC);
END
GO
