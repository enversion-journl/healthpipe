-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================
-- ILS archive tables (17 new tables)
-- 1:1 mirror of ILS dbo.* source tables
-- Populated by DFM-managed archive merge assets
-- ============================================================


-- ----------------------------------------------------------------------------
-- arc.ils_vareart  (1:1 from ILS dbo.vareart)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_vareart'
)
BEGIN
    CREATE TABLE [arc].[ils_vareart] (
        -- Source columns (1:1 from dbo.vareart)
        [prrowid]      varchar(36)    NOT NULL,
        [vareart]      varchar(12)    NULL,
        [navn]         varchar(80)    NULL,
        [ktoart]       varchar(20)    NULL,
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
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_vareart_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_vareart_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_vareart] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_vareart_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_vareart]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_vareart_dw_timestamp_update]
        ON [arc].[ils_vareart] ([dw_timestamp_update] DESC);
END
GO
