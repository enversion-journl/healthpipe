-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_changelog  (1:1 from ILS dbo.changelog)
-- Append-only change log for selected fields in several ILS tables. Grain: one
-- row per change event (prrowid). No sysdato — the delta runs on Dato.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_changelog'
)
BEGIN
    CREATE TABLE [arc].[ils_changelog] (
        -- Source columns (1:1 from dbo.changelog)
        [prrowid]          varchar(36)    NOT NULL,
        [tabelnavn]        varchar(40)    NULL,
        [primaernoegle]    varchar(30)    NULL,
        [feltnavn]         varchar(30)    NULL,
        [fravaerdi]        varchar(40)    NULL,
        [tilvaerdi]        varchar(30)    NULL,
        [opret]            bit            NULL,
        [feltkonverteret]  bit            NULL,
        [bd_init]          varchar(6)     NULL,
        [Dato]             datetime       NULL,
        [klok]             int            NULL,
        [feltlabel]        varchar(60)    NULL,
        [Pro2SrcPDB]       varchar(12)    NULL,
        [pro2created]      datetime       NULL,
        [pro2modified]     datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_changelog_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_ils_changelog] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_changelog_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_changelog]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_changelog_dw_timestamp_update]
        ON [arc].[ils_changelog] ([dw_timestamp_update] DESC);
END
GO

-- Serves checkpoint_query MAX([Dato]); without it the query scans ~9.5M rows.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_changelog_Dato'
      AND object_id = OBJECT_ID(N'[arc].[ils_changelog]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_changelog_Dato]
        ON [arc].[ils_changelog] ([Dato] DESC);
END
GO
