-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_lafthsgh  (1:1 from ILS dbo.lafthsgh)
-- Hospital-level contract participation flags (which hospitals participate in
-- each supplier agreement).
-- Column list mirrors _NON_KEY_COLUMNS["lafthsgh"] in ils_archive_configs.py
-- plus the prrowid merge key. Must be verified against the real ILS source on
-- branch deployment (known deploy-gate for this table).
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lafthsgh'
)
BEGIN
    CREATE TABLE [arc].[ils_lafthsgh] (
        -- Source columns (1:1 from dbo.lafthsgh)
        [prrowid]      varchar(36)    NOT NULL,
        [lnr]          varchar(20)    NULL,
        [aftale]       varchar(24)    NULL,
        [sgh]          varchar(8)     NULL,
        [sw_benyt]     bit            NULL,
        [sw_prioritet] bit            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_lafthsgh_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_ils_lafthsgh] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_lafthsgh_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lafthsgh]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lafthsgh_dw_timestamp_update]
        ON [arc].[ils_lafthsgh] ([dw_timestamp_update] DESC);
END
GO
