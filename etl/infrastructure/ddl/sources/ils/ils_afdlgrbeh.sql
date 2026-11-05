-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: afdlgrbeh from ILS (Department Stock)
-- 1:1 reflection of ILS_pro2_complete.dbo.afdlgrbeh (17 columns)
-- Insert-only — rows are never updated, snapshots accumulate over time.
-- Used by arc_ils_afdlgrbeh archive merge asset
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_afdlgrbeh'
)
BEGIN
    CREATE TABLE [arc].[ils_afdlgrbeh] (
        -- Source columns (1:1 from dbo.afdlgrbeh)
        [prrowid]          VARCHAR(36)    NOT NULL,
        [sgh]              VARCHAR(8)     NULL,
        [lgr]              VARCHAR(8)     NULL,
        [brik]             INT            NULL,
        [rnr]              VARCHAR(16)    NULL,
        [vnr]              VARCHAR(24)    NULL,
        [antal]            DECIMAL(18,3)  NULL,
        [lnr]              VARCHAR(20)    NULL,
        [batchnr]          VARCHAR(24)    NULL,
        [udlobdto]         DATETIME       NULL,
        [lotnr]            VARCHAR(24)    NULL,
        [sysdato]          DATETIME       NULL,
        [bruger]           VARCHAR(6)     NULL,
        [seqnr]            INT            NULL,
        [Pro2SrcPDB]       VARCHAR(12)    NULL,
        [pro2created]      DATETIME       NULL,
        [pro2modified]     DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert] DATETIME2  NOT NULL,
        [dw_timestamp_update] DATETIME2  NOT NULL
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_afdlgrbeh_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_afdlgrbeh]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_afdlgrbeh_dw_timestamp_update]
        ON [arc].[ils_afdlgrbeh] ([dw_timestamp_update] DESC);
END
GO
