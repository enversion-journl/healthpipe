-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: lgrbeh from ILS (Warehouse Stock)
-- 1:1 reflection of ILS_pro2_complete.dbo.lgrbeh (26 columns)
-- Insert-only — rows are never updated, snapshots accumulate over time.
-- Used by arc_ils_lgrbeh archive merge asset
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_lgrbeh'
)
BEGIN
    CREATE TABLE [arc].[ils_lgrbeh] (
        -- Source columns (1:1 from dbo.lgrbeh)
        [prrowid]          VARCHAR(36)    NOT NULL,
        [sgh]              VARCHAR(8)     NULL,
        [lgr]              VARCHAR(8)     NULL,
        [vnr]              VARCHAR(24)    NULL,
        [lgrlok]           VARCHAR(16)    NULL,
        [antal]            DECIMAL(18,3)  NULL,
        [batchnr]          VARCHAR(24)    NULL,
        [lnr]              VARCHAR(20)    NULL,
        [udlobdto]         DATETIME       NULL,
        [fralok]           VARCHAR(16)    NULL,
        [sw_egen_prod]     BIT            NULL,
        [sysdato]          DATETIME       NULL,
        [bruger]           VARCHAR(6)     NULL,
        [seqnr]            INT            NULL,
        [lotnr]            VARCHAR(24)    NULL,
        [udlevenh]         VARCHAR(40)    NULL,
        [bem]              VARCHAR(100)   NULL,
        [lvnr##1]          VARCHAR(168)   NULL,
        [lvnr##2]          VARCHAR(168)   NULL,
        [lvnr##3]          VARCHAR(168)   NULL,
        [lvnr##4]          VARCHAR(168)   NULL,
        [lgrbeh_prio]      INT            NULL,
        [sw_erstat]        BIT            NULL,
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
    WHERE name = 'IX_arc_ils_lgrbeh_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_lgrbeh]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_lgrbeh_dw_timestamp_update]
        ON [arc].[ils_lgrbeh] ([dw_timestamp_update] DESC);
END
GO
