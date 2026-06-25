-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Replace dim.IlsVare UNSPSC text columns with FK references to dim.UNSPSC
--
-- Replaces: UNSPSC, UNSPSC_Navn, UNSPSC_Intern, UNSPSC_Intern_Navn (4 text columns)
-- With:     DW_EK_UNSPSC, DW_EK_UNSPSC_Intern (2 INT FK to dim.UNSPSC)
--
-- The COALESCE(unspc, cleaned unspsc2) priority logic now lives in DimUpd_ilsvare.py
-- rather than being repeated across every fact RESOLVE query.

-- Step 1: Add DW_EK_UNSPSC if not present
IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'DW_EK_UNSPSC'
   )
BEGIN
    ALTER TABLE [dim].[IlsVare] ADD [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_UNSPSC to dim.IlsVare';
END
GO

-- Step 2: Add DW_EK_UNSPSC_Intern if not present
IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'DW_EK_UNSPSC_Intern'
   )
BEGIN
    ALTER TABLE [dim].[IlsVare] ADD [DW_EK_UNSPSC_Intern] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_UNSPSC_Intern to dim.IlsVare';
END
GO

-- Step 3: Backfill DW_EK_UNSPSC from text column UNSPSC (unspc)
IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC') IS NOT NULL
BEGIN
    EXEC(N'
        UPDATE iv
        SET iv.[DW_EK_UNSPSC] = COALESCE(um.[DW_EK_UNSPSC], -1)
        FROM [dim].[IlsVare] iv
        LEFT JOIN [map].[UNSPSC] um
            ON um.[CleanCode_UNSPSC] = NULLIF(LTRIM(RTRIM(iv.[UNSPSC])), '''')
        WHERE iv.[DW_EK_IlsVare] <> -1
          AND iv.[DW_EK_UNSPSC] = -1;
    ');
    PRINT CONCAT('Backfilled dim.IlsVare.DW_EK_UNSPSC: ', @@ROWCOUNT, ' rows');
END
GO

-- Step 4: Backfill DW_EK_UNSPSC_Intern from text column UNSPSC_Intern (unspsc2)
IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC_Intern') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC_Intern') IS NOT NULL
BEGIN
    EXEC(N'
        UPDATE iv
        SET iv.[DW_EK_UNSPSC_Intern] = COALESCE(um.[DW_EK_UNSPSC], -1)
        FROM [dim].[IlsVare] iv
        LEFT JOIN [map].[UNSPSC] um
            ON um.[CleanCode_UNSPSC] = CASE
                WHEN iv.[UNSPSC_Intern] LIKE ''%.0''
                THEN LEFT(
                    LTRIM(RTRIM(iv.[UNSPSC_Intern])),
                    LEN(LTRIM(RTRIM(iv.[UNSPSC_Intern]))) - 2
                )
                ELSE NULLIF(LTRIM(RTRIM(iv.[UNSPSC_Intern])), '''')
            END
        WHERE iv.[DW_EK_IlsVare] <> -1
          AND iv.[DW_EK_UNSPSC_Intern] = -1;
    ');
    PRINT CONCAT('Backfilled dim.IlsVare.DW_EK_UNSPSC_Intern: ', @@ROWCOUNT, ' rows');
END
GO

-- Step 5: Drop text columns (guard each individually)
IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC_Intern_Navn') IS NOT NULL
BEGIN
    ALTER TABLE [dim].[IlsVare] DROP COLUMN [UNSPSC_Intern_Navn];
    PRINT 'Dropped UNSPSC_Intern_Navn from dim.IlsVare';
END
GO

IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC_Intern') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC_Intern') IS NOT NULL
BEGIN
    ALTER TABLE [dim].[IlsVare] DROP COLUMN [UNSPSC_Intern];
    PRINT 'Dropped UNSPSC_Intern from dim.IlsVare';
END
GO

IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC_Navn') IS NOT NULL
BEGIN
    ALTER TABLE [dim].[IlsVare] DROP COLUMN [UNSPSC_Navn];
    PRINT 'Dropped UNSPSC_Navn from dim.IlsVare';
END
GO

IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC') IS NOT NULL
BEGIN
    ALTER TABLE [dim].[IlsVare] DROP COLUMN [UNSPSC];
    PRINT 'Dropped UNSPSC from dim.IlsVare';
END
GO

-- Step 6: Add indexes on the new FK columns
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'IX_Dim_IlsVare_UNSPSC'
)
AND OBJECT_ID(N'[dim].[IlsVare]') IS NOT NULL
AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC') IS NOT NULL
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_IlsVare_UNSPSC]
    ON [dim].[IlsVare] ([DW_EK_UNSPSC] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Dim_IlsVare_UNSPSC';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'IX_Dim_IlsVare_UNSPSC_Intern'
)
AND OBJECT_ID(N'[dim].[IlsVare]') IS NOT NULL
AND COL_LENGTH('dim.IlsVare', 'DW_EK_UNSPSC_Intern') IS NOT NULL
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_IlsVare_UNSPSC_Intern]
    ON [dim].[IlsVare] ([DW_EK_UNSPSC_Intern] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Dim_IlsVare_UNSPSC_Intern';
END
GO
