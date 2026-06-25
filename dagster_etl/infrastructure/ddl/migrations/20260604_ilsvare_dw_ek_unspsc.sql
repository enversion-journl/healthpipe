-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: dim.IlsVare DW_EK_UNSPSC FK to dim.UNSPSC (task 27788)
--
-- Upgrade path for existing databases. Fresh installs get the column from 02_dims.sql.
-- Backfill uses deprecated text columns UNSPSC / UNSPSC_Intern (unspc / unspsc2 sources).

IF OBJECT_ID(N'[dim].[IlsVare]') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'DW_EK_UNSPSC'
   )
BEGIN
    ALTER TABLE [dim].[IlsVare] ADD [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_UNSPSC to dim.IlsVare';
END
GO

IF OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'DW_EK_UNSPSC'
   )
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC') IS NOT NULL
BEGIN
    EXEC(N'
        UPDATE iv
        SET iv.[DW_EK_UNSPSC] = COALESCE(um.[DW_EK_UNSPSC], -1)
        FROM [dim].[IlsVare] iv
        LEFT JOIN [map].[UNSPSC] um
            ON um.[CleanCode_UNSPSC] = COALESCE(
                NULLIF(LTRIM(RTRIM(iv.[UNSPSC])), ''''),
                CASE
                    WHEN iv.[UNSPSC_Intern] LIKE ''%.0''
                    THEN LEFT(
                        LTRIM(RTRIM(iv.[UNSPSC_Intern])),
                        LEN(LTRIM(RTRIM(iv.[UNSPSC_Intern]))) - 2
                    )
                    ELSE NULLIF(LTRIM(RTRIM(iv.[UNSPSC_Intern])), '''')
                END
            )
        WHERE iv.[DW_EK_IlsVare] <> -1;
    ');
    PRINT CONCAT('Backfilled dim.IlsVare.DW_EK_UNSPSC for ', @@ROWCOUNT, ' rows');
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'IX_Dim_IlsVare_UNSPSC'
)
AND OBJECT_ID(N'[dim].[IlsVare]') IS NOT NULL
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_IlsVare_UNSPSC]
    ON [dim].[IlsVare] ([DW_EK_UNSPSC] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Dim_IlsVare_UNSPSC';
END
GO
