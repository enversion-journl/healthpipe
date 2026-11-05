-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: revert dim.IlsVare DW_EK_UNSPSC outrigger (revert)
-- UNSPSC FK belongs on facts via map.UNSPSC, not on dim.IlsVare.

IF OBJECT_ID(N'[dim].[IlsVare]') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]')
         AND name = 'IX_Dim_IlsVare_UNSPSC'
   )
BEGIN
    DROP INDEX [IX_Dim_IlsVare_UNSPSC] ON [dim].[IlsVare];
    PRINT 'Dropped IX_Dim_IlsVare_UNSPSC';
END
GO

IF OBJECT_ID(N'[dim].[IlsVare]') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[IlsVare]') AND name = 'DW_EK_UNSPSC'
   )
BEGIN
    DECLARE @df NVARCHAR(128);
    SELECT @df = dc.name
    FROM sys.default_constraints dc
    INNER JOIN sys.columns c
        ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[IlsVare]')
      AND c.name = 'DW_EK_UNSPSC';

    IF @df IS NOT NULL
        EXEC('ALTER TABLE [dim].[IlsVare] DROP CONSTRAINT [' + @df + ']');

    ALTER TABLE [dim].[IlsVare] DROP COLUMN [DW_EK_UNSPSC];
    PRINT 'Dropped DW_EK_UNSPSC from dim.IlsVare';
END
GO
