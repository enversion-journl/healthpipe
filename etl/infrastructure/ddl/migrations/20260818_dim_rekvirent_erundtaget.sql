-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Idempotent: add dim.Rekvirent.er_undtaget.
-- Convenience BIT flag from ref.RekvirentUndtagelse via (sgh, rnr). Default 0.

IF OBJECT_ID(N'[dim].[Rekvirent]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', N'er_undtaget') IS NULL
BEGIN
    ALTER TABLE [dim].[Rekvirent]
        ADD [er_undtaget] BIT NOT NULL
            CONSTRAINT [DF_dim_Rekvirent_er_undtaget] DEFAULT 0;
END
GO

IF OBJECT_ID(N'[dim].[Rekvirent]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]')
         AND name = 'IX_dim_Rekvirent_er_undtaget'
   )
BEGIN
    CREATE INDEX [IX_dim_Rekvirent_er_undtaget]
        ON [dim].[Rekvirent] ([er_undtaget])
        WHERE [er_undtaget] = 1;
END
GO
