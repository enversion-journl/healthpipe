-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Add DW_EK_IlsVare FK to dim.Vare (#27494)
-- Adds the IlsVare foreign key column and index to existing dim.Vare tables.
-- Idempotent — safe to run multiple times.

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND type = 'U')
BEGIN
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Vare' AND COLUMN_NAME = 'DW_EK_IlsVare'
    )
    BEGIN
        ALTER TABLE [dim].[Vare]
        ADD [DW_EK_IlsVare] INT NOT NULL DEFAULT -1;
        PRINT 'Added DW_EK_IlsVare to dim.Vare';
    END
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[Vare]') AND name = 'IX_Dim_Vare_IlsVare'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_Vare_IlsVare]
    ON [dim].[Vare] ([DW_EK_IlsVare]) ON [PRIMARY];
END
GO
