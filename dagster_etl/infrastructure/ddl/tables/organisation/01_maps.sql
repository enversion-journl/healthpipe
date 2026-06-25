-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Organisation: Map Table
-- map.Organisation - CleanCode -> DW_EK resolution (SOR only)
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- ============================================================================

-- Create map.Organisation if it does not exist
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[Organisation] (
        [DW_EK_Organisation] [int] IDENTITY(1, 1) NOT NULL,
        [CleanCode] [varchar](100) UNIQUE NOT NULL,
        [Code] [varchar](50) NOT NULL,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_Organisation] PRIMARY KEY CLUSTERED ([DW_EK_Organisation] ASC)
    ) ON [PRIMARY];

    CREATE INDEX IX_map_Organisation_CleanCode
        ON [map].[Organisation] ([CleanCode]);
END
GO
