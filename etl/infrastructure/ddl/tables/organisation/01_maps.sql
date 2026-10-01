-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Organisation: Map Table
-- map.Organisation - clean_code -> DW_EK resolution (SOR only)
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
        [dw_ek_Organisation] [int] IDENTITY(1, 1) NOT NULL,
        [clean_code] [varchar](100) UNIQUE NOT NULL,
        [code] [varchar](50) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Organisation_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Organisation_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_Organisation] PRIMARY KEY CLUSTERED ([dw_ek_Organisation] ASC)
    ) ON [PRIMARY];

    CREATE INDEX IX_map_Organisation_clean_code
        ON [map].[Organisation] ([clean_code]);
END
GO
