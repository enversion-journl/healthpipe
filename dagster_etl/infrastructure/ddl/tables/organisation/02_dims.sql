-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Organisation: Dimension Table
-- dim.Organisation - Analytics-ready organisation dimension (SOR only)
-- Self-referencing parent-child hierarchy from SOR registry
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- ============================================================================

-- Create dim.Organisation if it does not exist
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[Organisation] (
        [DW_EK_Organisation] [int] PRIMARY KEY,
        [Code] [varchar](50) NOT NULL,
        [DW_EK_Org_Parent] [int],
        [ParentCode] [varchar](50),
        [Name] [nvarchar](500),
        [Type] [varchar](100),
        [Active] [bit] DEFAULT 1,
        [DW_Timestamp_Insert] [datetime2](7) DEFAULT GETDATE(),
        [DW_Timestamp_Update] [datetime2](7) DEFAULT GETDATE()
    ) ON [PRIMARY];

    CREATE INDEX IX_dim_Organisation_Code
        ON [dim].[Organisation] ([Code]);

    CREATE INDEX IX_dim_Organisation_Parent
        ON [dim].[Organisation] ([DW_EK_Org_Parent])
        WHERE [DW_EK_Org_Parent] IS NOT NULL;
END
GO

-- Sentinel row for "Unknown" organisation (DW_EK = -1)
IF NOT EXISTS (SELECT 1 FROM [dim].[Organisation] WHERE DW_EK_Organisation = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Organisation] (
            DW_EK_Organisation, Code,
            DW_EK_Org_Parent, ParentCode,
            Name, Active,
            DW_Timestamp_Insert, DW_Timestamp_Update
        )
        VALUES (
            -1, ''UNKNOWN'',
            NULL, NULL,
            ''Unknown Organisation'', 1,
            GETDATE(), GETDATE()
        )
    ');
END
GO
