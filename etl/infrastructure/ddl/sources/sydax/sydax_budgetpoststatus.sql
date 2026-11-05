-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_budgetpoststatus  (1:1 from SydAX [Finans].[BudgetpostStatus])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_budgetpoststatus'
)
BEGIN
    CREATE TABLE [arc].[sydax_budgetpoststatus] (
        -- Merge keys
        [Kildesystem]           NVARCHAR(50)    NOT NULL,
        [BudgetpostStatusId]    BIGINT          NOT NULL,
        [StartDateTime]         DATETIME2(0)    NOT NULL,

        -- Source columns (1:1 from [Finans].[BudgetpostStatus], minus merge keys)
        [BudgetpostStatustekst] NVARCHAR(255)   NULL,

        -- DFM envelope columns
        [RunId]                 BIGINT          NULL,
        [EndDateTime]           DATETIME2(0)    NOT NULL,
        [RdwTs]                 DATETIME2(7)    NULL,
        [CreatedOn]             DATETIME        NULL,
        [Modified]              DATETIME        NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)    NOT NULL,

        CONSTRAINT [PK_arc_sydax_budgetpoststatus]
            PRIMARY KEY CLUSTERED ([Kildesystem], [BudgetpostStatusId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_budgetpoststatus_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_budgetpoststatus]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_budgetpoststatus_Modified]
        ON [arc].[sydax_budgetpoststatus] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_budgetpoststatus_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_budgetpoststatus]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_budgetpoststatus_CreatedOn]
        ON [arc].[sydax_budgetpoststatus] ([CreatedOn] DESC);
END
GO
