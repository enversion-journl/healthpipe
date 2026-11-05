-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_budgetmodel  (1:1 from SydAX [Finans].[Budgetmodel])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_budgetmodel'
)
BEGIN
    CREATE TABLE [arc].[sydax_budgetmodel] (
        -- Merge keys
        [Kildesystem]       NVARCHAR(50)    NOT NULL,
        [BudgetmodelId]     BIGINT          NOT NULL,
        [StartDateTime]     DATETIME2(0)    NOT NULL,

        -- Source columns (1:1 from [Finans].[Budgetmodel], minus merge keys)
        [ref_BudgetmodelId] BIGINT          NULL,
        [Budgetmodelnummer] NVARCHAR(100)   NULL,
        [Budgetmodeltekst]  NVARCHAR(255)   NULL,
        [Budgetmodeltype]   NVARCHAR(100)   NULL,
        [ErLukket]          BIT             NULL,

        -- DFM envelope columns
        [RunId]             BIGINT          NULL,
        [EndDateTime]       DATETIME2(0)    NOT NULL,
        [RdwTs]             DATETIME2(7)    NULL,
        [CreatedOn]         DATETIME        NULL,
        [Modified]          DATETIME        NULL,

        -- DWH audit columns
        [dw_timestamp_insert] DATETIME2(0)  NOT NULL,
        [dw_timestamp_update] DATETIME2(0)  NOT NULL,

        CONSTRAINT [PK_arc_sydax_budgetmodel]
            PRIMARY KEY CLUSTERED ([Kildesystem], [BudgetmodelId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_budgetmodel_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_budgetmodel]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_budgetmodel_Modified]
        ON [arc].[sydax_budgetmodel] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_budgetmodel_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_budgetmodel]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_budgetmodel_CreatedOn]
        ON [arc].[sydax_budgetmodel] ([CreatedOn] DESC);
END
GO
