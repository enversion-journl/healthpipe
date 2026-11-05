-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Budget (SydAX)
-- 4 arc tables: budgetpost, budgetmodel, budgetpoststatus, budgetbeloebtype
-- 1:1 mirror of [Finans].[Budgetpost] / [Budgetmodel] / [BudgetpostStatus]
--   / [BudgetbeløbType] source tables.
-- Populated by DFM-managed archive merge assets.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.sydax_budgetpost  (1:1 from SydAX [Finans].[Budgetpost])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_budgetpost'
)
BEGIN
    CREATE TABLE [arc].[sydax_budgetpost] (
        -- Merge keys
        [Kildesystem]               NVARCHAR(50)    NOT NULL,
        [ref_LedgerbudgetId]        BIGINT          NOT NULL,

        -- Source columns (1:1 from [Finans].[Budgetpost], minus merge keys)
        [BudgetmodelId]             BIGINT          NULL,
        [FinanskontoId]             BIGINT          NULL,
        [BudgetpostStatusId]        BIGINT          NULL,
        [BeloebstypeId]             BIGINT          NULL,
        [Budgetdato]                NVARCHAR(50)    NULL,
        [BudgetAdoptedDate]         NVARCHAR(50)    NULL,
        [Oprettelsestidspunkt]      DATETIME2(0)    NULL,
        [SidstAendretTidspunkt]     DATETIME2(0)    NULL,
        [Budgetbeloeb]              DECIMAL(30,12)  NULL,
        [BudgetbeloebValuta]        DECIMAL(30,12)  NULL,
        [Valutakode]                NVARCHAR(10)    NULL,
        [DebetKredit]               NVARCHAR(10)    NULL,
        [Budgetkode]                NVARCHAR(50)    NULL,
        [Ændringstype]              NVARCHAR(100)   NULL,
        [ÆndringstypeBeskrivelse]   NVARCHAR(255)   NULL,

        -- DFM envelope columns
        [RunId]                     BIGINT          NULL,
        [StartDateTime]             DATETIME2(0)    NOT NULL,
        [EndDateTime]               DATETIME2(0)    NOT NULL,
        [RdwTs]                     DATETIME2(7)    NULL,
        [CreatedOn]                 DATETIME        NULL,
        [Modified]                  DATETIME        NULL,

        -- DWH audit columns
        [dw_timestamp_insert]       DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]       DATETIME2(0)    NOT NULL,

        CONSTRAINT [PK_arc_sydax_budgetpost]
            PRIMARY KEY CLUSTERED ([Kildesystem], [ref_LedgerbudgetId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_budgetpost_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_budgetpost]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_budgetpost_Modified]
        ON [arc].[sydax_budgetpost] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_budgetpost_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_budgetpost]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_budgetpost_CreatedOn]
        ON [arc].[sydax_budgetpost] ([CreatedOn] DESC);
END
GO
