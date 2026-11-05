-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.finanskladde_sydax  (1:1 from SydAX [Finans].[Finanskladde])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskladde'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskladde] (
        -- Merge keys
        [Kildesystem]                      VARCHAR(10)    NOT NULL,
        [Kladdenummer]                     VARCHAR(20)    NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanskladde], minus merge keys)
        [FinanskladdeId]                   BIGINT         NOT NULL,
        [Kladdetekst]                      VARCHAR(1000)  NULL,
        [Kladdenavn]                       VARCHAR(100)   NULL,
        [ErLukket]                         BIT            NULL,
        [ref_LedgerjournaltableId]         BIGINT         NULL,
        [RunId]                            INT            NULL,
        [StartDateTime]                    DATETIME2(0)   NOT NULL,
        [EndDateTime]                      DATETIME2(0)   NULL,
        [RdwTs]                            DATETIME2(7)   NULL,
        [DokumentId]                       BIGINT         NULL,
        [DataAreaId]                       NVARCHAR(4)    NULL,
        [Maskinnummer]                     VARCHAR(5)     NULL,
        [CreatedOn]                        DATETIME       NULL,
        [Modified]                         DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]              DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]              DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskladde] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanskladdeId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladde_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladde]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladde_Modified]
        ON [arc].[sydax_finanskladde] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladde_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladde]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladde_CreatedOn]
        ON [arc].[sydax_finanskladde] ([CreatedOn] DESC);
END
GO
