-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.finanskonto_omkostningssted_sydax  (1:1 from SydAX [Finans].[FinanskontoOmkostningssted])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskonto_omkostningssted'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskonto_omkostningssted] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [FinanskontoId]                        BIGINT         NOT NULL,
        [Omkostningsstedid]                    BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[FinanskontoOmkostningssted], minus merge keys)
        [FinanskontoOmkostningsstedid]         BIGINT         NOT NULL,
        [ref_LedgerTableId]                    BIGINT         NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                  DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskonto_omkostningssted] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanskontoOmkostningsstedid], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_omkostningssted_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto_omkostningssted]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_omkostningssted_Modified]
        ON [arc].[sydax_finanskonto_omkostningssted] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_omkostningssted_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto_omkostningssted]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_omkostningssted_CreatedOn]
        ON [arc].[sydax_finanskonto_omkostningssted] ([CreatedOn] DESC);
END
GO
