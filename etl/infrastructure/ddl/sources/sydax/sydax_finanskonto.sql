-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.finanskonto_sydax  (1:1 from SydAX [Finans].[Finanskonto])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskonto'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskonto] (
        -- Merge keys
        [Kildesystem]                                          VARCHAR(10)    NOT NULL,
        [Kontonummer]                                          VARCHAR(500)   NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanskonto], minus merge keys)
        [FinanskontoId]                                        BIGINT         NOT NULL,
        [ErLukket]                                             BIT            NULL,
        [ve_RECID]                                             BIGINT         NULL,
        [afd_RECID]                                            BIGINT         NULL,
        [os_RECID]                                             BIGINT         NULL,
        [hk_RECID]                                             BIGINT         NULL,
        [ma_RECID]                                             BIGINT         NULL,
        [grp_RECID]                                            BIGINT         NULL,
        [art_RECID]                                            BIGINT         NULL,
        [d1_RECID]                                             BIGINT         NULL,
        [d2_RECID]                                             BIGINT         NULL,
        [l_RECID]                                              BIGINT         NULL,
        [ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId]            BIGINT         NULL,
        [RunId]                                                INT            NULL,
        [StartDateTime]                                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                                          DATETIME2(0)   NULL,
        [RdwTs]                                                DATETIME2(7)   NULL,
        [ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID]         BIGINT         NULL,
        [CreatedOn]                                            DATETIME       NULL,
        [Modified]                                             DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                                  DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]                                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskonto] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanskontoId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_Modified]
        ON [arc].[sydax_finanskonto] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskonto_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskonto_CreatedOn]
        ON [arc].[sydax_finanskonto] ([CreatedOn] DESC);
END
GO
