-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.finanskladdelinje_sydax  (1:1 from SydAX [Finans].[Finanskladdelinje])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanskladdelinje'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanskladdelinje] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)      NOT NULL,
        [ref_LedgertransId]        BIGINT           NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanskladdelinje], minus merge keys)
        [FinanskladdelinjeId]      BIGINT           NULL,
        [Linjenummer]              DECIMAL(32,16)   NULL,
        [dato]                     DATE             NULL,
        [Beskrivelse]              VARCHAR(100)     NULL,
        [Debet]                    NUMERIC(28,12)   NULL,
        [Kredit]                   NUMERIC(28,12)   NULL,
        [FinanskladdeId]           BIGINT           NULL,
        [FinanstransaktionstypeId] BIGINT           NULL,
        [Ekspeditionsnummer]       VARCHAR(7)       NULL,
        [Maskinnummer]             VARCHAR(5)       NULL,
        [FinanskontoId]            BIGINT           NULL,
        [BilagsnummerID]           BIGINT           NULL,
        [DokumentId]               BIGINT           NULL,
        [ref_RecId]                BIGINT           NULL,
        [RunId]                    INT              NULL,
        [StartDateTime]            DATETIME2(0)     NULL,
        [EndDateTime]              DATETIME2(0)     NULL,
        [RdwTs]                    DATETIME2(7)     NULL,
        [CreatedOn]                DATETIME         NULL,
        [Modified]                 DATETIME         NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)     NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)     NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanskladdelinje] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_LedgertransId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladdelinje_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladdelinje]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladdelinje_Modified]
        ON [arc].[sydax_finanskladdelinje] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanskladdelinje_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanskladdelinje]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanskladdelinje_CreatedOn]
        ON [arc].[sydax_finanskladdelinje] ([CreatedOn] DESC);
END
GO
