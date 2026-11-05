-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.finanspostering_sydax  (1:1 from SydAX [Finans].[Finanspostering])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanspostering'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanspostering] (
        -- Merge keys
        [Kildesystem]                      VARCHAR(10)      NOT NULL,
        [ref_LedgertransId]                BIGINT           NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanspostering], minus merge keys)
        [FinansposteringId]                BIGINT           NULL,
        [FinanskontoPrismeId]              BIGINT           NULL,
        [FinanskontoId]                    BIGINT           NULL,
        [Bogfoeringsdato]                  DATE             NULL,
        [Posteringsbeloeb]                 NUMERIC(28,12)   NULL,
        [Valutakode]                       CHAR(3)          NULL,
        [PosteringsbeloebValuta]           NUMERIC(28,12)   NULL,
        [Posteringstekst]                  VARCHAR(100)     NULL,
        [AfdelingPrismeId]                 BIGINT           NULL,
        [Bilagsnummer]                     VARCHAR(100)     NULL,
        [ErModpostering]                   BIT              NULL,
        [DebetKredit]                      CHAR(1)          NULL,
        [FinanskladdeId]                   BIGINT           NULL,
        [FinanstransaktionstypeId]         BIGINT           NULL,
        [FinansposteringstypeId]           BIGINT           NULL,
        [KontrolleretDato]                 DATE             NULL,
        [KontrolleretAf]                   VARCHAR(10)      NULL,
        [Oprettelsestidspunkt]             DATETIME2(7)     NULL,
        [OprettetAf]                       VARCHAR(10)      NULL,
        [RunId]                            INT              NULL,
        [StartDateTime]                    DATETIME2(0)     NULL,
        [EndDateTime]                      DATETIME2(0)     NULL,
        [RdwTs]                            DATETIME2(7)     NULL,
        [BilagsnummerID]                   BIGINT           NULL,
        [Posteringsmoms]                   NUMERIC(28,12)   NULL,
        [PosteringsbeloebInklMoms]         NUMERIC(28,12)   NULL,
        [DokumentId]                       BIGINT           NULL,
        [BærerId]                          BIGINT           NULL,
        [FormålId]                         BIGINT           NULL,
        [RefusionModtager]                 VARCHAR(10)      NULL,
        [Lager_Kasse]                      VARCHAR(10)      NULL,
        [Ekspeditionsnummer]               INT              NULL,
        [SDIdentifier]                     NVARCHAR(26)     NULL,
        [Primosaldo]                       NUMERIC(28,12)   NULL,
        [DataAreaId]                       NVARCHAR(4)      NULL,
        [FinansrelationId]                 BIGINT           NULL,
        [Oplysningspligt]                  CHAR(1)          NULL,
        [CreatedOn]                        DATETIME         NULL,
        [Modified]                         DATETIME         NULL,
        [Varemomsgruppe]                   VARCHAR(10)      NULL,
        [Bogføringstypetekst]              VARCHAR(1000)    NULL,

        -- DWH audit columns
        [dw_timestamp_insert]              DATETIME2(0)     NOT NULL,
        [dw_timestamp_update]              DATETIME2(0)     NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanspostering] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_LedgertransId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_Modified]
        ON [arc].[sydax_finanspostering] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_CreatedOn]
        ON [arc].[sydax_finanspostering] ([CreatedOn] DESC);
END
GO
