-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_hovedkonto  (1:1 from SydAX [Finans].[Hovedkonto])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_hovedkonto'
)
BEGIN
    CREATE TABLE [arc].[sydax_hovedkonto] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [ref_RECID]                            BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[Hovedkonto], minus merge keys)
        [HovedkontoId]                         BIGINT         NULL,
        [Hovedkontonummer]                     VARCHAR(20)    NULL,
        [Hovedkontotekst]                      VARCHAR(60)    NULL,
        [KontoplanId]                          BIGINT         NULL,
        [HovedkontotypeId]                     BIGINT         NULL,
        [RapporteringstypeId]                  BIGINT         NULL,
        [HovedkontoKategoriId]                 BIGINT         NULL,
        [DKForslag]                            VARCHAR(10)    NULL,
        [DKKrav]                               VARCHAR(10)    NULL,
        [Saldokontrol]                         VARCHAR(10)    NULL,
        [ModkontoId]                           BIGINT         NULL,
        [PrimokontoId]                         BIGINT         NULL,
        [SRUKode]                              VARCHAR(10)    NULL,
        [Afslut]                               VARCHAR(10)    NULL,
        [TvungenReference]                     VARCHAR(10)    NULL,
        [CloseType]                            VARCHAR(10)    NULL,
        [Autoriseret]                          VARCHAR(10)    NULL,
        [Standardkoncernkonto]                 VARCHAR(10)    NULL,
        [F1]                                   VARCHAR(4)     NULL,
        [F2]                                   VARCHAR(4)     NULL,
        [F3]                                   VARCHAR(4)     NULL,
        [D1]                                   VARCHAR(4)     NULL,
        [G1]                                   VARCHAR(4)     NULL,
        [Valuta]                               VARCHAR(10)    NULL,
        [ValutaValidering]                     VARCHAR(10)    NULL,
        [BrugerId]                             VARCHAR(10)    NULL,
        [BrugerValidering]                     VARCHAR(10)    NULL,
        [FinansposteringstypeId]               BIGINT         NULL,
        [KonteringValidering]                  VARCHAR(10)    NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [ref_MainAccountId]                    BIGINT         NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                  DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_hovedkonto] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_hovedkonto_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_hovedkonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_hovedkonto_Modified]
        ON [arc].[sydax_hovedkonto] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_hovedkonto_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_hovedkonto]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_hovedkonto_CreatedOn]
        ON [arc].[sydax_hovedkonto] ([CreatedOn] DESC);
END
GO
