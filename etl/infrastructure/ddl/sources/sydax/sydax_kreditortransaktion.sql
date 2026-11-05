-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.kreditortransaktion_sydax  (1:1 from SydAX [Kreditor].[Kreditortransaktion])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_kreditortransaktion'
)
BEGIN
    CREATE TABLE [arc].[sydax_kreditortransaktion] (
        -- Merge keys
        [Kildesystem]                              VARCHAR(10)      NOT NULL,
        [ref_VendTransId]                          BIGINT           NOT NULL,

        -- Source columns (1:1 from [Kreditor].[Kreditortransaktion], minus merge keys)
        [KreditortransaktionId]                    BIGINT           NULL,
        [KreditorId]                               BIGINT           NULL,
        [Transaktionsdato]                         DATE             NULL,
        [Bilagsnummer]                             VARCHAR(100)     NULL,
        [Fakturanummer]                            VARCHAR(100)     NULL,
        [EANnummer]                                VARCHAR(100)     NULL,
        [Fakturadato]                              DATE             NULL,
        [Transaktionsbeloeb]                       NUMERIC(28,12)   NULL,
        [Valutakode]                               CHAR(3)          NULL,
        [TransaktionsbeloebValuta]                 NUMERIC(28,12)   NULL,
        [UdlignetBeloeb]                           NUMERIC(28,12)   NULL,
        [UdlignetBeloebValuta]                     NUMERIC(28,12)   NULL,
        [Transaktionstekst]                        VARCHAR(100)     NULL,
        [Forfaldsdato]                             DATE             NULL,
        [ErLukket]                                 BIT              NULL,
        [LukketDato]                               DATE             NULL,
        [AfdelingPrismeId]                         BIGINT           NULL,
        [JuridiskEnhed]                            VARCHAR(30)      NULL,
        [Bankregistreringsnummer]                  VARCHAR(10)      NULL,
        [Bankkontonummer]                          VARCHAR(30)      NULL,
        [ErKorrektion]                             BIT              NULL,
        [KreditortransaktionstypeId]               BIGINT           NULL,
        [ErGodkendt]                               BIT              NULL,
        [GodkendtDato]                             DATE             NULL,
        [GodkendtAf]                               VARCHAR(50)      NULL,
        [Oprettelsestidspunkt]                     DATETIME2(7)     NULL,
        [OprettetAf]                               VARCHAR(10)      NULL,
        [IsCounterpost]                            BIT              NULL,
        [CounterpostsThisKreditortransaktionId]    BIGINT           NULL,
        [Betalingstekst]                           NVARCHAR(255)    NULL,
        [BetalingReturkode]                        VARCHAR(4)       NULL,
        [BetalingYderligerInfo]                    VARCHAR(105)     NULL,
        [Virksomhedsenhed]                         NVARCHAR(20)     NULL,
        [VirksomhedsenhedNavn]                     NVARCHAR(100)    NULL,
        [RunId]                                    INT              NULL,
        [StartDateTime]                            DATETIME2(0)     NULL,
        [EndDateTime]                              DATETIME2(0)     NULL,
        [RdwTs]                                    DATETIME2(7)     NULL,
        [SidstUdlignetDato]                        DATE             NULL,
        [SidstUdlignetBilagsnummer]                VARCHAR(100)     NULL,
        [DokumentId]                               BIGINT           NULL,
        [DataAreaId]                               NVARCHAR(4)      NULL,
        [CreatedOn]                                DATETIME         NULL,
        [Modified]                                 DATETIME         NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                      DATETIME2(0)     NOT NULL,
        [dw_timestamp_update]                      DATETIME2(0)     NOT NULL,

        CONSTRAINT [PK_arc_sydax_kreditortransaktion] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_VendTransId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditortransaktion_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditortransaktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditortransaktion_Modified]
        ON [arc].[sydax_kreditortransaktion] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditortransaktion_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditortransaktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditortransaktion_CreatedOn]
        ON [arc].[sydax_kreditortransaktion] ([CreatedOn] DESC);
END
GO
