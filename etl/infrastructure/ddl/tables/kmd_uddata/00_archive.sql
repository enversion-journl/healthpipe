-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: KMD UdData (UNSPSC + SpendAnalysisLines)
--
-- 1:1 reflection of the KMD Indkøbsanalyse source views.
-- Delta-loaded via audit-MAX watermark; UPSERT-merged on the entity key.
-- See DSI/mssql/archive/kmd_uddata_archive_configs.py
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.kmd_uddata_unspsc  (1:1 from KMD dimension.UNSPSC)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'kmd_uddata_unspsc'
)
BEGIN
    CREATE TABLE [arc].[kmd_uddata_unspsc] (
        -- Source columns (1:1 from KMD dimension.UNSPSC)
        [DW_SK_UNSPSC]                  INT             NOT NULL,
        [DW_EK_UNSPSC]                  INT             NOT NULL,
        [UNSPSCClassID]                 NVARCHAR(100)   NULL,
        [UNSPSCClassDescription]        NVARCHAR(400)   NULL,
        [UNSPSCLevel]                   SMALLINT        NULL,
        [TenderCategoryKey]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev1ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev1Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev1Label]          NVARCHAR(502)   NULL,
        [UNSPSCClassLev2ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev2Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev2Label]          NVARCHAR(502)   NULL,
        [UNSPSCClassLev3ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev3Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev3Label]          NVARCHAR(502)   NULL,
        [UNSPSCClassLev4ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev4Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev4Label]          NVARCHAR(502)   NULL,
        [DW_ArchiveID]                  BIGINT          NULL,
        [DW_ValidFrom]                  DATETIME2       NULL,
        [DW_ValidTo]                    DATETIME2       NULL,
        [DW_IsCurrent]                  BIT             NULL,
        [DW_Audit_Insert]               INT             NULL,
        [DW_Audit_Update]               INT             NULL,
        [DW_Audit]                      INT             NULL,

        -- DWH audit columns
        [dw_timestamp_insert]           DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]           DATETIME2(0)    NOT NULL,

        -- Persisted MAX of the three audit columns; backs the watermark lookup.
        -- Inline CASE: PERSISTED computed columns cannot reference UDFs; native
        -- GREATEST() is unavailable on SQL Server 2019 anyway.
        [computed_max_audit] AS (
            CASE
                WHEN [DW_Audit_Insert] IS NULL
                 AND [DW_Audit_Update] IS NULL
                 AND [DW_Audit] IS NULL
                    THEN NULL
                WHEN [DW_Audit_Insert] >= COALESCE([DW_Audit_Update], [DW_Audit_Insert])
                 AND [DW_Audit_Insert] >= COALESCE([DW_Audit], [DW_Audit_Insert])
                    THEN [DW_Audit_Insert]
                WHEN [DW_Audit_Update] >= COALESCE([DW_Audit], [DW_Audit_Update])
                    THEN [DW_Audit_Update]
                ELSE [DW_Audit]
            END
        ) PERSISTED,

        -- Source is SCD Type 2: DW_SK_UNSPSC is unique per row (one per version),
        -- DW_EK_UNSPSC is the entity key and may repeat across versions.
        CONSTRAINT [PK_arc_kmd_uddata_unspsc]
            PRIMARY KEY CLUSTERED ([DW_SK_UNSPSC])
    );
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_unspsc]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_unspsc_computed_max_audit'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_unspsc]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_unspsc_computed_max_audit]
        ON [arc].[kmd_uddata_unspsc] ([computed_max_audit] DESC);
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_unspsc]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_unspsc_dw_ek_unspsc'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_unspsc]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_unspsc_dw_ek_unspsc]
        ON [arc].[kmd_uddata_unspsc] ([DW_EK_UNSPSC]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.kmd_uddata_spend_analysis_lines  (1:1 from KMD fact.SpendAnalysisLines)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'kmd_uddata_spend_analysis_lines'
)
BEGIN
    CREATE TABLE [arc].[kmd_uddata_spend_analysis_lines] (
        -- Source columns (1:1 from KMD fact.SpendAnalysisLines)
        [DW_EK_SpendAnalysisLine]              BIGINT      NOT NULL,
        [DW_SK_Compliance]                     INT         NULL,
        [DW_SK_Date_SpendAnalysis]             INT         NULL,
        [DW_SK_Date_Issue]                     INT         NULL,
        [DW_SK_Date_OrderIssue]                INT         NULL,
        [DW_SK_Date_ActualDelivery]            INT         NULL,
        [DW_SK_Date_PaymentDue]                INT         NULL,
        [DW_SK_Date_Posting]                   INT         NULL,
        [DW_SK_Economy]                        INT         NULL,
        [DW_SK_PostingText]                    INT         NULL,
        [DW_SK_EconomyCustom]                  INT         NULL,
        [DW_SK_EconomyLine]                    INT         NULL,
        [DW_SK_AppendixNumber]                 INT         NULL,
        [DW_SK_ExchangeRates]                  INT         NULL,
        [DW_SK_ExpenceType]                    INT         NULL,
        [DW_SK_GeoData]                        INT         NULL,
        [DW_SK_Invoice]                        INT         NULL,
        [DW_SK_InvoiceLine]                    INT         NULL,
        [DW_SK_AllowanceChargeLine]            INT         NULL,
        [DW_SK_InvoiceType]                    INT         NULL,
        [DW_SK_LocalPurchase]                  INT         NULL,
        [DW_SK_NACE]                           INT         NULL,
        [DW_SK_Online]                         INT         NULL,
        [DW_SK_EtradeObligation]               INT         NULL,
        [DW_SK_OrderType]                      INT         NULL,
        [DW_SK_Organic]                        INT         NULL,
        [DW_SK_Organization]                   INT         NULL,
        [DW_SK_PriceAgreement]                 INT         NULL,
        [DW_SK_ProcurementAgreement]           INT         NULL,
        [DW_SK_ProcurementType]                INT         NULL,
        [DW_SK_SpendAnalysisLineItem]          INT         NULL,
        [DW_SK_Supplier]                       INT         NULL,
        [DW_SK_TenderGroup]                    INT         NULL,
        [DW_SK_UNSPSC]                         INT         NULL,
        [LineAmount]                           NUMERIC(38, 6) NULL,
        [InvoicedQuantity]                     NUMERIC(38, 6) NULL,
        [AgreementItemPrice]                   NUMERIC(38, 6) NULL,
        [RatePercentNumeric]                   NUMERIC(38, 6) NULL,
        [LineTaxAmount]                        NUMERIC(38, 6) NULL,
        [UnitPrice]                            NUMERIC(38, 6) NULL,
        [UnitPriceExchange]                    NUMERIC(38, 6) NULL,
        [ObservedUnitPrice]                    NUMERIC(38, 6) NULL,
        [PriceAgreementItemInScopeIndicator]   INT         NULL,
        [LinePercentOfTotal]                   NUMERIC(38, 6) NULL,
        [IsDiffLine]                           INT         NULL,
        [CO2Emission]                          NUMERIC(38, 6) NULL,
        [DW_ValidDate]                         DATE        NULL,
        [DW_ArchiveID]                         BIGINT      NULL,
        [DW_Audit]                             INT         NULL,
        [DW_Audit_Insert]                      INT         NULL,
        [DW_Audit_Update]                      INT         NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                  DATETIME2(0) NOT NULL,
        [dw_timestamp_update]                  DATETIME2(0) NOT NULL,

        -- Persisted MAX of the three audit columns; backs the watermark lookup.
        -- Inline CASE: PERSISTED computed columns cannot reference UDFs; native
        -- GREATEST() is unavailable on SQL Server 2019 anyway.
        [computed_max_audit] AS (
            CASE
                WHEN [DW_Audit_Insert] IS NULL
                 AND [DW_Audit_Update] IS NULL
                 AND [DW_Audit] IS NULL
                    THEN NULL
                WHEN [DW_Audit_Insert] >= COALESCE([DW_Audit_Update], [DW_Audit_Insert])
                 AND [DW_Audit_Insert] >= COALESCE([DW_Audit], [DW_Audit_Insert])
                    THEN [DW_Audit_Insert]
                WHEN [DW_Audit_Update] >= COALESCE([DW_Audit], [DW_Audit_Update])
                    THEN [DW_Audit_Update]
                ELSE [DW_Audit]
            END
        ) PERSISTED,

        CONSTRAINT [PK_arc_kmd_uddata_spend_analysis_lines]
            PRIMARY KEY CLUSTERED ([DW_EK_SpendAnalysisLine])
    );
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_spend_analysis_lines_computed_max_audit'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_spend_analysis_lines_computed_max_audit]
        ON [arc].[kmd_uddata_spend_analysis_lines] ([computed_max_audit] DESC);
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_spend_analysis_lines_unspsc_validdate'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_spend_analysis_lines_unspsc_validdate]
        ON [arc].[kmd_uddata_spend_analysis_lines] ([DW_SK_UNSPSC], [DW_ValidDate]);
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_spend_analysis_lines_invoice_line'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_spend_analysis_lines_invoice_line]
        ON [arc].[kmd_uddata_spend_analysis_lines] ([DW_SK_Invoice], [DW_SK_InvoiceLine])
        INCLUDE ([LineAmount], [InvoicedQuantity], [UnitPrice], [DW_SK_UNSPSC], [DW_ValidDate]);
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_spend_analysis_lines_timestamp_update'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_spend_analysis_lines]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_spend_analysis_lines_timestamp_update]
        ON [arc].[kmd_uddata_spend_analysis_lines] ([dw_timestamp_update])
        INCLUDE (
            [DW_EK_SpendAnalysisLine],
            [DW_SK_Invoice],
            [DW_SK_UNSPSC],
            [DW_SK_Date_SpendAnalysis],
            [DW_SK_Date_Issue],
            [DW_SK_Date_OrderIssue],
            [DW_SK_Date_ActualDelivery],
            [DW_SK_Date_PaymentDue],
            [DW_SK_Date_Posting],
            [LineAmount],
            [InvoicedQuantity],
            [UnitPrice],
            [LineTaxAmount],
            [CO2Emission],
            [DW_ValidDate]
        );
END
GO


-- ----------------------------------------------------------------------------
-- arc.kmd_uddata_invoice  (1:1 from KMD dimension.Invoice)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'kmd_uddata_invoice'
)
BEGIN
    CREATE TABLE [arc].[kmd_uddata_invoice] (
        [DW_SK_Invoice]                         INT             NOT NULL,
        [DW_EK_Invoice]                         INT             NOT NULL,
        [GUID]                                  UNIQUEIDENTIFIER NULL,
        [SupplierCompanyTaxID]                  NVARCHAR(50)    NULL,
        [InvoiceNumber]                         NVARCHAR(50)    NULL,
        [InvoiceUUID]                           NVARCHAR(50)    NULL,
        [IsCreditnote]                          INT             NULL,
        [Filename]                              NVARCHAR(300)   NULL,
        [FileFormat]                            NVARCHAR(20)    NULL,
        [InvoiceSize]                           NVARCHAR(50)    NULL,
        [HasItemID]                             INT             NULL,
        [HasUnitCode]                           INT             NULL,
        [HasItemDescription]                    INT             NULL,
        [HasOrderID]                            INT             NULL,
        [HasOrderIssueDate]                     INT             NULL,
        [HasUnitPrice]                          INT             NULL,
        [InvoiceNote]                           NVARCHAR(500)   NULL,
        [DeliveryLocationPostalZone]            NVARCHAR(10)    NULL,
        [DeliveryLocationAddressCityName]       NVARCHAR(100)   NULL,
        [DeliveryLocationStreetName]            NVARCHAR(100)   NULL,
        [DeliveryLocationBuildingNumber]        NVARCHAR(10)    NULL,
        [AccountingLocationCode]                NVARCHAR(20)    NULL,
        [LineExtensionTotalAmount]              NUMERIC(38, 6)  NULL,
        [PostalZoneSupplierFromInvoice]         NVARCHAR(10)    NULL,
        [SupplierCityFromInvoice]               NVARCHAR(100)   NULL,
        [SupplierAddressFromInvoice]            NVARCHAR(100)   NULL,
        [SupplierBuildingNumberFromInvoice]     NVARCHAR(10)    NULL,
        [BuyersOrderID]                         NVARCHAR(50)    NULL,
        [BuyerReference]                        NVARCHAR(50)    NULL,
        [BuyersAddress]                         NVARCHAR(100)   NULL,
        [BuyersContact]                         NVARCHAR(50)    NULL,
        [BuyersContactID]                       NVARCHAR(50)    NULL,
        [BuyersEAN]                             NVARCHAR(50)    NULL,
        [ContractNumber]                        NVARCHAR(50)    NULL,
        [SupplierName]                          NVARCHAR(100)   NULL,
        [SupplierContact]                       NVARCHAR(50)    NULL,
        [SupplierContactID]                     NVARCHAR(50)    NULL,
        [SupplierOrderID]                       NVARCHAR(50)    NULL,
        [PaymentChannelCode]                    NVARCHAR(50)    NULL,
        [PenaltySurchargeRateNumeric]           NUMERIC(38, 6)  NULL,
        [PersonalSecure]                        NVARCHAR(10)    NULL,
        [PaymentPeriodDays]                     INT             NULL,
        [ForeignCurrency]                       NVARCHAR(10)    NULL,
        [InvoiceSizeID]                         INT             NULL,
        [IsDiscarded]                           INT             NULL,
        [DW_ArchiveID]                          BIGINT          NULL,
        [DW_ValidFrom]                          DATETIME2       NULL,
        [DW_ValidTo]                            DATETIME2       NULL,
        [DW_IsCurrent]                          BIT             NULL,
        [DW_Audit_Insert]                       INT             NULL,
        [DW_Audit_Update]                       INT             NULL,
        [DW_Audit]                              INT             NULL,

        [dw_timestamp_insert]                   DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]                   DATETIME2(0)    NOT NULL,

        [computed_max_audit] AS (
            CASE
                WHEN [DW_Audit_Insert] IS NULL
                 AND [DW_Audit_Update] IS NULL
                 AND [DW_Audit] IS NULL
                    THEN NULL
                WHEN [DW_Audit_Insert] >= COALESCE([DW_Audit_Update], [DW_Audit_Insert])
                 AND [DW_Audit_Insert] >= COALESCE([DW_Audit], [DW_Audit_Insert])
                    THEN [DW_Audit_Insert]
                WHEN [DW_Audit_Update] >= COALESCE([DW_Audit], [DW_Audit_Update])
                    THEN [DW_Audit_Update]
                ELSE [DW_Audit]
            END
        ) PERSISTED,

        CONSTRAINT [PK_arc_kmd_uddata_invoice]
            PRIMARY KEY CLUSTERED ([DW_SK_Invoice])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_kmd_uddata_invoice_computed_max_audit'
      AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_invoice]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_invoice_computed_max_audit]
        ON [arc].[kmd_uddata_invoice] ([computed_max_audit] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_kmd_uddata_invoice_dw_ek_invoice'
      AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_invoice]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_invoice_dw_ek_invoice]
        ON [arc].[kmd_uddata_invoice] ([DW_EK_Invoice]);
END
GO
