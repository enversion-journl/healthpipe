-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
