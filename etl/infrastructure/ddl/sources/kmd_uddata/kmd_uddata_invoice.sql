-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
