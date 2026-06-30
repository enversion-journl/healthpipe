-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: index arc.spend_analysis_lines for IndkoebsanalyseLinje delta staging

IF OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_spend_analysis_lines_kmd_uddata_timestamp_update'
          AND object_id = OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_spend_analysis_lines_kmd_uddata_timestamp_update]
        ON [arc].[spend_analysis_lines_kmd_uddata] ([DW_Timestamp_Update])
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
