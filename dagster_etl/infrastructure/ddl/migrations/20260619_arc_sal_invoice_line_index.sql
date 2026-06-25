-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: composite index on arc.spend_analysis_lines for invoice-line aggregation

IF OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]', 'U') IS NOT NULL
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_spend_analysis_lines_kmd_uddata_dw_sk_invoice'
          AND object_id = OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]')
    )
    BEGIN
        DROP INDEX [IX_arc_spend_analysis_lines_kmd_uddata_dw_sk_invoice]
            ON [arc].[spend_analysis_lines_kmd_uddata];
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_spend_analysis_lines_kmd_uddata_invoice_line'
          AND object_id = OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]')
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_arc_spend_analysis_lines_kmd_uddata_invoice_line]
            ON [arc].[spend_analysis_lines_kmd_uddata] ([DW_SK_Invoice], [DW_SK_InvoiceLine])
            INCLUDE ([LineAmount], [InvoicedQuantity], [UnitPrice], [DW_SK_UNSPSC], [DW_ValidDate]);
    END
END
GO
