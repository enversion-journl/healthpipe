-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: stg.kmd_fakturalinje_unspsc_match — CleanCode keys + tiered match
--
-- Replaces InvoiceNumber_Norm/SupplierCVR with CleanCode_Kreditor/CleanCode_Faktura
-- (same builders as pre_stg_fakturalinje_mysupply). Adds MatchTier and InvoiceTotal
-- for header-level fallback. Populated by stg_kmd_fakturalinje_unspsc_match.

IF OBJECT_ID(N'[stg].[kmd_fakturalinje_unspsc_match]', 'U') IS NOT NULL
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_stg_kmd_fakturalinje_unspsc_match_lookup'
          AND object_id = OBJECT_ID(N'[stg].[kmd_fakturalinje_unspsc_match]')
    )
        DROP INDEX [IX_stg_kmd_fakturalinje_unspsc_match_lookup]
            ON [stg].[kmd_fakturalinje_unspsc_match];

    DROP TABLE [stg].[kmd_fakturalinje_unspsc_match];
END
GO

CREATE TABLE [stg].[kmd_fakturalinje_unspsc_match] (
    [CleanCode_Kreditor] NVARCHAR(200)  NOT NULL,
    [CleanCode_Faktura]  NVARCHAR(500)  NOT NULL,
    [MatchTier]          TINYINT        NOT NULL,
    [LineAmount_Sum]     DECIMAL(18, 4) NULL,
    [InvoicedQuantity]   DECIMAL(18, 4) NULL,
    [UnitPrice]          DECIMAL(18, 4) NULL,
    [InvoiceTotal]       DECIMAL(18, 4) NULL,
    [DW_EK_UNSPSC]       INT            NOT NULL
) ON [PRIMARY];
GO

CREATE NONCLUSTERED INDEX [IX_stg_kmd_fakturalinje_unspsc_match_lookup]
    ON [stg].[kmd_fakturalinje_unspsc_match] (
        [MatchTier],
        [CleanCode_Kreditor],
        [CleanCode_Faktura],
        [LineAmount_Sum],
        [InvoicedQuantity],
        [UnitPrice],
        [InvoiceTotal]
    );
GO
