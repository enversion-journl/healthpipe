-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: empty shell for stg.kmd_fakturalinje_unspsc_match
--
-- Staging tables are normally created by ETL SELECT INTO, but this lookup table
-- must exist before fct.Fakturalinje can join it. Populated by
-- stg_kmd_fakturalinje_unspsc_match (TRUNCATE + INSERT).

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = 'kmd_fakturalinje_unspsc_match'
)
BEGIN
    CREATE TABLE [stg].[kmd_fakturalinje_unspsc_match] (
        [InvoiceNumber_Norm] NVARCHAR(100)  NOT NULL,
        [SupplierCVR]        NVARCHAR(50)   NOT NULL,
        [UnitPrice]          DECIMAL(18, 4) NULL,
        [LineAmount_Sum]     DECIMAL(18, 4) NULL,
        [InvoicedQuantity]   DECIMAL(18, 4) NULL,
        [DW_EK_UNSPSC]       INT            NOT NULL
    ) ON [PRIMARY];
END
GO

IF OBJECT_ID(N'[stg].[kmd_fakturalinje_unspsc_match]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_stg_kmd_fakturalinje_unspsc_match_lookup'
          AND object_id = OBJECT_ID(N'[stg].[kmd_fakturalinje_unspsc_match]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_stg_kmd_fakturalinje_unspsc_match_lookup]
        ON [stg].[kmd_fakturalinje_unspsc_match] (
            [InvoiceNumber_Norm],
            [SupplierCVR],
            [LineAmount_Sum],
            [InvoicedQuantity],
            [UnitPrice]
        );
END
GO
