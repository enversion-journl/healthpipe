-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: ref.kmd_fakturalinje_unspsc_match — move UNSPSC match from stg to ref
--
-- Creates ref lookup with audit/soft-delete columns. Backfills from stg when present.
-- Populated going forward by ref_kmd_fakturalinje_unspsc_match (MERGE + DFM checkpoint).

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[kmd_fakturalinje_unspsc_match]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[kmd_fakturalinje_unspsc_match] (
        [DW_SK_KmdFakturalinjeUnspscMatch] INT IDENTITY(1, 1) NOT NULL,
        [CleanCode_Kreditor] NVARCHAR(200)  NOT NULL,
        [CleanCode_Faktura]  NVARCHAR(500)  NOT NULL,
        [MatchTier]          TINYINT        NOT NULL,
        [LineAmount_Sum]     DECIMAL(18, 4) NULL,
        [InvoicedQuantity]   DECIMAL(18, 4) NULL,
        [UnitPrice]          DECIMAL(18, 4) NULL,
        [InvoiceTotal]       DECIMAL(18, 4) NULL,
        [DW_EK_UNSPSC]       INT            NOT NULL,
        [DW_IsDeleted]       BIT            NOT NULL CONSTRAINT [DF_Ref_KmdFakturalinjeUnspscMatch_DW_IsDeleted] DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
        CONSTRAINT [PK_Ref_KmdFakturalinjeUnspscMatch]
            PRIMARY KEY CLUSTERED ([DW_SK_KmdFakturalinjeUnspscMatch] ASC)
    ) ON [PRIMARY];
END
GO

IF OBJECT_ID(N'[ref].[kmd_fakturalinje_unspsc_match]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[stg].[kmd_fakturalinje_unspsc_match]', 'U') IS NOT NULL
BEGIN
    INSERT INTO [ref].[kmd_fakturalinje_unspsc_match] (
        CleanCode_Kreditor,
        CleanCode_Faktura,
        MatchTier,
        LineAmount_Sum,
        InvoicedQuantity,
        UnitPrice,
        InvoiceTotal,
        DW_EK_UNSPSC,
        DW_IsDeleted,
        DW_Timestamp_Insert
    )
    SELECT
        s.CleanCode_Kreditor,
        s.CleanCode_Faktura,
        s.MatchTier,
        s.LineAmount_Sum,
        s.InvoicedQuantity,
        s.UnitPrice,
        s.InvoiceTotal,
        s.DW_EK_UNSPSC,
        0,
        CAST(SYSDATETIME() AS datetime2(0))
    FROM [stg].[kmd_fakturalinje_unspsc_match] s
    WHERE NOT EXISTS (
        SELECT 1
        FROM [ref].[kmd_fakturalinje_unspsc_match] r
        WHERE r.CleanCode_Kreditor = s.CleanCode_Kreditor
          AND r.CleanCode_Faktura = s.CleanCode_Faktura
          AND r.MatchTier = s.MatchTier
          AND (
              r.LineAmount_Sum = s.LineAmount_Sum
              OR (r.LineAmount_Sum IS NULL AND s.LineAmount_Sum IS NULL)
          )
          AND (
              r.InvoicedQuantity = s.InvoicedQuantity
              OR (r.InvoicedQuantity IS NULL AND s.InvoicedQuantity IS NULL)
          )
          AND (
              r.UnitPrice = s.UnitPrice
              OR (r.UnitPrice IS NULL AND s.UnitPrice IS NULL)
          )
          AND (
              r.InvoiceTotal = s.InvoiceTotal
              OR (r.InvoiceTotal IS NULL AND s.InvoiceTotal IS NULL)
          )
    );
END
GO

IF OBJECT_ID(N'[ref].[kmd_fakturalinje_unspsc_match]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE name = 'UX_ref_kmd_fakturalinje_unspsc_match_keys'
         AND object_id = OBJECT_ID(N'[ref].[kmd_fakturalinje_unspsc_match]')
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_ref_kmd_fakturalinje_unspsc_match_keys]
        ON [ref].[kmd_fakturalinje_unspsc_match] (
            [CleanCode_Kreditor],
            [MatchTier],
            [CleanCode_Faktura],
            [LineAmount_Sum],
            [InvoicedQuantity],
            [UnitPrice],
            [InvoiceTotal]
        )
        WHERE [DW_IsDeleted] = 0;
END
GO

IF OBJECT_ID(N'[ref].[kmd_fakturalinje_unspsc_match]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE name = 'IX_ref_kmd_fakturalinje_unspsc_match_timestamp'
         AND object_id = OBJECT_ID(N'[ref].[kmd_fakturalinje_unspsc_match]')
   )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_kmd_fakturalinje_unspsc_match_timestamp]
        ON [ref].[kmd_fakturalinje_unspsc_match] (
            [DW_Timestamp_Update],
            [DW_Timestamp_Insert]
        )
        WHERE [DW_IsDeleted] = 0;
END
GO

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
