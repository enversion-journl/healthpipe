-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Add DW_EK_UNSPSC FK to fct.Fakturalinje (KMD Option 2)
--
-- Resolves UNSPSC by matching KMD SpendAnalysisLines to MySupply invoice lines via:
--   InvoiceNumber + SupplierCompanyTaxID (CVR) + LineAmount + InvoicedQuantity

-- Step 1: Add DW_EK_UNSPSC column if not present
IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_UNSPSC'
   )
BEGIN
    ALTER TABLE [fct].[Fakturalinje] ADD [DW_EK_UNSPSC] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_UNSPSC to fct.Fakturalinje';
END
GO

-- Step 2: Add arc index for DW_SK_Invoice join (idempotent)
IF OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]')
         AND name = 'IX_arc_spend_analysis_lines_kmd_uddata_dw_sk_invoice'
   )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_spend_analysis_lines_kmd_uddata_dw_sk_invoice]
        ON [arc].[spend_analysis_lines_kmd_uddata] ([DW_SK_Invoice]);
    PRINT 'Created index IX_arc_spend_analysis_lines_kmd_uddata_dw_sk_invoice';
END
GO

-- Step 3: Backfill DW_EK_UNSPSC from KMD match
IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[arc].[invoice_kmd_uddata]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[arc].[unspsc_kmd_uddata]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[Kreditor]', 'U') IS NOT NULL
   AND COL_LENGTH('fct.Fakturalinje', 'DW_EK_UNSPSC') IS NOT NULL
BEGIN
    EXEC(N'
        DROP TABLE IF EXISTS #KMD_UNSPSC_Match;

        ;WITH KmdCandidates AS (
            SELECT
                UPPER(LTRIM(RTRIM(REPLACE(inv.[InvoiceNumber], '' '', '''')))) AS InvoiceNumber_Norm,
                LTRIM(RTRIM(inv.[SupplierCompanyTaxID])) AS SupplierCVR,
                TRY_CONVERT(DECIMAL(18, 4), sal.[LineAmount]) AS LineAmount,
                TRY_CONVERT(DECIMAL(18, 4), sal.[InvoicedQuantity]) AS InvoicedQuantity,
                um.[DW_EK_UNSPSC],
                ROW_NUMBER() OVER (
                    PARTITION BY
                        UPPER(LTRIM(RTRIM(REPLACE(inv.[InvoiceNumber], '' '', '''')))),
                        LTRIM(RTRIM(inv.[SupplierCompanyTaxID])),
                        TRY_CONVERT(DECIMAL(18, 4), sal.[LineAmount]),
                        TRY_CONVERT(DECIMAL(18, 4), sal.[InvoicedQuantity])
                    ORDER BY sal.[DW_ValidDate] DESC
                ) AS rn
            FROM [arc].[spend_analysis_lines_kmd_uddata] sal
            INNER JOIN [arc].[invoice_kmd_uddata] inv
                ON inv.[DW_SK_Invoice] = sal.[DW_SK_Invoice]
                AND inv.[DW_IsCurrent] = 1
            INNER JOIN [arc].[unspsc_kmd_uddata] ku
                ON ku.[DW_SK_UNSPSC] = sal.[DW_SK_UNSPSC]
                AND ku.[DW_IsCurrent] = 1
            INNER JOIN [map].[UNSPSC] um
                ON um.[CleanCode_UNSPSC] = ku.[UNSPSCClassID]
            WHERE inv.[InvoiceNumber] IS NOT NULL
              AND inv.[SupplierCompanyTaxID] IS NOT NULL
        )
        SELECT
            InvoiceNumber_Norm,
            SupplierCVR,
            LineAmount,
            InvoicedQuantity,
            DW_EK_UNSPSC
        INTO #KMD_UNSPSC_Match
        FROM KmdCandidates
        WHERE rn = 1;

        CREATE NONCLUSTERED INDEX IX_KMD_UNSPSC_Match
            ON #KMD_UNSPSC_Match (
                InvoiceNumber_Norm,
                SupplierCVR,
                LineAmount,
                InvoicedQuantity
            );

        UPDATE fl
        SET fl.[DW_EK_UNSPSC] = COALESCE(kmd.[DW_EK_UNSPSC], -1)
        FROM [fct].[Fakturalinje] fl
        INNER JOIN [dim].[Kreditor] dk
            ON dk.[DW_EK_Kreditor] = fl.[DW_EK_Kreditor]
           AND fl.[DW_EK_Kreditor] <> -1
        LEFT JOIN #KMD_UNSPSC_Match kmd
            ON kmd.[InvoiceNumber_Norm] = UPPER(LTRIM(RTRIM(REPLACE(fl.[FakturaNr], '' '', ''''))))
           AND kmd.[SupplierCVR] = LTRIM(RTRIM(dk.[Cvrnr]))
           AND kmd.[LineAmount] = TRY_CONVERT(DECIMAL(18, 4), fl.[LinjeBeloeb])
           AND kmd.[InvoicedQuantity] = TRY_CONVERT(DECIMAL(18, 4), fl.[AntalEnhederFaktureret])
        WHERE fl.[DW_EK_UNSPSC] = -1;

        DROP TABLE IF EXISTS #KMD_UNSPSC_Match;
    ');
    PRINT CONCAT('Backfilled fct.Fakturalinje.DW_EK_UNSPSC: ', @@ROWCOUNT, ' rows');
END
GO

-- Step 4: Create index on DW_EK_UNSPSC if not present
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = 'IX_Fct_Fakturalinje_UNSPSC'
)
AND OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL
AND COL_LENGTH('fct.Fakturalinje', 'DW_EK_UNSPSC') IS NOT NULL
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_UNSPSC]
        ON [fct].[Fakturalinje] ([DW_EK_UNSPSC] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Fakturalinje_UNSPSC';
END
GO
