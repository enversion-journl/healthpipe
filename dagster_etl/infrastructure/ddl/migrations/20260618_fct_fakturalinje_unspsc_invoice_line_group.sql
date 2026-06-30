-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Re-backfill fct.Fakturalinje.DW_EK_UNSPSC with invoice-line grouped KMD match
--
-- KMD micro-lines aggregated per (DW_SK_Invoice, DW_SK_InvoiceLine).
-- DW_SK_Invoice expanded to FakturaNr + CVR before MySupply join.
-- Match keys:
--   SUM(LineAmount) = LinjeBeloeb
--   SUM(InvoicedQuantity) = AntalEnhederFaktureret
--   UnitPrice = EnhedsPris (BasePrice_EnhedsPris)

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

        ;WITH KmdLines AS (
            SELECT
                sal.[DW_SK_Invoice],
                sal.[DW_SK_InvoiceLine],
                TRY_CONVERT(DECIMAL(18, 4), sal.[UnitPrice]) AS UnitPrice,
                TRY_CONVERT(DECIMAL(18, 4), sal.[LineAmount]) AS LineAmount,
                TRY_CONVERT(DECIMAL(18, 4), sal.[InvoicedQuantity]) AS InvoicedQuantity,
                um.[DW_EK_UNSPSC],
                sal.[DW_ValidDate]
            FROM [arc].[spend_analysis_lines_kmd_uddata] sal
            INNER JOIN [arc].[unspsc_kmd_uddata] ku
                ON ku.[DW_SK_UNSPSC] = sal.[DW_SK_UNSPSC]
                AND ku.[DW_IsCurrent] = 1
            INNER JOIN [map].[UNSPSC] um
                ON um.[CleanCode_UNSPSC] = ku.[UNSPSCClassID]
            WHERE sal.[DW_SK_Invoice] IS NOT NULL
              AND sal.[DW_SK_InvoiceLine] IS NOT NULL
        ),
        KmdAggregated AS (
            SELECT
                DW_SK_Invoice,
                DW_SK_InvoiceLine,
                SUM(LineAmount) AS LineAmount_Sum,
                SUM(InvoicedQuantity) AS InvoicedQuantity_Sum,
                MAX(UnitPrice) AS UnitPrice,
                MAX(DW_EK_UNSPSC) AS DW_EK_UNSPSC,
                MAX(DW_ValidDate) AS DW_ValidDate_Max
            FROM KmdLines
            GROUP BY DW_SK_Invoice, DW_SK_InvoiceLine
        ),
        KmdExpanded AS (
            SELECT
                UPPER(LTRIM(RTRIM(REPLACE(inv.[InvoiceNumber], '' '', '''')))) AS InvoiceNumber_Norm,
                LTRIM(RTRIM(inv.[SupplierCompanyTaxID])) AS SupplierCVR,
                agg.LineAmount_Sum,
                agg.InvoicedQuantity_Sum,
                agg.UnitPrice,
                agg.DW_EK_UNSPSC,
                agg.DW_ValidDate_Max
            FROM KmdAggregated agg
            INNER JOIN [arc].[invoice_kmd_uddata] inv
                ON inv.[DW_SK_Invoice] = agg.[DW_SK_Invoice]
                AND inv.[DW_IsCurrent] = 1
            WHERE inv.[InvoiceNumber] IS NOT NULL
              AND inv.[SupplierCompanyTaxID] IS NOT NULL
        ),
        KmdCandidates AS (
            SELECT
                InvoiceNumber_Norm,
                SupplierCVR,
                UnitPrice,
                LineAmount_Sum,
                InvoicedQuantity_Sum AS InvoicedQuantity,
                DW_EK_UNSPSC,
                ROW_NUMBER() OVER (
                    PARTITION BY InvoiceNumber_Norm, SupplierCVR, LineAmount_Sum, InvoicedQuantity_Sum, UnitPrice
                    ORDER BY DW_ValidDate_Max DESC
                ) AS rn
            FROM KmdExpanded
        )
        SELECT
            InvoiceNumber_Norm,
            SupplierCVR,
            UnitPrice,
            LineAmount_Sum,
            InvoicedQuantity,
            DW_EK_UNSPSC
        INTO #KMD_UNSPSC_Match
        FROM KmdCandidates
        WHERE rn = 1;

        CREATE NONCLUSTERED INDEX IX_KMD_UNSPSC_Match
            ON #KMD_UNSPSC_Match (
                InvoiceNumber_Norm,
                SupplierCVR,
                LineAmount_Sum,
                InvoicedQuantity,
                UnitPrice
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
           AND kmd.[LineAmount_Sum] = TRY_CONVERT(DECIMAL(18, 4), fl.[LinjeBeloeb])
           AND kmd.[InvoicedQuantity] = TRY_CONVERT(DECIMAL(18, 4), fl.[AntalEnhederFaktureret])
           AND kmd.[UnitPrice] = TRY_CONVERT(DECIMAL(18, 4), fl.[EnhedsPris]);

        DROP TABLE IF EXISTS #KMD_UNSPSC_Match;
    ');
    PRINT CONCAT(
        'Re-backfilled fct.Fakturalinje.DW_EK_UNSPSC via invoice-line grouped KMD match: ',
        @@ROWCOUNT,
        ' rows'
    );
END
GO
