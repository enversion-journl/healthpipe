-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Add IsCreditnote BIT to fct.IndkoebsanalyseLinje (task 28292)
-- Idempotent: only adds column if it does not already exist.
-- Backfill from arc.invoice_kmd_uddata via arc.spend_analysis_lines_kmd_uddata.

-- Table is created by staging_infra_erp (03_facts.sql) with IsCreditnote on fresh installs.
-- This migration only upgrades databases where the table predates the column.
IF OBJECT_ID(N'[fct].[IndkoebsanalyseLinje]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fct].[IndkoebsanalyseLinje]')
         AND name = 'IsCreditnote'
   )
BEGIN
    ALTER TABLE [fct].[IndkoebsanalyseLinje]
        ADD [IsCreditnote] BIT NOT NULL DEFAULT 0;
    PRINT 'Added IsCreditnote to fct.IndkoebsanalyseLinje';
END
GO

-- Backfill IsCreditnote from arc.invoice_kmd_uddata (DW_IsCurrent=1)
IF OBJECT_ID(N'[fct].[IndkoebsanalyseLinje]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'invoice_kmd_uddata'
   )
BEGIN
    UPDATE f
    SET f.[IsCreditnote] = CAST(COALESCE(inv.[IsCreditnote], 0) AS BIT)
    FROM [fct].[IndkoebsanalyseLinje] f
    INNER JOIN [arc].[spend_analysis_lines_kmd_uddata] sal
        ON sal.[DW_EK_SpendAnalysisLine] = CAST(f.[bk_SpendAnalysisLine] AS BIGINT)
    INNER JOIN [arc].[invoice_kmd_uddata] inv
        ON inv.[DW_SK_Invoice] = sal.[DW_SK_Invoice]
        AND inv.[DW_IsCurrent] = 1
    WHERE f.[IsCreditnote] = 0
      AND inv.[IsCreditnote] = 1;

    PRINT 'Backfilled IsCreditnote on fct.IndkoebsanalyseLinje';
END
GO
