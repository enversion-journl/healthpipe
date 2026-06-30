-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Add DW_EK_Postering and CleanCode_Postering columns to fct.Postering
-- Aligns with canonical key pattern (see fct.Fakturalinje)
-- ============================================================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Postering')
BEGIN
    ALTER TABLE [fct].[Postering] ADD [DW_EK_Postering] INT NOT NULL DEFAULT -1;
    ALTER TABLE [fct].[Postering] ADD [CleanCode_Postering] NVARCHAR(100) NULL;
    PRINT 'Added DW_EK_Postering and CleanCode_Postering columns to fct.Postering';
END
GO

-- ============================================================================
-- Backfill: Populate CleanCode_Postering and DW_EK_Postering for existing rows
--
-- CleanCode_Postering = 'SYDAX_' + ref_LedgertransId (matches DSI enrichment logic:
--   Kildesystem is always 'SydAX' → UPPER/TRIM → 'SYDAX')
-- DW_EK_Postering resolved from map.Postering, COALESCE to -1 for unmatched.
-- WHERE clause makes this idempotent — no-op after the next normal ETL cycle.
-- ============================================================================
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
   AND EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Postering')
   AND EXISTS (SELECT 1 FROM [fct].[Postering] WHERE DW_EK_Postering = -1 OR CleanCode_Postering IS NULL)
BEGIN
    UPDATE f
    SET f.CleanCode_Postering = 'SYDAX_' + CAST(f.ref_LedgertransId AS NVARCHAR(50)),
        f.DW_EK_Postering = COALESCE(m.DW_EK_Postering, -1)
    FROM [fct].[Postering] f
    LEFT JOIN [map].[Postering] m
        ON m.CleanCode_Postering = 'SYDAX_' + CAST(f.ref_LedgertransId AS NVARCHAR(50))
    WHERE f.DW_EK_Postering = -1
       OR f.CleanCode_Postering IS NULL;

    PRINT 'Backfilled DW_EK_Postering and CleanCode_Postering on existing fct.Postering rows';
END
GO
