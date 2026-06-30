-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: add header-identity columns to dim.Faktura
--
-- The faktura<->postering bridge (ref.Faktura_To_Postering) ranks the
-- "newest faktura per postering" using the MySupply header load order. Carrying
-- statistic_invoice_header_id + Header_DW_LoadTimestamp onto the (persistent)
-- dimension lets the bridge rank off fct.Faktura -> dim.Faktura instead of the
-- volatile DROP/CREATE stg.dim_faktura_mysupply (which can be empty/stale at
-- bridge build time and silently zero out the crosswalk).
--
-- Idempotent: ADD COLUMN guarded by sys.columns checks.
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
   AND NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'statistic_invoice_header_id'
   )
BEGIN
    ALTER TABLE [dim].[Faktura] ADD [statistic_invoice_header_id] BIGINT NULL;
    PRINT 'Added dim.Faktura.statistic_invoice_header_id';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
   AND NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'Header_DW_LoadTimestamp'
   )
BEGIN
    ALTER TABLE [dim].[Faktura] ADD [Header_DW_LoadTimestamp] DATETIME2 NULL;
    PRINT 'Added dim.Faktura.Header_DW_LoadTimestamp';
END
GO
