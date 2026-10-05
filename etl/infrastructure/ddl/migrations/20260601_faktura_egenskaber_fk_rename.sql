-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Rename DW_EK_Faktura → DW_EK_FakturaEgenskaber in the
-- FakturaEgenskaber keyspace (map, dim, fct.Postering) and ADD the new FK
-- column to fct.Faktura + fct.Fakturalinje.
--
-- Fix Faktura ↔ FakturaEgenskaber relationship modelling.
--
-- Idempotent: safe to re-run (checks new name does NOT already exist before
-- calling sp_rename; ADD COLUMN guarded by sys.columns check).
-- ============================================================================

-- ---------- 1. map.FakturaEgenskaber: rename IDENTITY column ----------------

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]')
      AND name = 'DW_EK_Faktura'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]')
      AND name = 'DW_EK_FakturaEgenskaber'
)
BEGIN
    EXEC sp_rename N'map.FakturaEgenskaber.DW_EK_Faktura',
                   N'DW_EK_FakturaEgenskaber', N'COLUMN';
    PRINT 'Renamed map.FakturaEgenskaber.DW_EK_Faktura → DW_EK_FakturaEgenskaber';
END
GO

-- ---------- 2. dim.FakturaEgenskaber: rename PK column ----------------------

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]')
      AND name = 'DW_EK_Faktura'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]')
      AND name = 'DW_EK_FakturaEgenskaber'
)
BEGIN
    EXEC sp_rename N'dim.FakturaEgenskaber.DW_EK_Faktura',
                   N'DW_EK_FakturaEgenskaber', N'COLUMN';
    PRINT 'Renamed dim.FakturaEgenskaber.DW_EK_Faktura → DW_EK_FakturaEgenskaber';
END
GO

-- ---------- 3. fct.Postering: rename FK column ------------------------------

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
      AND name = 'DW_EK_Faktura'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
      AND name = 'DW_EK_FakturaEgenskaber'
)
BEGIN
    EXEC sp_rename N'fct.Postering.DW_EK_Faktura',
                   N'DW_EK_FakturaEgenskaber', N'COLUMN';
    PRINT 'Renamed fct.Postering.DW_EK_Faktura → DW_EK_FakturaEgenskaber';
END
GO

-- ---------- 4. fct.Faktura: ADD DW_EK_FakturaEgenskaber --------------------

IF EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND type = 'U'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
      AND name = 'DW_EK_FakturaEgenskaber'
)
BEGIN
    ALTER TABLE [fct].[Faktura]
        ADD [DW_EK_FakturaEgenskaber] INT NOT NULL DEFAULT -1;
    PRINT 'Added fct.Faktura.DW_EK_FakturaEgenskaber (INT NOT NULL DEFAULT -1)';
END
GO

-- ---------- 5. fct.Fakturalinje: ADD DW_EK_FakturaEgenskaber ----------------

IF EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = 'DW_EK_FakturaEgenskaber'
)
BEGIN
    ALTER TABLE [fct].[Fakturalinje]
        ADD [DW_EK_FakturaEgenskaber] INT NOT NULL DEFAULT -1;
    PRINT 'Added fct.Fakturalinje.DW_EK_FakturaEgenskaber (INT NOT NULL DEFAULT -1)';
END
GO
