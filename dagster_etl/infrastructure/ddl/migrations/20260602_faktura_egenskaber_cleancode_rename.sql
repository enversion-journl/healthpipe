-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Rename map.FakturaEgenskaber.CleanCode_Faktura
--            → CleanCode_FakturaEgenskaber
--
-- Complete the Faktura ↔ FakturaEgenskaber rename.
--
-- WHY: The composite business key of the FakturaEgenskaber entity
--   (CleanCode_Kreditor + '_' + CleanCode_Faktura) was still called
--   CleanCode_Faktura on map.FakturaEgenskaber, colliding semantically with the
--   header-level map.Faktura.CleanCode_Faktura. The surrogate key was already
--   renamed to DW_EK_FakturaEgenskaber (20260601_faktura_egenskaber_fk_rename.sql);
--   this renames the matching business key so the name is unambiguous and
--   consistent with the staging outputs (stg.dim_faktura_mysupply,
--   stg.fct_postering_sydax) and fct join keys.
--
-- NOTE: map.Faktura.CleanCode_Faktura (the header key) is intentionally left
--   unchanged. Only map.FakturaEgenskaber is touched here.
--
--   sp_rename on a column carries its constraints/indexes with it — the
--   PK_Map_FakturaEgenskaber clustered key and IX_Map_FakturaEgenskaber_CleanCode
--   nonclustered index reference the column by id, so no drop/recreate needed.
--
-- Idempotent: only renames when the old name exists and the new one does not.
-- ============================================================================

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]')
      AND name = 'CleanCode_Faktura'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]')
      AND name = 'CleanCode_FakturaEgenskaber'
)
BEGIN
    EXEC sp_rename N'map.FakturaEgenskaber.CleanCode_Faktura',
                   N'CleanCode_FakturaEgenskaber', N'COLUMN';
    PRINT 'Renamed map.FakturaEgenskaber.CleanCode_Faktura → CleanCode_FakturaEgenskaber';
END
GO
