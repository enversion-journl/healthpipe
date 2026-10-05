-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Rename dim.VareudleveringEgenskaber revision columns to requisition (#28151)
--
-- The ILS source column dbo.udlev.rvis is a requisition number ("Rekvisitionsnummer"),
-- not a revision number. Correct the two misnamed DW columns:
--   Revisionsnummer  -> Rekvisitionsnummer
--   RevisionsvarerId -> RekvisitionsvareId   (note: singular "vare", no plural "r")
--
-- No index or constraint references either column (the only index on this table,
-- IX_Dim_VareudleveringEgenskaber_CleanCode, is on CleanCode_*), so a plain sp_rename
-- is sufficient. Applied to BOTH staging and DSO by the *_migrations assets.
-- Idempotent: each rename is guarded on both old and new names.

-- 1. Revisionsnummer -> Rekvisitionsnummer
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'Revisionsnummer'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'Rekvisitionsnummer'
)
BEGIN
    EXEC sp_rename
        N'[dim].[VareudleveringEgenskaber].[Revisionsnummer]',
        N'Rekvisitionsnummer',
        N'COLUMN';
    PRINT 'Renamed dim.VareudleveringEgenskaber.Revisionsnummer -> Rekvisitionsnummer';
END
GO

-- 2. RevisionsvarerId -> RekvisitionsvareId
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'RevisionsvarerId'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'RekvisitionsvareId'
)
BEGIN
    EXEC sp_rename
        N'[dim].[VareudleveringEgenskaber].[RevisionsvarerId]',
        N'RekvisitionsvareId',
        N'COLUMN';
    PRINT 'Renamed dim.VareudleveringEgenskaber.RevisionsvarerId -> RekvisitionsvareId';
END
GO

PRINT '=== Migration 28151 complete ===';
GO
