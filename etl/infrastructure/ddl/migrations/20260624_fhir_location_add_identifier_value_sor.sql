-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add identifier_value_sor to fhir.Location
--
-- Location resources are identified by their SOR code (Identifier system
-- https://sundhedsdatastyrelsen.dk/sor, set in Location_SOR_IFDB_dfm.py). location_id
-- is a server-assigned Aidbox UUID, so the flat table previously carried no
-- business key. This exposes Location.identifier.value (SOR) — mirroring
-- fhir.Organization.identifier_value_sor — so the source key is reconcilable.
-- Idempotent; safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Location]', 'identifier_value_sor') IS NULL
BEGIN
    ALTER TABLE [fhir].[Location]
        ADD [identifier_value_sor] NVARCHAR(255) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_FHIR_Location_IdentifierValueSor'
      AND object_id = OBJECT_ID(N'[fhir].[Location]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_Location_IdentifierValueSor]
        ON [fhir].[Location] ([identifier_value_sor] ASC)
        ON [PRIMARY];
END
GO
