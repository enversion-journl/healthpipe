-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add source_identifier_value to fhir.Condition
--
-- Exposes Condition.identifier.value in the flat table so consumers can join
-- IFDB source keys (e.g. DIAGNOSE.OBJEKT_ID via urn:source:ifdb:diagnose).
-- Idempotent; safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Condition]', 'source_identifier_value') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [source_identifier_value] NVARCHAR(255) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_FHIR_Condition_SourceIdentifierValue'
      AND object_id = OBJECT_ID(N'[fhir].[Condition]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_SourceIdentifierValue]
        ON [fhir].[Condition] ([source_system] ASC, [source_identifier_value] ASC)
        ON [PRIMARY];
END
GO
