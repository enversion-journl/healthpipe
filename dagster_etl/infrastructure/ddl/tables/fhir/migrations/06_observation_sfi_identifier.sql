-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add per-SFI source identifier to fhir.Observation
--
-- SFI Observations carry both the parent AKTIVITETSRESULTAT identifier and the
-- per-element identifier (<OBJEKT_ID>#sfi#<element_nummer>). The per-element
-- value is needed by fhir.SFI_datapoint to distinguish repeated source
-- occurrences without using the random FHIR resource UUID as clinical identity.
--
-- Idempotent: safe to re-run on staging and DSO.
-- =============================================================================

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
      AND type = 'U'
)
AND COL_LENGTH('fhir.Observation', 'ifdb_aktivitet_sfi_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[Observation]
        ADD [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL;
END
GO
