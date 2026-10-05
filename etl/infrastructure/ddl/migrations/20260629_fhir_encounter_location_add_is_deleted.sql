-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: add is_deleted to fhir.Encounter_Location.
--
-- Encounter_Location is a finer-grained projection of the SAME Encounter resource
-- as fhir.Encounter, keyed on encounter_id. When an Encounter is deleted on the
-- FHIR server, its Encounter_Location rows are stale too -- but the table had no
-- soft-delete flag. This adds one (mirroring fhir.Encounter.is_deleted) so the
-- dqa_fhir_server_cleanup asset can soft-delete those rows instead of dropping
-- them. The SoF projection does not populate it; only the cleanup sets it.
-- Idempotent; safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Encounter_Location]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Encounter_Location]', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[Encounter_Location] ADD [is_deleted] INT NULL;
END
GO
