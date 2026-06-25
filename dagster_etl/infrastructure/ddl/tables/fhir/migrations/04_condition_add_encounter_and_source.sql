-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add encounter_id and source_system to fhir.Condition
--
-- PAS_DIAGNOSE-driven Condition resources carry an encounter reference and
-- multiple source identifier systems (SYSTEM_IFDB_DIAGNOSE vs
-- SYSTEM_IFDB_PAS_DIAGNOSE). Flattening these through condition_view requires
-- the two new columns so downstream DWH consumers can join Condition to
-- Encounter and distinguish between the two IFDB sources.
--
-- Idempotent: safe to re-run on staging and DSO. Each ALTER + CREATE INDEX
-- is gated on a metadata check so re-running a partial run completes the
-- missing pieces without erroring.
--
-- Rollback: there is no automatic down-migration for FHIR DDL changes in
-- this repo (mirroring the existing 01-03 migrations). To revert manually:
--     DROP INDEX [IX_FHIR_Condition_EncounterId] ON [fhir].[Condition];
--     ALTER TABLE [fhir].[Condition] DROP COLUMN [encounter_id];
--     ALTER TABLE [fhir].[Condition] DROP COLUMN [source_system];
-- Run only if a downstream consumer breaks on the new columns.
-- =============================================================================

IF COL_LENGTH('[fhir].[Condition]', 'encounter_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [encounter_id] NVARCHAR(255) NULL;
END
GO

IF COL_LENGTH('[fhir].[Condition]', 'source_system') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [source_system] NVARCHAR(100) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_FHIR_Condition_EncounterId'
      AND object_id = OBJECT_ID(N'[fhir].[Condition]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_EncounterId]
        ON [fhir].[Condition] ([encounter_id] ASC) ON [PRIMARY];
END
GO
