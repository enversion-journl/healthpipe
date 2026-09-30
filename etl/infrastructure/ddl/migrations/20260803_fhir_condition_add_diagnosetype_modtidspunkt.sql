-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add diagnosetype, modtidspunkt and dw_is_deleted to fhir.Condition
--
-- US 26726: diagnosetype (diagnosetype-kvalifikator fra
-- IFDB.PAS_DIAGNOSE.DIAGNOSETYPE; A=aktions-, B=bi-,
-- V=henvisnings-/visitationsdiagnose) was carried on the FHIR resource but
-- never projected to the flat table, so it was invisible to SQL consumers.
-- It is now emitted as the first-class extension
-- http://regionsyddanmark.dk/fhir/StructureDefinition/diagnosetype.
-- modtidspunkt and dw_is_deleted follow the pattern already used on
-- fhir.Patient.
--
-- ROLLOUT NOTE: the diagnosetype and modtidspunkt extensions only exist on
-- RE-POSTED Condition resources (emitted from this change onwards).
-- Re-materialise fhir_diagnose_*/fhir_pas_diagnose_* and then sof_condition_*
-- to populate all three columns for existing rows; until then they read NULL.
-- Idempotent; safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Condition]', 'diagnosetype') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [diagnosetype] NVARCHAR(50) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Condition]', 'modtidspunkt') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [modtidspunkt] DATETIME2 NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Condition]', 'dw_is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [dw_is_deleted] INT NULL;
END
GO
