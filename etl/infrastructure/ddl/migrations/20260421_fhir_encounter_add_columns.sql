-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add Encounter columns for SOF / sync parity (staging vs DSO)
--
-- Adds class_display, episode_of_care_id, identifier_value if missing.
-- Creates IX_FHIR_Encounter_EpisodeOfCareId if missing (matches 01_tables.sql).
-- Idempotent: safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Encounter]', 'class_display') IS NULL
BEGIN
    ALTER TABLE [fhir].[Encounter]
        ADD [class_display] NVARCHAR(255) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Encounter]', 'episode_of_care_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[Encounter]
        ADD [episode_of_care_id] NVARCHAR(255) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Encounter]', 'identifier_value') IS NULL
BEGIN
    ALTER TABLE [fhir].[Encounter]
        ADD [identifier_value] NVARCHAR(255) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
      AND name = N'IX_FHIR_Encounter_EpisodeOfCareId'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_EpisodeOfCareId]
        ON [fhir].[Encounter] ([episode_of_care_id] ASC) ON [PRIMARY];
END
GO
