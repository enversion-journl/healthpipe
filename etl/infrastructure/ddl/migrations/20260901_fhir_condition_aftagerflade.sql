-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- fhir.Condition: episode_of_care_id + lpr3_ignored.
--
-- Aftagerfladen ensrettes paa tvaers af diagnose- og procedurespor: begge
-- kolonner findes allerede paa fhir.Procedure med samme semantik og samme
-- FHIR-stier. tables/-filen baerer kolonnerne for friske databaser; denne fil
-- bringer allerede-deployede paa samme stand. Idempotent; sikker paa baade
-- staging og DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition]', 'episode_of_care_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition] ADD [episode_of_care_id] NVARCHAR(255) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition]', 'lpr3_ignored') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition] ADD [lpr3_ignored] BIT NULL;
END
GO
