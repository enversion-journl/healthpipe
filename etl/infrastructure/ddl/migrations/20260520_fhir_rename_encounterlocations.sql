-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename fhir.EncounterLocations -> fhir.Encounter_Locations
-- Follows Parent_Child underscore convention (Thread #55219).
-- Pattern matches 20260520_fhir_rename_careplanactivity.sql.
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'EncounterLocations' AND SCHEMA_NAME(schema_id) = 'fhir')
   AND NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Encounter_Locations' AND SCHEMA_NAME(schema_id) = 'fhir')
BEGIN
    EXEC sp_rename 'fhir.EncounterLocations', 'Encounter_Locations';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_EncounterLocations_EncounterId' AND object_id = OBJECT_ID('fhir.Encounter_Locations'))
        EXEC sp_rename 'fhir.Encounter_Locations.IX_FHIR_EncounterLocations_EncounterId', 'IX_FHIR_Encounter_Locations_EncounterId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_EncounterLocations_LastUpdated' AND object_id = OBJECT_ID('fhir.Encounter_Locations'))
        EXEC sp_rename 'fhir.Encounter_Locations.IX_FHIR_EncounterLocations_LastUpdated', 'IX_FHIR_Encounter_Locations_LastUpdated', 'INDEX';

    PRINT 'Renamed fhir.EncounterLocations -> fhir.Encounter_Locations (table + indexes)';
END

-- Drop the old view so it can be recreated as vw_fhir.Encounter_Locations
IF OBJECT_ID(N'[vw_fhir].[EncounterLocations]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_fhir].[EncounterLocations];
    PRINT 'Dropped old view vw_fhir.EncounterLocations (will be recreated as vw_fhir.Encounter_Locations)';
END
GO
