-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename fhir.EncounterLocations -> fhir.Encounter_Location (singular)
-- Aligns with FHIR resource naming: each row is one Location reference.
-- Handles both pre- and post-20260520 rename state (EncounterLocations or
-- Encounter_Locations -> Encounter_Location).
-- ============================================================================

-- Case 1: original name still present
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'EncounterLocations' AND SCHEMA_NAME(schema_id) = 'fhir')
   AND NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Encounter_Location' AND SCHEMA_NAME(schema_id) = 'fhir')
BEGIN
    EXEC sp_rename 'fhir.EncounterLocations', 'Encounter_Location';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_EncounterLocations_EncounterId' AND object_id = OBJECT_ID('fhir.Encounter_Location'))
        EXEC sp_rename 'fhir.Encounter_Location.IX_FHIR_EncounterLocations_EncounterId', 'IX_FHIR_Encounter_Location_EncounterId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_EncounterLocations_LastUpdated' AND object_id = OBJECT_ID('fhir.Encounter_Location'))
        EXEC sp_rename 'fhir.Encounter_Location.IX_FHIR_EncounterLocations_LastUpdated', 'IX_FHIR_Encounter_Location_LastUpdated', 'INDEX';

    PRINT 'Renamed fhir.EncounterLocations -> fhir.Encounter_Location (table + indexes)';
END

-- Case 2: intermediate name from 20260520 migration
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Encounter_Locations' AND SCHEMA_NAME(schema_id) = 'fhir')
   AND NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Encounter_Location' AND SCHEMA_NAME(schema_id) = 'fhir')
BEGIN
    EXEC sp_rename 'fhir.Encounter_Locations', 'Encounter_Location';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_Encounter_Locations_EncounterId' AND object_id = OBJECT_ID('fhir.Encounter_Location'))
        EXEC sp_rename 'fhir.Encounter_Location.IX_FHIR_Encounter_Locations_EncounterId', 'IX_FHIR_Encounter_Location_EncounterId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_Encounter_Locations_LastUpdated' AND object_id = OBJECT_ID('fhir.Encounter_Location'))
        EXEC sp_rename 'fhir.Encounter_Location.IX_FHIR_Encounter_Locations_LastUpdated', 'IX_FHIR_Encounter_Location_LastUpdated', 'INDEX';

    PRINT 'Renamed fhir.Encounter_Locations -> fhir.Encounter_Location (table + indexes)';
END

-- Drop old views so they can be recreated with the new name
IF OBJECT_ID(N'[vw_fhir].[EncounterLocations]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_fhir].[EncounterLocations];
    PRINT 'Dropped old view vw_fhir.EncounterLocations';
END

IF OBJECT_ID(N'[vw_fhir].[Encounter_Locations]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_fhir].[Encounter_Locations];
    PRINT 'Dropped intermediate view vw_fhir.Encounter_Locations';
END
GO
