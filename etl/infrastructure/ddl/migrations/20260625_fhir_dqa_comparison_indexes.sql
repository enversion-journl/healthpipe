-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: indexes that speed up DQA comparisons (and business-key lookups).
--
-- The flat FHIR tables already index most FK columns, but NOT the business-key
-- columns (identifier_value / ifdb_patient_id / identifier_value_sor) nor a few
-- FK columns the referential checks scan. On large tables (e.g. ~18M-row
-- fhir.Encounter) that means full clustered scans for:
--   * uniqueness        (GROUP BY identifier_value)
--   * source_reconciliation (DISTINCT identifier_value + key joins)
--   * referential_integrity (NOT EXISTS over the FK column)
--   * ad-hoc lookups like WHERE identifier_value = '...'
--
-- arc.* business keys are already the clustered PKs, so no arc indexes are needed.
-- Kept in sync with the CREATE TABLE blocks in ../01_tables.sql. Idempotent.
-- Note: index builds on large tables run offline (table locked) on non-Enterprise
-- editions; run during a maintenance window.
-- =============================================================================

-- fhir.Encounter --------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Encounter_IdentifierValue'
               AND object_id = OBJECT_ID(N'[fhir].[Encounter]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_IdentifierValue]
        ON [fhir].[Encounter] ([identifier_value] ASC) ON [PRIMARY];
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Encounter_ServiceProviderId'
               AND object_id = OBJECT_ID(N'[fhir].[Encounter]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_ServiceProviderId]
        ON [fhir].[Encounter] ([service_provider_id] ASC) ON [PRIMARY];
GO
-- FK columns for the weekly referential_integrity anti-joins (Encounter ->
-- Patient / EpisodeOfCare). Defined in 01_tables.sql for fresh tables; added
-- here so pre-existing tables (e.g. the 18M-row dev Encounter) get them too --
-- without these the anti-join full-scans the wide clustered table.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Encounter_PatientId'
               AND object_id = OBJECT_ID(N'[fhir].[Encounter]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_PatientId]
        ON [fhir].[Encounter] ([patient_id] ASC) ON [PRIMARY];
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Encounter_EpisodeOfCareId'
               AND object_id = OBJECT_ID(N'[fhir].[Encounter]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_EpisodeOfCareId]
        ON [fhir].[Encounter] ([episode_of_care_id] ASC) ON [PRIMARY];
GO

-- fhir.EpisodeOfCare ----------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_EpisodeOfCare_IdentifierValue'
               AND object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_IdentifierValue]
        ON [fhir].[EpisodeOfCare] ([identifier_value] ASC) ON [PRIMARY];
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_EpisodeOfCare_ManagingOrganizationId'
               AND object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_ManagingOrganizationId]
        ON [fhir].[EpisodeOfCare] ([managing_organization_id] ASC) ON [PRIMARY];
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_EpisodeOfCare_PreviousEpisodeOfCareId'
               AND object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_PreviousEpisodeOfCareId]
        ON [fhir].[EpisodeOfCare] ([previous_episode_of_care_id] ASC) ON [PRIMARY];
GO

-- fhir.Patient ----------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Patient_IfdbPatientId'
               AND object_id = OBJECT_ID(N'[fhir].[Patient]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Patient_IfdbPatientId]
        ON [fhir].[Patient] ([ifdb_patient_id] ASC) ON [PRIMARY];
GO

-- fhir.Organization -----------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Organization_IdentifierValueSor'
               AND object_id = OBJECT_ID(N'[fhir].[Organization]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Organization_IdentifierValueSor]
        ON [fhir].[Organization] ([identifier_value_sor] ASC) ON [PRIMARY];
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Organization_PartOfId'
               AND object_id = OBJECT_ID(N'[fhir].[Organization]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Organization_PartOfId]
        ON [fhir].[Organization] ([part_of_id] ASC) ON [PRIMARY];
GO

-- fhir.Location ---------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Location_IdentifierValueSor'
               AND object_id = OBJECT_ID(N'[fhir].[Location]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Location_IdentifierValueSor]
        ON [fhir].[Location] ([identifier_value_sor] ASC) ON [PRIMARY];
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Location_ManagingOrganizationId'
               AND object_id = OBJECT_ID(N'[fhir].[Location]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Location_ManagingOrganizationId]
        ON [fhir].[Location] ([managing_organization_id] ASC) ON [PRIMARY];
GO

-- fhir.Encounter_Location -----------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_FHIR_Encounter_Location_LocationId'
               AND object_id = OBJECT_ID(N'[fhir].[Encounter_Location]'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_Location_LocationId]
        ON [fhir].[Encounter_Location] ([location_id] ASC) ON [PRIMARY];
GO
