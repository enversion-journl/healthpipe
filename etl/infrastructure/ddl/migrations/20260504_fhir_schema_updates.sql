-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- FHIR schema updates: EpisodeOfCare, Organization, Encounter
-- Part of Operations 28004: Tilføjelser til FHIR-ressourcer (KP-case)
--
-- fhir.EpisodeOfCare: add previous_eoc_ref_type_code, discharge_disposition_code
-- fhir.Organization:  add identifier_value_sor, remove identifier_value
-- fhir.Encounter:     add service_provider_id
--
-- Safe to re-run: IF NOT EXISTS / IF EXISTS guards on each column.
-- ============================================================================

-- fhir.EpisodeOfCare: previous_eoc_ref_type_code
IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND name = 'previous_eoc_ref_type_code')
BEGIN
    ALTER TABLE [fhir].[EpisodeOfCare] ADD [previous_eoc_ref_type_code] NVARCHAR(255) NULL;
    PRINT 'Added previous_eoc_ref_type_code to fhir.EpisodeOfCare';
END
GO

-- fhir.EpisodeOfCare: discharge_disposition_code
IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND name = 'discharge_disposition_code')
BEGIN
    ALTER TABLE [fhir].[EpisodeOfCare] ADD [discharge_disposition_code] NVARCHAR(255) NULL;
    PRINT 'Added discharge_disposition_code to fhir.EpisodeOfCare';
END
GO

-- fhir.Organization: identifier_value_sor
IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Organization]') AND name = 'identifier_value_sor')
BEGIN
    ALTER TABLE [fhir].[Organization] ADD [identifier_value_sor] NVARCHAR(255) NULL;
    PRINT 'Added identifier_value_sor to fhir.Organization';
END
GO

-- fhir.Organization: remove identifier_value
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Organization]') AND name = 'identifier_value')
BEGIN
    ALTER TABLE [fhir].[Organization] DROP COLUMN [identifier_value];
    PRINT 'Removed identifier_value from fhir.Organization';
END
GO

-- fhir.Encounter: service_provider_id
IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]') AND name = 'service_provider_id')
BEGIN
    ALTER TABLE [fhir].[Encounter] ADD [service_provider_id] NVARCHAR(255) NULL;
    PRINT 'Added service_provider_id to fhir.Encounter';
END
GO
