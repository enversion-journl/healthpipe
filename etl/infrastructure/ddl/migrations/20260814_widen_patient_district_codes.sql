-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Widen person-district codes on fhir.Patient and dim.Borger
--
-- WHY: SoF stages into a temp table typed from the target. IFDB district
-- codes exceed NVARCHAR(8) / VARCHAR(8) (observed max 20 / 32 in
-- arc.ifdb_patient). marital_status_code and citizenship_code stay at 8.
--
-- In-place ALTER. No drop, no checkpoint reset.
-- RUNS ON BOTH DATABASES. No-op where the table or a wide-enough column
-- already exists.
--
-- Base DDL in tables/fhir/01_tables.sql and tables/shared_dimensions/02_dims.sql
-- is the fresh-database source of truth.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir'
         AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'district_code'
         AND CHARACTER_MAXIMUM_LENGTH > 0
         AND CHARACTER_MAXIMUM_LENGTH < 20
   )
    ALTER TABLE [fhir].[Patient] ALTER COLUMN [district_code] NVARCHAR(20) NULL;
GO

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir'
         AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'social_district_code'
         AND CHARACTER_MAXIMUM_LENGTH > 0
         AND CHARACTER_MAXIMUM_LENGTH < 32
   )
    ALTER TABLE [fhir].[Patient] ALTER COLUMN [social_district_code] NVARCHAR(32) NULL;
GO

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'dim'
         AND TABLE_NAME = N'Borger'
         AND COLUMN_NAME = N'person_distriktkode'
         AND CHARACTER_MAXIMUM_LENGTH > 0
         AND CHARACTER_MAXIMUM_LENGTH < 20
   )
    ALTER TABLE [dim].[Borger] ALTER COLUMN [person_distriktkode] VARCHAR(20) NULL;
GO

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'dim'
         AND TABLE_NAME = N'Borger'
         AND COLUMN_NAME = N'person_socialdistriktkode'
         AND CHARACTER_MAXIMUM_LENGTH > 0
         AND CHARACTER_MAXIMUM_LENGTH < 32
   )
    ALTER TABLE [dim].[Borger] ALTER COLUMN [person_socialdistriktkode] VARCHAR(32) NULL;
GO
