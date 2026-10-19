-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Provenance columns on fhir.Patient
--
-- Patient is assembled by DSA/FHIR/Patient/patient_builder.py from one
-- contribution per source. meta_source is the builder's URI; each source_* flag
-- mirrors one meta.tag and says whether that source contributed to the citizen.
--
-- The columns are NULL until the resource is rebuilt and projected again, so
-- they are complete only after a full Patient rebuild.
--
-- In-place ALTER. No drop, no checkpoint reset. RUNS ON BOTH DATABASES.
-- No-op where the table or the column already exists.
--
-- Base DDL in tables/fhir/01_tables.sql is the fresh-database source of truth.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'meta_source'
   )
    ALTER TABLE [fhir].[Patient] ADD [meta_source] NVARCHAR(255) NULL;
GO

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'source_cpr'
   )
    ALTER TABLE [fhir].[Patient] ADD [source_cpr] BIT NULL;
GO

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'source_ifdb'
   )
    ALTER TABLE [fhir].[Patient] ADD [source_ifdb] BIT NULL;
GO

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'source_haiba'
   )
    ALTER TABLE [fhir].[Patient] ADD [source_haiba] BIT NULL;
GO

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'source_sdsdrg'
   )
    ALTER TABLE [fhir].[Patient] ADD [source_sdsdrg] BIT NULL;
GO
