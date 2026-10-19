-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- The municipality of residence on fhir.Patient
--
-- The Patient builder puts the municipality code of the current address on
-- Patient as an extension. municipality_code mirrors it, also for a person with
-- name and address protection, where the address itself is replaced by the
-- standard text. dim.Borger looks up populationsansvar on the column.
--
-- The column is NULL until the resource is rebuilt and projected again.
-- In-place ALTER; nothing is dropped, and no checkpoint is reset. Runs on both
-- databases and is a no-op where the column exists.
--
-- Base DDL in tables/fhir/01_tables.sql is the source for a new database.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'municipality_code'
   )
    ALTER TABLE [fhir].[Patient] ADD [municipality_code] NVARCHAR(8) NULL;
GO
