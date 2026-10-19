-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- The update date of the register on fhir.Patient
--
-- The contribution of the register puts the delivery date of the latest CPR
-- delivery that changed the fields of the register on Patient as an extension.
-- cpr_updated mirrors it, and dim.Borger.opdateret_fra_cpr is read from here.
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
         AND COLUMN_NAME = N'cpr_updated'
   )
    ALTER TABLE [fhir].[Patient] ADD [cpr_updated] DATE NULL;
GO
