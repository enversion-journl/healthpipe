-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Provenance flag for udredningsret on fhir.Patient
--
-- The udredningsret stubs are written through the Patient write point and tagged
-- SDSUDRED in meta.tag, like the other sources. source_sdsudred mirrors that tag.
--
-- The column is NULL until the resource is projected again. In-place ALTER;
-- nothing is dropped, and no checkpoint is reset. Runs on both databases and
-- is a no-op where the column exists.
--
-- Base DDL in tables/fhir/01_tables.sql is the source for a new database.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Patient'
         AND COLUMN_NAME = N'source_sdsudred'
   )
    ALTER TABLE [fhir].[Patient] ADD [source_sdsudred] BIT NULL;
GO
