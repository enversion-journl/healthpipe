-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- EPJI accounts on fhir.Practitioner
--
-- fhir.Practitioner gets a second producer: EPJI's PRACTITIONER table, one row
-- per EPJ account. Three columns are added:
--
--   * identifier_staff: the account's staff code (identifier system
--     urn:source:ifdb:staff), the same value IFDB carries in practitioner_id on
--     fhir.Observation, fhir.Procedure, fhir.CarePlan_Activity and
--     fhir.QuestionnaireResponse. NULL on SilkeborgData rows.
--   * identifier_staff_exact: identifier_staff in Latin1_General_BIN2. EPJI
--     holds some staff codes in two cases as two accounts, and the database's
--     case-insensitive collation would match both. Joins on the staff code go
--     through this column and keep dw_is_deleted = 0.
--   * identifier_regionsid: the region's user identity
--     (http://example.org/BSK/RegionsId). Shared by the accounts of one person,
--     so it groups accounts into people and is not unique.
--
-- IX_FHIR_Practitioner_IdentifierStaffExact is UNIQUE over the live rows that
-- carry a code, so such a join finds at most one account. Its filter names
-- identifier_staff, because SQL Server rejects a computed column in an index
-- filter; the two columns are NULL on the same rows. An existing non-unique
-- index of that name is dropped and recreated as unique. The CREATE fails if
-- two live rows already share a code; this lists them:
--
--   SELECT identifier_staff_exact, COUNT(*) FROM fhir.Practitioner
--   WHERE identifier_staff IS NOT NULL AND dw_is_deleted = 0
--   GROUP BY identifier_staff_exact HAVING COUNT(*) > 1;
--
-- The columns are NULL until the EPJI resources are posted and projected.
-- In-place ALTER; only that index is dropped, and no checkpoint is reset.
-- vw_fhir.Practitioner (SELECT *) is refreshed. Runs on staging and DSO and can
-- be rerun.
--
-- Base DDL in tables/fhir/01_tables.sql is the source for a new database.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Practitioner]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Practitioner', N'identifier_staff') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Practitioner] ADD [identifier_staff] NVARCHAR(64) NULL;');
GO

IF OBJECT_ID(N'[fhir].[Practitioner]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Practitioner', N'identifier_staff') IS NOT NULL
   AND COL_LENGTH(N'fhir.Practitioner', N'identifier_staff_exact') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Practitioner] ADD [identifier_staff_exact]
        AS ([identifier_staff] COLLATE Latin1_General_BIN2) PERSISTED;');
GO

IF OBJECT_ID(N'[fhir].[Practitioner]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Practitioner', N'identifier_regionsid') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Practitioner] ADD [identifier_regionsid] NVARCHAR(64) NULL;');
GO

IF OBJECT_ID(N'[fhir].[Practitioner]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[fhir].[Practitioner]')
         AND name = N'IX_FHIR_Practitioner_IdentifierStaffExact'
         AND is_unique = 0
   )
    EXEC(N'DROP INDEX [IX_FHIR_Practitioner_IdentifierStaffExact] ON [fhir].[Practitioner];');
GO

IF OBJECT_ID(N'[fhir].[Practitioner]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Practitioner', N'identifier_staff_exact') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[fhir].[Practitioner]')
         AND name = N'IX_FHIR_Practitioner_IdentifierStaffExact'
   )
    EXEC(N'CREATE UNIQUE NONCLUSTERED INDEX [IX_FHIR_Practitioner_IdentifierStaffExact]
        ON [fhir].[Practitioner] ([identifier_staff_exact] ASC)
        WHERE [identifier_staff] IS NOT NULL AND [dw_is_deleted] = 0 ON [PRIMARY];');
GO

IF OBJECT_ID(N'[fhir].[Practitioner]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Practitioner', N'identifier_regionsid') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[fhir].[Practitioner]')
         AND name = N'IX_FHIR_Practitioner_IdentifierRegionsId'
   )
    EXEC(N'CREATE NONCLUSTERED INDEX [IX_FHIR_Practitioner_IdentifierRegionsId]
        ON [fhir].[Practitioner] ([identifier_regionsid] ASC) ON [PRIMARY];');
GO

IF OBJECT_ID(N'[vw_fhir].[Practitioner]', 'V') IS NOT NULL
    EXEC sp_refreshview N'[vw_fhir].[Practitioner]';
GO
