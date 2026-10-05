-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: add is_deleted to fhir.Organization and fhir.Location.
--
-- Both tables previously had no soft-delete flag, so a resource hard-deleted on
-- the FHIR server (e.g. the duplicate-Organization cleanup that causes dangling
-- references) lingered in the flat table with no way to retire it -- their
-- fhir_server_reconciliation checks were detection-only (WARN). This adds the
-- flag (mirroring fhir.Encounter.is_deleted) so dqa_fhir_server_cleanup can
-- soft-delete stale rows (entities 'organization' / 'location'). The projections
-- do not populate it; only the cleanup sets it.
-- Idempotent; safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Organization]', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[Organization] ADD [is_deleted] INT NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Location]', 'is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[Location] ADD [is_deleted] INT NULL;
END
GO
