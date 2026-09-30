-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: add [dw_is_deleted] to fhir.Encounter and fhir.EpisodeOfCare.
--
-- Carries the urn:fhir:isDeleted extension (valueInteger 0/1) from the SoF
-- views into the DWH fhir.* tables, so a soft-deleted resource is visible in
-- the warehouse (alongside status='entered-in-error'). Patient already has the
-- column (01_tables.sql); these two were missing.
--
-- Adds the column WITHOUT dropping the table (no data loss). Idempotent and
-- safe to re-run on BOTH the staging and DSO databases (both infrastructure
-- assets run tables/fhir/migrations).
--
-- Table-existence guarded: on a fresh database 01_tables.sql creates the table
-- WITH the column (runs before this migration), so this is a no-op; on an
-- existing database the CREATE is skipped and this adds the column. If the
-- table does not exist at all (e.g. partial deploy), the guard makes this a
-- no-op rather than a failure.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Encounter]', N'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Encounter]', 'dw_is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[Encounter] ADD [dw_is_deleted] INT NULL;
END
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', N'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[EpisodeOfCare]', 'dw_is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[EpisodeOfCare] ADD [dw_is_deleted] INT NULL;
END
GO
