-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: add [healthissue_code] + [healthissue_text] to
-- fhir.EpisodeOfCare.
--
-- Carries the urn:source:ifdb:healthissue_code valueCoding-extension
-- (forløbstype fra IFDB.HEALTHISSUE, opslået via HEALTHISSUE_ID) from the SoF
-- view into the flat table: code = valueCoding.code, text = valueCoding.display.
--
-- Adds the columns WITHOUT dropping the table (no data loss). Idempotent and
-- safe to re-run on BOTH the staging and DSO databases (both infrastructure
-- assets run tables/fhir/migrations).
--
-- Table-existence guarded: on a fresh database 01_tables.sql creates the table
-- WITH the columns (runs before this migration), so this is a no-op; on an
-- existing database the CREATE is skipped and this adds the columns.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', N'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[EpisodeOfCare]', 'healthissue_code') IS NULL
BEGIN
    ALTER TABLE [fhir].[EpisodeOfCare] ADD [healthissue_code] NVARCHAR(255) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', N'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[EpisodeOfCare]', 'healthissue_text') IS NULL
BEGIN
    ALTER TABLE [fhir].[EpisodeOfCare] ADD [healthissue_text] NVARCHAR(128) NULL;
END
GO

-- Widen healthissue_text 64 -> 128: observed max
-- HEALTHISSUE_TEXT length is 57 (PROD 2026-07-10), so NVARCHAR(64) left only a
-- 7-char margin — a new/renamed category > 64 chars would truncate on SoF
-- flatten and then loop forever in the healthissue_code drift check. 128 gives
-- ample headroom without the full source width (512). Defensive/idempotent: only
-- fires if an earlier deploy created the column as NVARCHAR(64). COL_LENGTH is in
-- bytes, so NVARCHAR(128) = 256.
IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', N'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[EpisodeOfCare]', 'healthissue_text') < 256
BEGIN
    ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [healthissue_text] NVARCHAR(128) NULL;
END
GO
