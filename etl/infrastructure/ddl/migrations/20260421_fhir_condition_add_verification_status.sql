-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add verification_status to fhir.Condition
--
-- The column was added to 01_tables.sql baseline but tables created before
-- this change are missing it. Needed for SOF condition_view which extracts
-- verificationStatus from FHIR Condition resources (entered-in-error, confirmed).
-- Idempotent: safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Condition]', 'verification_status') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD [verification_status] NVARCHAR(50) NULL;
END
GO
