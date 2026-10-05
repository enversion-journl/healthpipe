-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add reason_code columns to fhir.ServiceRequest
--
-- Exposes ServiceRequest.reasonCode (the RIS_YDELSE.XML <diagnose><kode> referral
-- diagnosis, emitted as an SKS-D coding) in the flat table. The diagnosis lives
-- only in the XML, so without these columns it would not reach the flat
-- projection / DSO.
-- Idempotent; safe to re-run on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[ServiceRequest]', 'reason_code') IS NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest]
        ADD [reason_code] NVARCHAR(255) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[ServiceRequest]', 'reason_code_system') IS NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest]
        ADD [reason_code_system] NVARCHAR(500) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[ServiceRequest]', 'reason_code_display') IS NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest]
        ADD [reason_code_display] NVARCHAR(500) NULL;
END
GO
