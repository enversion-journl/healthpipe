-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Replace ServiceRequest reasonCode columns with reason_reference_id
-- WHY: RIS referral diagnoses are standalone Condition resources referenced via
-- reasonReference (same join semantics as HENVISNING→PAS-diagnose). Requires
-- re-post of fhir_service_request_* and fhir_ris_ydelse ServiceRequest assets.

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[ServiceRequest]', 'reason_code') IS NOT NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest] DROP COLUMN [reason_code];
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[ServiceRequest]', 'reason_code_system') IS NOT NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest] DROP COLUMN [reason_code_system];
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[ServiceRequest]', 'reason_code_display') IS NOT NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest] DROP COLUMN [reason_code_display];
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[ServiceRequest]', 'reason_reference_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest]
        ADD [reason_reference_id] NVARCHAR(255) NULL;
END
GO
