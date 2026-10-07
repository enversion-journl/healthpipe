-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- fhir.Procedure.source_identifier_value.
--
-- 20260824_fhir_procedure_nye_kolonner.sql tilføjede kolonnen i samme fil som
-- knivtid. I miljøer, hvor den fil allerede stod som applied, før
-- source_identifier-blokken kom med, blev knivtid oprettet og filen lukket,
-- mens kildenøglen aldrig kom på. DQA-dækningen joiner på kolonnen og falder
-- derfor. Denne fil er den manglende del, idempotent.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', 'source_identifier_value') IS NULL
BEGIN
    IF COL_LENGTH(N'[fhir].[Procedure]', 'source_key') IS NOT NULL
        EXEC sp_rename N'[fhir].[Procedure].[source_key]', N'source_identifier_value', N'COLUMN';
    ELSE
        ALTER TABLE [fhir].[Procedure] ADD [source_identifier_value] NVARCHAR(255) NULL;
END
GO
