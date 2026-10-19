-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- The time IFDB last updated a PAS diagnosis (PAS_DIAGNOSE.OPDATERET_TID).
--
-- The value is built into the resource's pasDiagnoseSourceMeta extension but
-- was never projected into the flat table. Only PAS diagnoses carry it; the
-- column is NULL for the other Condition sources.
--
-- The value is already on the FHIR resources, so no reposting is needed; only
-- the new ViewDefinition and reprocessing of the projection.

IF COL_LENGTH('fhir.Condition', 'opdateret_tid') IS NULL
    ALTER TABLE [fhir].[Condition] ADD [opdateret_tid] DATETIME2 NULL;
GO

-- The SELECT * views keep stored column metadata and must follow the table at once.
IF OBJECT_ID(N'[vw_fhir].[Condition]', N'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[Condition]';
GO

IF OBJECT_ID(N'[vw_fhir].[Condition_pas_diagnose]', N'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[Condition_pas_diagnose]';
GO
