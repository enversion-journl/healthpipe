-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Bevar lavt udfyldte felter og RIS-kildens historiske organisationsværdier.
-- Felterne findes allerede på FHIR-ressourcerne. Kør denne migration på staging
-- og DSO, opdatér procedure_view og genindlæs SOF/DSO. Organisation-joins alene
-- kan ikke genskabe kildens historiske koder og navne.
-- Ny migration: den tidligere leverancemigration kan allerede være anvendt.

IF COL_LENGTH('fhir.Procedure', 'location_orgcode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [location_orgcode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'used_contrast_code') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [used_contrast_code] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'used_contrast_text') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [used_contrast_text] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'staff_category_code') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [staff_category_code] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'staff_category_text') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [staff_category_text] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_kode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [stamafdeling_kode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_navn') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [stamafdeling_navn] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_orgkode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [stamafdeling_orgkode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_kode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [udfoert_enhed_kode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_navn') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [udfoert_enhed_navn] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_orgkode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [udfoert_enhed_orgkode] NVARCHAR(512) NULL;
GO
