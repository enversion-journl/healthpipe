-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- SFI_datapoint performance: supplementary indexes on FHIR + CodeSystem tables.
-- Safe to re-run on staging and DSO.
-- ============================================================================

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse', 'questionnaire_canonical') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse', 'questionnaire_version') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
          AND name = N'IX_FHIR_QR_CanonicalVersion'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_QR_CanonicalVersion]
        ON [fhir].[QuestionnaireResponse] (
            [questionnaire_canonical] ASC,
            [questionnaire_version] ASC
        )
        INCLUDE ([patient_id], [authored], [source_model_name])
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_QR_CanonicalVersion on fhir.QuestionnaireResponse';
END
GO

IF OBJECT_ID(N'[codesystem].[sfi_concept]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[codesystem].[sfi_concept]')
          AND name = N'IX_codesystem_sfi_concept_Display'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_codesystem_sfi_concept_Display]
        ON [codesystem].[sfi_concept] ([Display] ASC)
        INCLUDE ([Code])
        ON [PRIMARY];
    PRINT 'Created IX_codesystem_sfi_concept_Display on codesystem.sfi_concept';
END
GO
