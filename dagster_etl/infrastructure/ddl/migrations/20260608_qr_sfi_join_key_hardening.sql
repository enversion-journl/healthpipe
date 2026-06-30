-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- QR -> SFI join-key hardening.
--
-- Makes existing fhir.QuestionnaireResponse tables safe for the QR branch of
-- fhir.SFI_datapoint_build by ensuring the persisted canonical/version columns
-- exist, are backfilled from questionnaire, and are indexed.
--
-- Safe to re-run on staging and DSO.
-- ============================================================================

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
BEGIN
    IF COL_LENGTH('fhir.QuestionnaireResponse', 'questionnaire_canonical') IS NULL
    BEGIN
        ALTER TABLE [fhir].[QuestionnaireResponse]
            ADD [questionnaire_canonical] NVARCHAR(500) NULL;
        PRINT 'Added questionnaire_canonical to fhir.QuestionnaireResponse';
    END;

    IF COL_LENGTH('fhir.QuestionnaireResponse', 'questionnaire_version') IS NULL
    BEGIN
        ALTER TABLE [fhir].[QuestionnaireResponse]
            ADD [questionnaire_version] NVARCHAR(100) NULL;
        PRINT 'Added questionnaire_version to fhir.QuestionnaireResponse';
    END;

    UPDATE qr
    SET
        questionnaire_canonical =
            CASE
                WHEN qr.questionnaire IS NULL THEN NULL
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN LEFT(qr.questionnaire, CHARINDEX(N'|', qr.questionnaire) - 1)
                ELSE qr.questionnaire
            END,
        questionnaire_version =
            CASE
                WHEN qr.questionnaire IS NULL THEN NULL
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN SUBSTRING(
                        qr.questionnaire,
                        CHARINDEX(N'|', qr.questionnaire) + 1,
                        LEN(qr.questionnaire)
                    )
                ELSE NULL
            END
    FROM [fhir].[QuestionnaireResponse] AS qr
    WHERE
        qr.questionnaire IS NOT NULL
        AND (
            qr.questionnaire_canonical IS NULL
            OR qr.questionnaire_canonical <> CASE
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN LEFT(qr.questionnaire, CHARINDEX(N'|', qr.questionnaire) - 1)
                ELSE qr.questionnaire
            END
            OR ISNULL(qr.questionnaire_version, N'') <> ISNULL(CASE
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN SUBSTRING(
                        qr.questionnaire,
                        CHARINDEX(N'|', qr.questionnaire) + 1,
                        LEN(qr.questionnaire)
                    )
                ELSE NULL
            END, N'')
        );

    IF NOT EXISTS (
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
    END;
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
          AND name = N'IX_FHIR_SFI_datapoint_store_SourceResource'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_SourceResource]
        ON [fhir].[SFI_datapoint_store] (
            [source_resource] ASC,
            [source_resource_id] ASC
        )
        INCLUDE ([datapoint_key])
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_SFI_datapoint_store_SourceResource on fhir.SFI_datapoint_store';
END
GO
