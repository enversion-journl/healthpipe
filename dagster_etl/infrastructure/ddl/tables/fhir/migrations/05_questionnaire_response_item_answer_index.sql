-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Add item_key + answer_index to fhir.QuestionnaireResponse_item
--
-- The SOF questionnaire_response_item_view emits one row per
-- QuestionnaireResponse item answer. Existing target tables created before the
-- SFI QuestionnaireResponse item work may miss item_key and/or answer_index.
--
-- Idempotent: safe to re-run on staging and DSO. Each step is guarded so a
-- partial prior run can be completed by running staging_infra_fhir again.
-- =============================================================================

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
      AND type = 'U'
)
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'item_key') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD [item_key] NVARCHAR(100) NOT NULL
            CONSTRAINT [DF_FHIR_QRItem_ItemKey] DEFAULT N'';
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
      AND type = 'U'
)
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'answer_index') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD [answer_index] NVARCHAR(10) NOT NULL
            CONSTRAINT [DF_FHIR_QRItem_AnswerIndex] DEFAULT N'';
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
      AND type = 'U'
)
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'item_key') IS NOT NULL
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'answer_index') IS NOT NULL
AND EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
      AND [name] = N'PK_FHIR_QuestionnaireResponse_item'
)
AND (
    NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints kc
        INNER JOIN sys.index_columns ic
            ON ic.object_id = kc.parent_object_id
            AND ic.index_id = kc.unique_index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
            AND c.column_id = ic.column_id
        WHERE kc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
          AND kc.[name] = N'PK_FHIR_QuestionnaireResponse_item'
          AND c.[name] = N'item_key'
    )
    OR NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints kc
        INNER JOIN sys.index_columns ic
            ON ic.object_id = kc.parent_object_id
            AND ic.index_id = kc.unique_index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
            AND c.column_id = ic.column_id
        WHERE kc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
          AND kc.[name] = N'PK_FHIR_QuestionnaireResponse_item'
          AND c.[name] = N'answer_index'
    )
)
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        DROP CONSTRAINT [PK_FHIR_QuestionnaireResponse_item];

    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD CONSTRAINT [PK_FHIR_QuestionnaireResponse_item]
        PRIMARY KEY CLUSTERED ([qr_id], [link_id], [item_key], [answer_index]);
END
GO
