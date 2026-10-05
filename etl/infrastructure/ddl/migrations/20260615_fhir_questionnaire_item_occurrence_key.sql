-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: Make fhir.Questionnaire_item grain occurrence-aware
--
-- SFI linkId is a clinical concept key, not a safe row identity. The same
-- concept can appear more than once in repeated/nested IFDB questionnaire
-- output, and silently deduplicating those rows would drop legitimate data.
-- This migration moves the SQL grain to:
--   questionnaire_id + link_id + item_key
-- where item_key is native Questionnaire.item.id shared with QR.item.id.
--
-- Idempotent / re-runnable. This migration only ensures the item_key column and
-- the occurrence-aware PK exist; it does NOT backfill legacy rows in place.
-- item_key is populated by the SOF Questionnaire reload (native Questionnaire.item.id),
-- and the pipeline truncates + reloads fhir.Questionnaire_item, so there are no
-- pre-existing blank-key rows to migrate. An in-place backfill to link_id was
-- removed: it collapsed repeated occurrences of the same (questionnaire_id,
-- link_id) onto one PK and was the only non-idempotent step. Once the table is
-- occurrence-keyed every block below is a clean no-op.
-- =============================================================================

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND type = 'U'
)
AND COL_LENGTH('fhir.Questionnaire_item', 'item_key') IS NULL
BEGIN
    ALTER TABLE [fhir].[Questionnaire_item]
        ADD [item_key] NVARCHAR(100) NOT NULL
            CONSTRAINT [DF_FHIR_QuestionnaireItem_ItemKey] DEFAULT N'';
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND type = 'U'
)
AND COL_LENGTH('fhir.Questionnaire_item', 'item_key') IS NOT NULL
AND EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND [name] = N'PK_FHIR_Questionnaire_item'
)
AND NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints kc
    INNER JOIN sys.index_columns ic
        ON ic.object_id = kc.parent_object_id
        AND ic.index_id = kc.unique_index_id
    INNER JOIN sys.columns c
        ON c.object_id = ic.object_id
        AND c.column_id = ic.column_id
    WHERE kc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND kc.[name] = N'PK_FHIR_Questionnaire_item'
      AND c.[name] = N'item_key'
)
BEGIN
    ALTER TABLE [fhir].[Questionnaire_item]
        DROP CONSTRAINT [PK_FHIR_Questionnaire_item];
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND type = 'U'
)
AND COL_LENGTH('fhir.Questionnaire_item', 'item_key') IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND [name] = N'PK_FHIR_Questionnaire_item'
)
BEGIN
    ALTER TABLE [fhir].[Questionnaire_item]
        ADD CONSTRAINT [PK_FHIR_Questionnaire_item]
        PRIMARY KEY CLUSTERED ([questionnaire_id], [link_id], [item_key]);
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND [name] = N'IX_FHIR_QuestionnaireItem_UrlVersionLinkId'
)
BEGIN
    DROP INDEX [IX_FHIR_QuestionnaireItem_UrlVersionLinkId]
        ON [fhir].[Questionnaire_item];
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND type = 'U'
)
AND NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
      AND [name] = N'IX_FHIR_QuestionnaireItem_UrlVersionLinkId'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_QuestionnaireItem_UrlVersionLinkId]
        ON [fhir].[Questionnaire_item] (
            [questionnaire_url] ASC,
            [questionnaire_version] ASC,
            [link_id] ASC,
            [item_key] ASC
        ) ON [PRIMARY];
END
GO
