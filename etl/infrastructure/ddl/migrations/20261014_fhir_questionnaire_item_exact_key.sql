-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: compare link_id byte for byte in the fhir.Questionnaire_item key
--
-- The source spells some SFI codes in two cases, e.g.
--   hbdAKbehandlPl.RM_Kardiologi1:edAKbehandlPltxt
--   hbdAKbehandlPl.RM_Kardiologi1:edAKbehandlPlTXT
-- Aidbox keeps them as two Questionnaire items. When both carry the same
-- item_key, the case-insensitive database collation turns them into one primary
-- key and the SOF MERGE fails with a duplicate key.
--
-- The key moves to a persisted computed column, link_id_exact = link_id in
-- Latin1_General_BIN2. link_id itself keeps the database collation, so every
-- existing join on it is unchanged. The SOF MERGE and the DSO sync match on
-- link_id_exact (sof_utils "exact_match_keys", SyncTableConfig.exact_match_keys).
--
-- Starting points this file handles, on staging and DSO alike:
--   * the old primary key on (questionnaire_id, link_id, item_key);
--   * the non-unique clustered index IX_FHIR_QuestionnaireItem_TempKey that
--     replaced the primary key by hand on QA;
--   * a heap with neither.
-- No rows are changed. The migration stops with THROW before it touches the
-- key if two rows are equal byte for byte, rather than pick one.
-- Idempotent: each block is guarded on the state it creates.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', N'U') IS NOT NULL
   AND COL_LENGTH('fhir.Questionnaire_item', 'link_id_exact') IS NULL
BEGIN
    ALTER TABLE [fhir].[Questionnaire_item]
        ADD [link_id_exact] AS ([link_id] COLLATE Latin1_General_BIN2) PERSISTED NOT NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', N'U') IS NOT NULL
   AND COL_LENGTH('fhir.Questionnaire_item', 'link_id_exact') IS NOT NULL
BEGIN
    DECLARE @duplicates INT;
    EXEC sp_executesql
        N'SELECT @n = COUNT(*) FROM (
              SELECT 1 AS x
              FROM [fhir].[Questionnaire_item]
              GROUP BY [questionnaire_id], [link_id_exact], [item_key]
              HAVING COUNT(*) > 1
          ) AS d',
        N'@n INT OUTPUT', @n = @duplicates OUTPUT;
    IF @duplicates > 0
        THROW 50029, N'fhir.Questionnaire_item has rows that are equal byte for byte on (questionnaire_id, link_id, item_key); resolve them before the exact key is created.', 1;
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', N'U') IS NOT NULL
   AND EXISTS (
       SELECT 1
       FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
         AND [name] = N'IX_FHIR_QuestionnaireItem_TempKey'
   )
BEGIN
    DROP INDEX [IX_FHIR_QuestionnaireItem_TempKey] ON [fhir].[Questionnaire_item];
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', N'U') IS NOT NULL
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
         AND c.[name] = N'link_id_exact'
   )
BEGIN
    ALTER TABLE [fhir].[Questionnaire_item] DROP CONSTRAINT [PK_FHIR_Questionnaire_item];
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', N'U') IS NOT NULL
   AND COL_LENGTH('fhir.Questionnaire_item', 'link_id_exact') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.key_constraints
       WHERE parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
         AND [name] = N'PK_FHIR_Questionnaire_item'
   )
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Questionnaire_item]
        ADD CONSTRAINT [PK_FHIR_Questionnaire_item]
        PRIMARY KEY CLUSTERED ([questionnaire_id], [link_id_exact], [item_key]);');
END
GO
