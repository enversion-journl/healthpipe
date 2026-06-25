-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- SFI_datapoint delta discovery: sargable change-detection columns + indexes.
--
-- The incremental refresh previously filtered on
--   COALESCE(DW_Timestamp_Update, DW_Timestamp_Insert) >= @watermark
-- which is non-sargable and forced full clustered scans of fhir.Observation,
-- fhir.QuestionnaireResponse and fhir.QuestionnaireResponse_item on EVERY
-- run, even when nothing changed.
--
-- This migration adds a PERSISTED computed column DW_Timestamp_Effective
-- (= ISNULL(DW_Timestamp_Update, DW_Timestamp_Insert)) plus a supporting
-- index per table, so changed-resource discovery becomes an index seek.
--
-- Idempotent; safe to re-run.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fhir.Observation
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.Observation', 'DW_Timestamp_Effective') IS NULL
BEGIN
    ALTER TABLE [fhir].[Observation]
        ADD [DW_Timestamp_Effective] AS
            ISNULL([DW_Timestamp_Update], [DW_Timestamp_Insert]) PERSISTED;
    PRINT 'Added DW_Timestamp_Effective to fhir.Observation';
END
GO

IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.Observation', 'DW_Timestamp_Effective') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
          AND name = N'IX_FHIR_Observation_SystemDWEffective'
    )
BEGIN
    -- Discovery filters element_system = 'urn:dk:ifdb:sfi' (equality) and
    -- DW_Timestamp_Effective >= @watermark (range): composite key seeks both.
    CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_SystemDWEffective]
        ON [fhir].[Observation] (
            [element_system] ASC,
            [DW_Timestamp_Effective] ASC
        )
        INCLUDE ([observation_id])
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_Observation_SystemDWEffective on fhir.Observation';
END
GO

-- ---------------------------------------------------------------------------
-- fhir.QuestionnaireResponse
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse', 'DW_Timestamp_Effective') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse]
        ADD [DW_Timestamp_Effective] AS
            ISNULL([DW_Timestamp_Update], [DW_Timestamp_Insert]) PERSISTED;
    PRINT 'Added DW_Timestamp_Effective to fhir.QuestionnaireResponse';
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse', 'DW_Timestamp_Effective') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
          AND name = N'IX_FHIR_QR_DWEffective'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_QR_DWEffective]
        ON [fhir].[QuestionnaireResponse] ([DW_Timestamp_Effective] ASC)
        INCLUDE ([qr_id])
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_QR_DWEffective on fhir.QuestionnaireResponse';
END
GO

-- ---------------------------------------------------------------------------
-- fhir.QuestionnaireResponse_item
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'DW_Timestamp_Effective') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD [DW_Timestamp_Effective] AS
            ISNULL([DW_Timestamp_Update], [DW_Timestamp_Insert]) PERSISTED;
    PRINT 'Added DW_Timestamp_Effective to fhir.QuestionnaireResponse_item';
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'DW_Timestamp_Effective') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
          AND name = N'IX_FHIR_QRI_DWEffective'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_QRI_DWEffective]
        ON [fhir].[QuestionnaireResponse_item] ([DW_Timestamp_Effective] ASC)
        INCLUDE ([qr_id])
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_QRI_DWEffective on fhir.QuestionnaireResponse_item';
END
GO
