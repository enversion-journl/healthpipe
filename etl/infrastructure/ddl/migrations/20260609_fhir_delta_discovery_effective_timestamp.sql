-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- SFI_datapoint delta discovery: sargable change-detection columns + indexes.
--
-- The incremental refresh previously filtered on
--   COALESCE(dw_timestamp_update, dw_timestamp_insert) >= @watermark
-- which is non-sargable and forced full clustered scans of fhir.Observation,
-- fhir.QuestionnaireResponse and fhir.QuestionnaireResponse_item on EVERY
-- run, even when nothing changed.
--
-- This migration adds a PERSISTED computed column dw_timestamp_effective
-- (= ISNULL(dw_timestamp_update, dw_timestamp_insert)) plus a supporting
-- index per table, so changed-resource discovery becomes an index seek.
--
-- Idempotent; safe to re-run.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fhir.Observation
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.Observation', 'dw_timestamp_effective') IS NULL
BEGIN
    ALTER TABLE [fhir].[Observation]
        ADD [dw_timestamp_effective] AS
            ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;
    PRINT 'Added dw_timestamp_effective to fhir.Observation';
END
GO

IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.Observation', 'dw_timestamp_effective') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
          AND name = N'IX_FHIR_Observation_SystemDWEffective'
    )
BEGIN
    -- Discovery filters element_system = 'urn:dk:ifdb:sfi' (equality) and
    -- dw_timestamp_effective >= @watermark (range): composite key seeks both.
    CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_SystemDWEffective]
        ON [fhir].[Observation] (
            [element_system] ASC,
            [dw_timestamp_effective] ASC
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
    AND COL_LENGTH('fhir.QuestionnaireResponse', 'dw_timestamp_effective') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse]
        ADD [dw_timestamp_effective] AS
            ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;
    PRINT 'Added dw_timestamp_effective to fhir.QuestionnaireResponse';
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse', 'dw_timestamp_effective') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
          AND name = N'IX_FHIR_QR_DWEffective'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_QR_DWEffective]
        ON [fhir].[QuestionnaireResponse] ([dw_timestamp_effective] ASC)
        INCLUDE ([qr_id])
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_QR_DWEffective on fhir.QuestionnaireResponse';
END
GO

-- ---------------------------------------------------------------------------
-- fhir.QuestionnaireResponse_item
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'dw_timestamp_effective') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD [dw_timestamp_effective] AS
            ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;
    PRINT 'Added dw_timestamp_effective to fhir.QuestionnaireResponse_item';
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
    AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'dw_timestamp_effective') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
          AND name = N'IX_FHIR_QRI_DWEffective'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_QRI_DWEffective]
        ON [fhir].[QuestionnaireResponse_item] ([dw_timestamp_effective] ASC)
        INCLUDE ([qr_id])
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_QRI_DWEffective on fhir.QuestionnaireResponse_item';
END
GO
