-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- SFI_datapoint store grain and consumer indexes.
--
-- Adds a stable datapoint_key and source-grain columns so fhir.SFI_datapoint_store
-- can be refreshed with MERGE semantics instead of TRUNCATE/INSERT.
-- Existing rows are left with NULL datapoint_key; the next refresh replaces them
-- from fhir.SFI_datapoint_build and the filtered unique index enforces new rows.
-- ============================================================================

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'datapoint_key') IS NULL
BEGIN
    ALTER TABLE [fhir].[SFI_datapoint_store]
        ADD [datapoint_key] NVARCHAR(64) NULL;
    PRINT 'Added datapoint_key to fhir.SFI_datapoint_store';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'source_resource_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[SFI_datapoint_store]
        ADD [source_resource_id] NVARCHAR(255) NULL;
    PRINT 'Added source_resource_id to fhir.SFI_datapoint_store';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'source_link_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[SFI_datapoint_store]
        ADD [source_link_id] NVARCHAR(255) NULL;
    PRINT 'Added source_link_id to fhir.SFI_datapoint_store';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'source_item_key') IS NULL
BEGIN
    ALTER TABLE [fhir].[SFI_datapoint_store]
        ADD [source_item_key] NVARCHAR(100) NULL;
    PRINT 'Added source_item_key to fhir.SFI_datapoint_store';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'source_answer_index') IS NULL
BEGIN
    ALTER TABLE [fhir].[SFI_datapoint_store]
        ADD [source_answer_index] NVARCHAR(10) NULL;
    PRINT 'Added source_answer_index to fhir.SFI_datapoint_store';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'source_last_updated') IS NULL
BEGIN
    ALTER TABLE [fhir].[SFI_datapoint_store]
        ADD [source_last_updated] DATETIME2 NULL;
    PRINT 'Added source_last_updated to fhir.SFI_datapoint_store';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
          AND name = N'UX_FHIR_SFI_datapoint_store_DatapointKey'
    )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_FHIR_SFI_datapoint_store_DatapointKey]
        ON [fhir].[SFI_datapoint_store] ([datapoint_key] ASC)
        WHERE [datapoint_key] IS NOT NULL
        ON [PRIMARY];
    PRINT 'Created UX_FHIR_SFI_datapoint_store_DatapointKey';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'occurred_at_dt') IS NOT NULL
    AND EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
          AND name = N'IX_FHIR_SFI_datapoint_store_ElementPatientDate'
    )
BEGIN
    DROP INDEX [IX_FHIR_SFI_datapoint_store_ElementPatientDate]
        ON [fhir].[SFI_datapoint_store];
    PRINT 'Dropped IX_FHIR_SFI_datapoint_store_ElementPatientDate for rebuild';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'occurred_at_dt') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
          AND name = N'IX_FHIR_SFI_datapoint_store_ElementPatientDate'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_ElementPatientDate]
        ON [fhir].[SFI_datapoint_store] (
            [element_code] ASC,
            [occurred_at_dt] ASC,
            [patient_id] ASC
        )
        INCLUDE (
            [occurred_at],
            [element_display],
            [source_model_name],
            [organization_id],
            [value_type],
            [has_value],
            [value_int],
            [value_dec],
            [value_text],
            [value_quantity],
            [value_unit]
        )
        ON [PRIMARY];
    PRINT 'Recreated IX_FHIR_SFI_datapoint_store_ElementPatientDate';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'occurred_at_dt') IS NOT NULL
    AND EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
          AND name = N'IX_FHIR_SFI_datapoint_store_ModelElement'
    )
BEGIN
    DROP INDEX [IX_FHIR_SFI_datapoint_store_ModelElement]
        ON [fhir].[SFI_datapoint_store];
    PRINT 'Dropped IX_FHIR_SFI_datapoint_store_ModelElement for rebuild';
END
GO

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND COL_LENGTH(N'fhir.SFI_datapoint_store', N'occurred_at_dt') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
          AND name = N'IX_FHIR_SFI_datapoint_store_ModelElement'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_ModelElement]
        ON [fhir].[SFI_datapoint_store] (
            [source_model_name] ASC,
            [occurred_at_dt] ASC,
            [element_code] ASC
        )
        INCLUDE (
            [patient_id],
            [organization_id],
            [occurred_at],
            [element_display]
        )
        ON [PRIMARY];
    PRINT 'Recreated IX_FHIR_SFI_datapoint_store_ModelElement';
END
GO
