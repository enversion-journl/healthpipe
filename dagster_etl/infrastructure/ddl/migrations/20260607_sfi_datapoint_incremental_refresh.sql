-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Support index for SFI_datapoint incremental refresh.
--
-- The incremental refresh strategy uses scoped DELETE + INSERT keyed on
-- (source_resource, source_resource_id). This index makes the scoped DELETE
-- efficient by avoiding a full table scan of fhir.SFI_datapoint_store.
-- ============================================================================

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
        ON [PRIMARY];
    PRINT 'Created IX_FHIR_SFI_datapoint_store_SourceResource for incremental refresh';
END
GO
