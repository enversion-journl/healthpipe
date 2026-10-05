-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Phase 3: typed date column, date index, and full-text search on SFI_datapoint_store.
-- Idempotent; safe to re-run on staging and DSO.

IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
   AND COL_LENGTH('fhir.SFI_datapoint_store', 'occurred_at_dt') IS NULL
BEGIN
    ALTER TABLE [fhir].[SFI_datapoint_store]
        ADD [occurred_at_dt] DATETIME2(0) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_FHIR_SFI_datapoint_store_PatientOccurredDt'
      AND object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_PatientOccurredDt]
        ON [fhir].[SFI_datapoint_store] ([patient_id] ASC, [occurred_at_dt] ASC)
        INCLUDE ([element_code], [element_display], [source_model_name], [source_resource])
        ON [PRIMARY];
END
GO

DECLARE @is_fulltext_enabled bit = 0;

BEGIN TRY
    SET @is_fulltext_enabled = CASE
        WHEN CAST(SERVERPROPERTY('IsFullTextInstalled') AS int) = 1
            AND CAST(FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') AS int) = 1
            AND CAST(DATABASEPROPERTYEX(DB_NAME(), 'IsFullTextEnabled') AS int) = 1
        THEN 1 ELSE 0
    END;
END TRY
BEGIN CATCH
    PRINT 'Skipping fulltext for SFI_datapoint_store; fulltext feature check failed: ' + ERROR_MESSAGE();
END CATCH

IF @is_fulltext_enabled = 1
BEGIN
    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = N'ftc_SFI_datapoint')
        BEGIN
            EXEC(N'CREATE FULLTEXT CATALOG [ftc_SFI_datapoint] AS DEFAULT;');
            PRINT 'Created full-text catalog ftc_SFI_datapoint';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Skipping full-text catalog creation for SFI_datapoint_store; ' + ERROR_MESSAGE();
    END CATCH

    BEGIN TRY
        IF NOT EXISTS (
            SELECT 1
            FROM sys.fulltext_indexes
            WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
        )
        BEGIN
            EXEC(N'CREATE FULLTEXT INDEX ON [fhir].[SFI_datapoint_store] (
                [element_display] LANGUAGE 1030,
                [source_element_label] LANGUAGE 1030
            )
            KEY INDEX [PK_FHIR_SFI_datapoint_store]
            ON [ftc_SFI_datapoint]
            WITH CHANGE_TRACKING AUTO;');
            PRINT 'Created full-text index on fhir.SFI_datapoint_store';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Skipping full-text index creation for SFI_datapoint_store; ' + ERROR_MESSAGE();
    END CATCH
END
GO
