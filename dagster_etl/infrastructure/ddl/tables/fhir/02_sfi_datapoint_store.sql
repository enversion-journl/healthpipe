-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.SFI_datapoint_store
-- Materialized consumer table for fhir.SFI_datapoint (refreshed from SFI_datapoint_build).
-- Deployed on staging and DSO; populated on DSO only via dso_refresh_sfi_datapoint_* assets.
-- ============================================================================

IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[SFI_datapoint_store] (
        [sfi_datapoint_id] BIGINT IDENTITY(1, 1) NOT NULL,
        [datapoint_key] NVARCHAR(64) NOT NULL,
        [fhir_resource_id] NVARCHAR(255) NOT NULL,
        [source_resource_id] NVARCHAR(255) NOT NULL,
        [source_link_id] NVARCHAR(255) NULL,
        [source_item_key] NVARCHAR(100) NULL,
        [source_answer_index] NVARCHAR(10) NULL,
        [source_last_updated] DATETIME2 NULL,
        [ifdb_aktivitet_id] NVARCHAR(255) NULL,
        [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL,
        [patient_id] NVARCHAR(255) NULL,
        [encounter_id] NVARCHAR(255) NULL,
        [organization_id] NVARCHAR(255) NULL,
        [practitioner_id] NVARCHAR(255) NULL,
        [practitioner_display] NVARCHAR(500) NULL,
        [source_resource] NVARCHAR(50) NULL,
        [occurred_at] NVARCHAR(50) NULL,
        [occurred_at_dt] DATETIME2(0) NULL,
        [source_model_name] NVARCHAR(500) NULL,
        [source_model_version] NVARCHAR(100) NULL,
        [source_element_id] NVARCHAR(500) NULL,
        [source_element_name] NVARCHAR(500) NULL,
        [source_element_label] NVARCHAR(500) NULL,
        [sfi_path] NVARCHAR(1000) NULL,
        [source_hierarchy_path] NVARCHAR(1000) NULL,
        [source_group_path] NVARCHAR(1000) NULL,
        [source_repetition_path] NVARCHAR(1000) NULL,
        [klynge_niveau] NVARCHAR(100) NULL,
        [repetition_niveau] NVARCHAR(100) NULL,
        [raekke_type] NVARCHAR(100) NULL,
        [klynge_type] NVARCHAR(100) NULL,
        [klynge_valg_ledetekst] NVARCHAR(MAX) NULL,
        [element_code] NVARCHAR(450) NULL,
        [element_display] NVARCHAR(500) NULL,
        [value_type] NVARCHAR(50) NULL,
        [has_value] BIT NULL,
        [value_int] NVARCHAR(50) NULL,
        [value_dec] NVARCHAR(50) NULL,
        [value_text] NVARCHAR(MAX) NULL,
        [value_datetime] NVARCHAR(50) NULL,
        [value_bool] NVARCHAR(10) NULL,
        [value_quantity] NVARCHAR(100) NULL,
        [value_unit] NVARCHAR(100) NULL,
        [value_coding_code] NVARCHAR(255) NULL,
        [value_coding_display] NVARCHAR(500) NULL,
        [element_nummer] NVARCHAR(50) NULL,
        [klynge_id] NVARCHAR(50) NULL,
        [klynge_id_hierarki] NVARCHAR(500) NULL,
        [klynge_parent_id] NVARCHAR(50) NULL,
        [repetition_id] NVARCHAR(50) NULL,
        [element_kode] NVARCHAR(255) NULL,
        [element_original_kode] NVARCHAR(255) NULL,
        [element_administrativ_enhed_id] NVARCHAR(255) NULL,
        [klynge_navne] NVARCHAR(MAX) NULL,
        [klynge_ledetekster] NVARCHAR(MAX) NULL,
        [klynge_ledetekst] NVARCHAR(500) NULL,
        [repetition_id_hierarki] NVARCHAR(500) NULL,
        [opdateret_tid] NVARCHAR(50) NULL,
        [xml_status] INT NULL,
        [xml_status_text] NVARCHAR(255) NULL,
        [refreshed_at] DATETIME2(0) NOT NULL,
        CONSTRAINT [PK_FHIR_SFI_datapoint_store] PRIMARY KEY CLUSTERED ([sfi_datapoint_id])
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
    WITH (DATA_COMPRESSION = PAGE);

    CREATE UNIQUE NONCLUSTERED INDEX [UX_FHIR_SFI_datapoint_store_DatapointKey]
        ON [fhir].[SFI_datapoint_store] ([datapoint_key] ASC)
        ON [PRIMARY];

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

    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_PatientDate]
        ON [fhir].[SFI_datapoint_store] ([patient_id] ASC, [occurred_at] ASC)
        INCLUDE ([element_code], [element_display], [source_model_name], [source_resource])
        ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_ModelElement]
        ON [fhir].[SFI_datapoint_store] ([source_model_name] ASC, [occurred_at_dt] ASC, [element_code] ASC)
        INCLUDE ([patient_id], [organization_id], [occurred_at], [element_display])
        ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_PatientOccurredDt]
        ON [fhir].[SFI_datapoint_store] ([patient_id] ASC, [occurred_at_dt] ASC)
        INCLUDE ([element_code], [element_display], [source_model_name], [source_resource])
        ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_SourceResource]
        ON [fhir].[SFI_datapoint_store] ([source_resource] ASC, [source_resource_id] ASC)
        INCLUDE ([datapoint_key])
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
