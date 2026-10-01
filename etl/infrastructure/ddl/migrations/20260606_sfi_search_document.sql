-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- SFI metadata search document.
--
-- Creates a metadata-driven search surface for SFI concepts. User free text
-- should match this small codesystem table first; large fhir.SFI_datapoint
-- extracts should then filter by the returned element_code values.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[sfi_search_document]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[sfi_search_document] (
        [element_code]         NVARCHAR(450) NOT NULL,
        [element_display]      NVARCHAR(500) NULL,
        [source_model_name]    NVARCHAR(500) NULL,
        [source_element_label] NVARCHAR(500) NULL,
        [sfi_path]             NVARCHAR(1000) NULL,
        [alias_text]           NVARCHAR(MAX) NULL,
        [search_text]          NVARCHAR(MAX) NOT NULL,
        [search_source_hash]   VARBINARY(32) NULL,
        [is_active]            BIT NOT NULL DEFAULT 1,
        [DW_Timestamp_Insert]  DATETIME2(0) NOT NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update]  DATETIME2(0) NULL,
        CONSTRAINT [PK_codesystem_sfi_search_document_v2] PRIMARY KEY CLUSTERED ([element_code])
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
    PRINT 'Created codesystem.sfi_search_document';
END
GO

IF OBJECT_ID(N'[codesystem].[sfi_search_document]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[codesystem].[sfi_search_document]')
          AND name = N'IX_codesystem_sfi_search_document_Model'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_codesystem_sfi_search_document_Model]
        ON [codesystem].[sfi_search_document] ([source_model_name], [element_code])
        INCLUDE ([element_display], [source_element_label], [is_active]);
    PRINT 'Created IX_codesystem_sfi_search_document_Model';
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
    PRINT 'Skipping fulltext for sfi_search_document; fulltext feature check failed: ' + ERROR_MESSAGE();
END CATCH

IF @is_fulltext_enabled = 1
BEGIN
    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = N'ftc_codesystem_sfi_search')
        BEGIN
            EXEC(N'CREATE FULLTEXT CATALOG [ftc_codesystem_sfi_search] AS DEFAULT;');
            PRINT 'Created ftc_codesystem_sfi_search';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Skipping fulltext catalog creation for sfi_search_document; ' + ERROR_MESSAGE();
    END CATCH

    BEGIN TRY
        IF OBJECT_ID(N'[codesystem].[sfi_search_document]', 'U') IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM sys.fulltext_indexes
                WHERE object_id = OBJECT_ID(N'[codesystem].[sfi_search_document]')
            )
        BEGIN
            EXEC(N'CREATE FULLTEXT INDEX ON [codesystem].[sfi_search_document] (
                [search_text] LANGUAGE 1030
            )
            KEY INDEX [PK_codesystem_sfi_search_document_v2]
            ON [ftc_codesystem_sfi_search]
            WITH CHANGE_TRACKING AUTO;');
            PRINT 'Created fulltext index on codesystem.sfi_search_document';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Skipping fulltext index creation for sfi_search_document; ' + ERROR_MESSAGE();
    END CATCH
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[sfi_search_override]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[sfi_search_override] (
        [override_id]         BIGINT IDENTITY(1, 1) NOT NULL,
        [search_phrase]       NVARCHAR(400) NOT NULL,
        [normalized_phrase]   NVARCHAR(400) NOT NULL,
        [element_code]        NVARCHAR(450) NOT NULL,
        [reason]              NVARCHAR(400) NULL,
        [is_active]           BIT NOT NULL DEFAULT 1,
        [DW_Timestamp_Insert] DATETIME2(0) NOT NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
        CONSTRAINT [PK_codesystem_sfi_search_override_v2] PRIMARY KEY CLUSTERED ([override_id])
    ) ON [PRIMARY];
    PRINT 'Created codesystem.sfi_search_override';
END
GO

IF OBJECT_ID(N'[codesystem].[sfi_search_override]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[codesystem].[sfi_search_override]')
          AND name = N'IX_codesystem_sfi_search_override_Phrase'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_codesystem_sfi_search_override_Phrase]
        ON [codesystem].[sfi_search_override] ([normalized_phrase], [element_code])
        INCLUDE ([search_phrase], [reason], [is_active]);
    PRINT 'Created IX_codesystem_sfi_search_override_Phrase';
END
GO
