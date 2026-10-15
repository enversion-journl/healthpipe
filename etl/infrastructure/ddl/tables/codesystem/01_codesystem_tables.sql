-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- CodeSystem: Persistent FHIR CodeSystem tables
-- codesystem.{name}_concept, codesystem.{name}_properties, codesystem.{name}_hierarchy
--
-- Each codesystem follows the standard triple:
--   _concept:    code (PK), display
--   _properties: code + property_code (PK), value, type
--   _hierarchy:  parent_code + child_code (PK)
-- All tables include dw_timestamp_insert / dw_timestamp_update audit columns.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- rekvirent
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[RekvirentConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_RekvirentConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[RekvirentProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_RekvirentProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_RekvirentProperties_property_code]
        ON [codesystem].[RekvirentProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[RekvirentHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_RekvirentHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_RekvirentHierarchy_child_code]
        ON [codesystem].[RekvirentHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- ---------------------------------------------------------------------------
-- lager
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[LagerConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[LagerConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_LagerConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[LagerProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_LagerProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_LagerProperties_property_code]
        ON [codesystem].[LagerProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[LagerHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_LagerHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_LagerHierarchy_child_code]
        ON [codesystem].[LagerHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- ---------------------------------------------------------------------------
-- ean
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[EANConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[EANConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_EANConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[EANProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_EANProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_EANProperties_property_code]
        ON [codesystem].[EANProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[EANHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_EANHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_EANHierarchy_child_code]
        ON [codesystem].[EANHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- ---------------------------------------------------------------------------
-- sor
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SORConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SORConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SORConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SORConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SORConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SORProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SORProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SORProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SORProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SORProperties_property_code]
        ON [codesystem].[SORProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SORHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SORHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SORHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SORHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SORHierarchy_child_code]
        ON [codesystem].[SORHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- ---------------------------------------------------------------------------
-- shak
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SHAKConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SHAKConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SHAKProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SHAKProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SHAKProperties_property_code]
        ON [codesystem].[SHAKProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SHAKHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SHAKHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SHAKHierarchy_child_code]
        ON [codesystem].[SHAKHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- ---------------------------------------------------------------------------
-- okosted
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[OkostedConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_OkostedConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[OkostedProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_OkostedProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_OkostedProperties_property_code]
        ON [codesystem].[OkostedProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[OkostedHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_OkostedHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_OkostedHierarchy_child_code]
        ON [codesystem].[OkostedHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- ---------------------------------------------------------------------------
-- sfi (Sundhedsfagligt Indhold)
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIConcept]') AND type = 'U'
)
BEGIN
    -- Code is the qualified SFI key which can include hierarchical paths with
    -- separators (e.g. "model._fb_Group/SubGroup#Element"). Width 450 is the
    -- max for a clustered NVARCHAR PK (900-byte index key limit).
    CREATE TABLE [codesystem].[SFIConcept] (
        [code]                NVARCHAR(450)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SFIConcept] PRIMARY KEY CLUSTERED ([code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIConcept_display]
        ON [codesystem].[SFIConcept] ([display]) INCLUDE ([code]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFIProperties] (
        [code]                NVARCHAR(450)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SFIProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIProperties_property_code]
        ON [codesystem].[SFIProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFIHierarchy] (
        [parent_code]          NVARCHAR(450)  NOT NULL,
        [child_code]           NVARCHAR(450)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SFIHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIHierarchy_child_code]
        ON [codesystem].[SFIHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- Optional curation layer on top of source-native SFI names/hierarchy.
-- One alias can group several SFI concepts when users need a maintained
-- analytical set (for example "Glasgow Coma Scale" as a whole), without
-- changing the source CodeSystem concepts themselves.
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAlias]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFIAlias] (
        [alias_code]           NVARCHAR(255)  NOT NULL,
        [alias_display]        NVARCHAR(500)  NOT NULL,
        [alias_type]           NVARCHAR(50)   NOT NULL CONSTRAINT [DF_SFIAlias_alias_type] DEFAULT N'curated_set',
        [description]         NVARCHAR(MAX)  NULL,
        [active_from]          DATE           NOT NULL CONSTRAINT [DF_SFIAlias_active_from] DEFAULT CAST(GETDATE() AS DATE),
        [active_to]            DATE           NULL,
        [owner]               NVARCHAR(255)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIAlias_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIAlias_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_codesystem_SFIAlias] PRIMARY KEY CLUSTERED ([alias_code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFIAliasMember] (
        [alias_code]           NVARCHAR(255)  NOT NULL,
        [sfi_code]             NVARCHAR(450)  NOT NULL,
        [member_role]          NVARCHAR(100)  NULL,
        [sort_order]           INT            NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIAliasMember_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SFIAliasMember_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_codesystem_SFIAliasMember] PRIMARY KEY CLUSTERED ([alias_code], [sfi_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIAliasMember_sfi_code]
        ON [codesystem].[SFIAliasMember] ([sfi_code]) INCLUDE ([alias_code], [member_role], [sort_order]);
END
GO

-- Automatically generated search document per SFI concept.
-- This is intentionally metadata-driven: user free text is matched against
-- displays, source labels, model names, SFI paths and alias text before large
-- fhir.SFI_datapoint extracts are filtered by element_code.
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFISearchDocument] (
        [element_code]         NVARCHAR(450) NOT NULL,
        [element_display]      NVARCHAR(500) NULL,
        [source_model_name]    NVARCHAR(500) NULL,
        [source_element_label] NVARCHAR(500) NULL,
        [sfi_path]             NVARCHAR(1000) NULL,
        [alias_text]           NVARCHAR(MAX) NULL,
        [search_text]          NVARCHAR(MAX) NOT NULL,
        [search_source_hash]   VARBINARY(32) NULL,
        [is_active]            BIT NOT NULL CONSTRAINT [DF_SFISearchDocument_is_active] DEFAULT 1,
        [dw_timestamp_insert]  DATETIME2(0) NOT NULL CONSTRAINT [DF_SFISearchDocument_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]  DATETIME2(0) NOT NULL CONSTRAINT [DF_SFISearchDocument_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_codesystem_SFISearchDocument] PRIMARY KEY CLUSTERED ([element_code])
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFISearchDocument_Model]
        ON [codesystem].[SFISearchDocument] ([source_model_name], [element_code])
        INCLUDE ([element_display], [source_element_label], [is_active]);
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
        END
    END TRY
    BEGIN CATCH
        PRINT 'Skipping fulltext catalog creation for sfi_search_document; ' + ERROR_MESSAGE();
    END CATCH

    BEGIN TRY
        IF OBJECT_ID(N'[codesystem].[SFISearchDocument]', 'U') IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM sys.fulltext_indexes
                WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]')
            )
        BEGIN
            EXEC(N'CREATE FULLTEXT INDEX ON [codesystem].[SFISearchDocument] (
                [search_text] LANGUAGE 1030
            )
            KEY INDEX [PK_codesystem_SFISearchDocument]
            ON [ftc_codesystem_sfi_search]
            WITH CHANGE_TRACKING AUTO;');
        END
    END TRY
    BEGIN CATCH
        PRINT 'Skipping fulltext index creation for sfi_search_document; ' + ERROR_MESSAGE();
    END CATCH
END
GO

-- Small feedback layer for real observed misses (abbreviations, spelling
-- variants, clinical everyday language). This is not the primary search source.
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFISearchOverride] (
        [override_id]         BIGINT IDENTITY(1, 1) NOT NULL,
        [search_phrase]       NVARCHAR(400) NOT NULL,
        [normalized_phrase]   NVARCHAR(400) NOT NULL,
        [element_code]        NVARCHAR(450) NOT NULL,
        [reason]              NVARCHAR(400) NULL,
        [is_active]           BIT NOT NULL CONSTRAINT [DF_SFISearchOverride_is_active] DEFAULT 1,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_SFISearchOverride_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_SFISearchOverride_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_codesystem_SFISearchOverride] PRIMARY KEY CLUSTERED ([override_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFISearchOverride_Phrase]
        ON [codesystem].[SFISearchOverride] ([normalized_phrase], [element_code])
        INCLUDE ([search_phrase], [reason], [is_active]);
END
GO

-- ---------------------------------------------------------------------------
-- sks
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SKSConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SKSConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SKSConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SKSConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SKSConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

-- Migration: SKS properties are multi-valued (a code can belong to several
-- classifications), so the PK must include [value]. Drop tables created with
-- the old PK — content is fully re-derived from ext.sks_register by cs_sks.
IF EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]') AND type = 'U'
)
AND NOT EXISTS (
    SELECT * FROM sys.index_columns ic
    INNER JOIN sys.indexes i
        ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    WHERE i.object_id = OBJECT_ID(N'[codesystem].[SKSProperties]')
      AND i.is_primary_key = 1
      AND COL_NAME(ic.object_id, ic.column_id) = 'value'
)
BEGIN
    DROP TABLE [codesystem].[SKSProperties];
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]') AND type = 'U'
)
BEGIN
    -- [value] is part of the PK; kept at NVARCHAR(200) to stay within the
    -- 900-byte clustered key limit (values are short classification prefixes
    -- and boolean literals).
    CREATE TABLE [codesystem].[SKSProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(200)  NOT NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SKSProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SKSProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SKSProperties] PRIMARY KEY CLUSTERED ([code], [property_code], [value])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SKSProperties_property_code]
        ON [codesystem].[SKSProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SKSHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SKSHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SKSHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SKSHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SKSHierarchy_child_code]
        ON [codesystem].[SKSHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO

-- ---------------------------------------------------------------------------
-- shakalike (EPJI location.external_id)
-- ---------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKAlikeConcept]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SHAKAlikeConcept] (
        [code]                NVARCHAR(100)  NOT NULL,
        [display]             NVARCHAR(500)  NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeConcept_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeConcept_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SHAKAlikeConcept] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKAlikeProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SHAKAlikeProperties] (
        [code]                NVARCHAR(100)  NOT NULL,
        [property_code]        NVARCHAR(100)  NOT NULL,
        [value]               NVARCHAR(500)  NULL,
        [type]                NVARCHAR(50)   NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeProperties_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeProperties_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SHAKAlikeProperties] PRIMARY KEY CLUSTERED ([code], [property_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SHAKAlikeProperties_property_code]
        ON [codesystem].[SHAKAlikeProperties] ([property_code]) INCLUDE ([code], [value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKAlikeHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SHAKAlikeHierarchy] (
        [parent_code]          NVARCHAR(100)  NOT NULL,
        [child_code]           NVARCHAR(100)  NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_codesystem_SHAKAlikeHierarchy] PRIMARY KEY CLUSTERED ([parent_code], [child_code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SHAKAlikeHierarchy_child_code]
        ON [codesystem].[SHAKAlikeHierarchy] ([child_code]) INCLUDE ([parent_code]);
END
GO
