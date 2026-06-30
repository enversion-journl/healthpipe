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
--   _concept:    Code (PK), Display
--   _properties: Code + PropertyCode (PK), Value, Type
--   _hierarchy:  ParentCode + ChildCode (PK)
-- All tables include DW_Timestamp_Insert / DW_Timestamp_Update audit columns.
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
        [Code]                NVARCHAR(100)  NOT NULL,
        [Display]             NVARCHAR(500)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_RekvirentConcept] PRIMARY KEY CLUSTERED ([Code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[RekvirentProperties] (
        [Code]                NVARCHAR(100)  NOT NULL,
        [PropertyCode]        NVARCHAR(100)  NOT NULL,
        [Value]               NVARCHAR(500)  NULL,
        [Type]                NVARCHAR(50)   NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_RekvirentProperties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_RekvirentProperties_PropertyCode]
        ON [codesystem].[RekvirentProperties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[RekvirentHierarchy] (
        [ParentCode]          NVARCHAR(100)  NOT NULL,
        [ChildCode]           NVARCHAR(100)  NOT NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_RekvirentHierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_RekvirentHierarchy_ChildCode]
        ON [codesystem].[RekvirentHierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
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
        [Code]                NVARCHAR(100)  NOT NULL,
        [Display]             NVARCHAR(500)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_LagerConcept] PRIMARY KEY CLUSTERED ([Code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[LagerProperties] (
        [Code]                NVARCHAR(100)  NOT NULL,
        [PropertyCode]        NVARCHAR(100)  NOT NULL,
        [Value]               NVARCHAR(500)  NULL,
        [Type]                NVARCHAR(50)   NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_LagerProperties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_LagerProperties_PropertyCode]
        ON [codesystem].[LagerProperties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[LagerHierarchy] (
        [ParentCode]          NVARCHAR(100)  NOT NULL,
        [ChildCode]           NVARCHAR(100)  NOT NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_LagerHierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_LagerHierarchy_ChildCode]
        ON [codesystem].[LagerHierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
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
        [Code]                NVARCHAR(100)  NOT NULL,
        [Display]             NVARCHAR(500)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_EANConcept] PRIMARY KEY CLUSTERED ([Code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[EANProperties] (
        [Code]                NVARCHAR(100)  NOT NULL,
        [PropertyCode]        NVARCHAR(100)  NOT NULL,
        [Value]               NVARCHAR(500)  NULL,
        [Type]                NVARCHAR(50)   NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_EANProperties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_EANProperties_PropertyCode]
        ON [codesystem].[EANProperties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[EANHierarchy] (
        [ParentCode]          NVARCHAR(100)  NOT NULL,
        [ChildCode]           NVARCHAR(100)  NOT NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_EANHierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_EANHierarchy_ChildCode]
        ON [codesystem].[EANHierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
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
        [Code]                NVARCHAR(100)  NOT NULL,
        [Display]             NVARCHAR(500)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SORConcept] PRIMARY KEY CLUSTERED ([Code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SORProperties] (
        [Code]                NVARCHAR(100)  NOT NULL,
        [PropertyCode]        NVARCHAR(100)  NOT NULL,
        [Value]               NVARCHAR(500)  NULL,
        [Type]                NVARCHAR(50)   NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SORProperties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SORProperties_PropertyCode]
        ON [codesystem].[SORProperties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SORHierarchy] (
        [ParentCode]          NVARCHAR(100)  NOT NULL,
        [ChildCode]           NVARCHAR(100)  NOT NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SORHierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SORHierarchy_ChildCode]
        ON [codesystem].[SORHierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
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
        [Code]                NVARCHAR(100)  NOT NULL,
        [Display]             NVARCHAR(500)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_OkostedConcept] PRIMARY KEY CLUSTERED ([Code])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[OkostedProperties] (
        [Code]                NVARCHAR(100)  NOT NULL,
        [PropertyCode]        NVARCHAR(100)  NOT NULL,
        [Value]               NVARCHAR(500)  NULL,
        [Type]                NVARCHAR(50)   NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_OkostedProperties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_OkostedProperties_PropertyCode]
        ON [codesystem].[OkostedProperties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[OkostedHierarchy] (
        [ParentCode]          NVARCHAR(100)  NOT NULL,
        [ChildCode]           NVARCHAR(100)  NOT NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_OkostedHierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_OkostedHierarchy_ChildCode]
        ON [codesystem].[OkostedHierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
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
        [Code]                NVARCHAR(450)  NOT NULL,
        [Display]             NVARCHAR(500)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SFIConcept] PRIMARY KEY CLUSTERED ([Code])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIConcept_Display]
        ON [codesystem].[SFIConcept] ([Display]) INCLUDE ([Code]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFIProperties] (
        [Code]                NVARCHAR(450)  NOT NULL,
        [PropertyCode]        NVARCHAR(100)  NOT NULL,
        [Value]               NVARCHAR(500)  NULL,
        [Type]                NVARCHAR(50)   NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SFIProperties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIProperties_PropertyCode]
        ON [codesystem].[SFIProperties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFIHierarchy] (
        [ParentCode]          NVARCHAR(450)  NOT NULL,
        [ChildCode]           NVARCHAR(450)  NOT NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SFIHierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIHierarchy_ChildCode]
        ON [codesystem].[SFIHierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
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
        [AliasCode]           NVARCHAR(255)  NOT NULL,
        [AliasDisplay]        NVARCHAR(500)  NOT NULL,
        [AliasType]           NVARCHAR(50)   NOT NULL DEFAULT N'curated_set',
        [Description]         NVARCHAR(MAX)  NULL,
        [ActiveFrom]          DATE           NOT NULL DEFAULT CAST(GETDATE() AS DATE),
        [ActiveTo]            DATE           NULL,
        [Owner]               NVARCHAR(255)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0)   NULL,
        CONSTRAINT [PK_codesystem_SFIAlias] PRIMARY KEY CLUSTERED ([AliasCode])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SFIAliasMember] (
        [AliasCode]           NVARCHAR(255)  NOT NULL,
        [SfiCode]             NVARCHAR(450)  NOT NULL,
        [MemberRole]          NVARCHAR(100)  NULL,
        [SortOrder]           INT            NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0)   NULL,
        CONSTRAINT [PK_codesystem_SFIAliasMember] PRIMARY KEY CLUSTERED ([AliasCode], [SfiCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SFIAliasMember_SfiCode]
        ON [codesystem].[SFIAliasMember] ([SfiCode]) INCLUDE ([AliasCode], [MemberRole], [SortOrder]);
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
        [is_active]            BIT NOT NULL DEFAULT 1,
        [DW_Timestamp_Insert]  DATETIME2(0) NOT NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update]  DATETIME2(0) NULL,
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
        [is_active]           BIT NOT NULL DEFAULT 1,
        [DW_Timestamp_Insert] DATETIME2(0) NOT NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
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
        [Code]                NVARCHAR(100)  NOT NULL,
        [Display]             NVARCHAR(500)  NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SKSConcept] PRIMARY KEY CLUSTERED ([Code])
    );
END
GO

-- Migration: SKS properties are multi-valued (a code can belong to several
-- classifications), so the PK must include [Value]. Drop tables created with
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
      AND COL_NAME(ic.object_id, ic.column_id) = 'Value'
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
    -- [Value] is part of the PK; kept at NVARCHAR(200) to stay within the
    -- 900-byte clustered key limit (values are short classification prefixes
    -- and boolean literals).
    CREATE TABLE [codesystem].[SKSProperties] (
        [Code]                NVARCHAR(100)  NOT NULL,
        [PropertyCode]        NVARCHAR(100)  NOT NULL,
        [Value]               NVARCHAR(200)  NOT NULL,
        [Type]                NVARCHAR(50)   NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SKSProperties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode], [Value])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SKSProperties_PropertyCode]
        ON [codesystem].[SKSProperties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]') AND type = 'U'
)
BEGIN
    CREATE TABLE [codesystem].[SKSHierarchy] (
        [ParentCode]          NVARCHAR(100)  NOT NULL,
        [ChildCode]           NVARCHAR(100)  NOT NULL,
        [DW_Timestamp_Insert] DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update] DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_codesystem_SKSHierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode])
    );

    CREATE NONCLUSTERED INDEX [IX_codesystem_SKSHierarchy_ChildCode]
        ON [codesystem].[SKSHierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
END
GO
