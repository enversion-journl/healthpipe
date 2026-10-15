-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- SHAK-a-like CodeSystem + ConceptMap + dim (EPJI location).
--
-- New objects only (IF NOT EXISTS). Baseline DDL in
--   tables/codesystem/01_codesystem_tables.sql
--   tables/epji_archive/00_archive.sql
--   tables/conceptmap/01_conceptmap_tables.sql
--   tables/conceptmap/dso/01_conceptmap_tables.sql
--   tables/erp/01_maps.sql / 02_dims.sql
--   views/conceptmap/01_vw_conceptmap.sql
-- is the source of truth; this migration creates the same objects on
-- environments that already have an older baseline applied.
-- ============================================================================

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

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKAlikeToSOR]') AND type = 'U'
)
BEGIN
    IF DB_NAME() LIKE N'%staging%'
    BEGIN
        CREATE TABLE [conceptmap].[SHAKAlikeToSOR] (
            [source_code]                 NVARCHAR(100)  NOT NULL,
            [source_display]              NVARCHAR(500)  NULL,
            [relationship]               NVARCHAR(50)   NOT NULL,
            [target_code]                 NVARCHAR(100)  NOT NULL,
            [target_display]              NVARCHAR(500)  NULL,
            [dw_ek_SHAKAlike]            INT            NULL,
            [dw_ek_Organisation_target]  INT            NULL,
            [dw_timestamp_insert]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME(),
            [dw_timestamp_update]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeToSOR_dw_timestamp_update] DEFAULT SYSDATETIME(),
            CONSTRAINT [PK_conceptmap_SHAKAlikeToSOR] PRIMARY KEY CLUSTERED ([source_code], [target_code])
        );

        CREATE NONCLUSTERED INDEX [IX_conceptmap_SHAKAlikeToSOR_target_code]
            ON [conceptmap].[SHAKAlikeToSOR] ([target_code]) INCLUDE ([source_code], [relationship]);
    END
    ELSE
    BEGIN
        CREATE TABLE [conceptmap].[SHAKAlikeToSOR] (
            [source_code]                NVARCHAR(100) NOT NULL,
            [source_display]             NVARCHAR(500) NULL,
            [relationship]              NVARCHAR(50)  NOT NULL,
            [target_code]                NVARCHAR(100) NOT NULL,
            [target_display]             NVARCHAR(500) NULL,
            [dw_ek_SHAKAlike]           INT           NOT NULL,
            [dw_ek_Organisation_target] INT           NOT NULL,
            [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_SHAKAlikeToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME(),
            [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_SHAKAlikeToSOR_dw_timestamp_update] DEFAULT SYSDATETIME(),
            CONSTRAINT [PK_conceptmap_SHAKAlikeToSOR] PRIMARY KEY CLUSTERED ([dw_ek_SHAKAlike])
        );
    END
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[SHAKAlike]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[SHAKAlike] (
        [dw_ek_SHAKAlike] [int] IDENTITY(1, 1) NOT NULL,
        [clean_code_SHAKAlike] [varchar](100) UNIQUE NOT NULL,
        [bk_code] [varchar](50) NOT NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAKAlike_map_mig_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAKAlike_map_mig_dw_timestamp_update] DEFAULT GETDATE(),
        CONSTRAINT [PK_Map_SHAKAlike] PRIMARY KEY CLUSTERED ([dw_ek_SHAKAlike] ASC)
    );

    CREATE INDEX IX_map_SHAKAlike_clean_code_SHAKAlike
        ON [map].[SHAKAlike] ([clean_code_SHAKAlike]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[SHAKAlike]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dim].[SHAKAlike] (
        [dw_ek_SHAKAlike] [int] PRIMARY KEY,
        [code_shakalike] [varchar](50) NOT NULL,
        [name] [nvarchar](500),
        [level] [int],
        [dw_ek_SHAKAlike_parent] [int],
        [parent_code] [varchar](50),
        [active] [bit] CONSTRAINT [DF_SHAKAlike_mig_active] DEFAULT 1,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAKAlike_mig_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_SHAKAlike_mig_dw_timestamp_update] DEFAULT GETDATE()
    );

    CREATE INDEX IX_dim_SHAKAlike_code_shakalike
        ON [dim].[SHAKAlike] ([code_shakalike]);

    CREATE INDEX IX_dim_SHAKAlike_Parent
        ON [dim].[SHAKAlike] ([dw_ek_SHAKAlike_parent])
        WHERE [dw_ek_SHAKAlike_parent] IS NOT NULL;
END
GO

IF OBJECT_ID(N'[dim].[SHAKAlike]', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [dim].[SHAKAlike] WHERE dw_ek_SHAKAlike = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[SHAKAlike] (
            dw_ek_SHAKAlike, code_shakalike, name, [level],
            dw_ek_SHAKAlike_parent, parent_code, active,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (
            -1, ''UNKNOWN'', ''Unknown SHAK-a-like'', NULL,
            NULL, NULL, 1,
            GETDATE(), GETDATE()
        )
    ');
END
GO

-- Scope: STAGING only — arc.* tables have no reason to exist on DSO. Unlike
-- the baseline (tables/epji_archive/00_archive.sql, its own staging-only
-- folder), this migration still runs via the shared migrations/ folder on
-- both DBs, so it needs its own DB_NAME guard to stay a no-op on DSO.
IF DB_NAME() LIKE N'%staging%'
   AND NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[epji_location]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[epji_location] (
        [code]                 NVARCHAR(100) NOT NULL,
        [location_pk]          NVARCHAR(100) NULL,
        [display]              NVARCHAR(500) NULL,
        [sor_code]             NVARCHAR(100) NULL,
        [identifier_system]    NVARCHAR(500) NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_arc_epji_location_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_epji_location] PRIMARY KEY CLUSTERED ([code])
    );
END
GO

-- Staging DSO-sync source. Same semantics as vw_shak_to_sor: keep rows whose
-- SOR target is closed/absent from ref.Organisation (unresolved target key).
--
-- Scope: STAGING only, matching views/conceptmap/01_vw_conceptmap.sql, which
-- dso_infrastructure.py never executes. CREATE VIEW must be the only
-- statement in its batch, so the DB_NAME guard requires dynamic SQL here.
IF DB_NAME() LIKE N'%staging%'
BEGIN
    EXEC(N'
    CREATE OR ALTER VIEW [conceptmap].[vw_shakalike_to_sor] AS
    WITH ranked AS (
        SELECT
            [source_code], [source_display], [relationship], [target_code], [target_display],
            ISNULL([dw_ek_SHAKAlike], -1) AS [dw_ek_SHAKAlike],
            ISNULL([dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_target],
            [dw_timestamp_insert], [dw_timestamp_update],
            ROW_NUMBER() OVER (
                PARTITION BY [dw_ek_SHAKAlike]
                ORDER BY CASE [relationship]
                            WHEN ''equal'' THEN 1 WHEN ''equivalent'' THEN 2
                            WHEN ''narrower'' THEN 3 WHEN ''inexact'' THEN 4 ELSE 99 END,
                         [dw_timestamp_update] DESC
            ) AS _rn
        FROM [conceptmap].[SHAKAlikeToSOR]
        WHERE [dw_ek_SHAKAlike] IS NOT NULL AND [dw_ek_SHAKAlike] <> -1
          AND [relationship] <> ''unmatched''
    )
    SELECT [source_code], [source_display], [relationship], [target_code], [target_display],
           [dw_ek_SHAKAlike], [dw_ek_Organisation_target], [dw_timestamp_insert], [dw_timestamp_update]
    FROM ranked WHERE _rn = 1;
    ');
END
GO
