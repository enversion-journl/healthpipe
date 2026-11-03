-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ConceptMap: Persistent FHIR ConceptMap mapping tables
-- conceptmap.{source}_to_{target} - source_code/target_code relationships
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[RekvirentToSOR] (
        [source_code]                NVARCHAR(100)  NOT NULL,
        [source_display]             NVARCHAR(500)  NULL,
        [relationship]              NVARCHAR(50)   NOT NULL,
        [target_code]                NVARCHAR(100)  NOT NULL,
        [target_display]             NVARCHAR(500)  NULL,
        [dw_ek_Rekvirent]           INT            NULL,
        [dw_ek_Organisation_target] INT            NULL,
        [dw_timestamp_insert]       DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentToSOR_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)   NOT NULL CONSTRAINT [DF_RekvirentToSOR_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_conceptmap_RekvirentToSOR] PRIMARY KEY CLUSTERED ([source_code], [target_code])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_RekvirentToSOR_target_code]
        ON [conceptmap].[RekvirentToSOR] ([target_code]) INCLUDE ([source_code], [relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[LagerToSOR] (
        [source_code]                NVARCHAR(100)  NOT NULL,
        [source_display]             NVARCHAR(500)  NULL,
        [relationship]              NVARCHAR(50)   NOT NULL,
        [target_code]                NVARCHAR(100)  NOT NULL,
        [target_display]             NVARCHAR(500)  NULL,
        [dw_ek_Lager]               INT            NULL,
        [dw_ek_Organisation_target] INT            NULL,
        [dw_timestamp_insert]       DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerToSOR_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)   NOT NULL CONSTRAINT [DF_LagerToSOR_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_conceptmap_LagerToSOR] PRIMARY KEY CLUSTERED ([source_code], [target_code])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_LagerToSOR_target_code]
        ON [conceptmap].[LagerToSOR] ([target_code]) INCLUDE ([source_code], [relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[EANToSOR] (
        [source_code]                NVARCHAR(100)  NOT NULL,
        [source_display]             NVARCHAR(500)  NULL,
        [relationship]              NVARCHAR(50)   NOT NULL,
        [target_code]                NVARCHAR(100)  NOT NULL,
        [target_display]             NVARCHAR(500)  NULL,
        [dw_ek_EAN]                 INT            NULL,
        [dw_ek_Organisation_target] INT            NULL,
        [dw_timestamp_insert]       DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANToSOR_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)   NOT NULL CONSTRAINT [DF_EANToSOR_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_conceptmap_EANToSOR] PRIMARY KEY CLUSTERED ([source_code], [target_code])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_EANToSOR_target_code]
        ON [conceptmap].[EANToSOR] ([target_code]) INCLUDE ([source_code], [relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND type = 'U'
)
BEGIN
    -- DW_EK_* are resolved AFTER the MERGE (update_conceptmap_dw_ek joins map.Okosted
    -- and ref.Organisation), so they must be nullable at insert time — matching the
    -- other conceptmap.*ToSOR staging tables. The dedup view vw_okosted_to_sor filters
    -- out unresolved (NULL/-1) rows before the DSO sync.
    CREATE TABLE [conceptmap].[OkostedToSOR] (
        [source_code]                 NVARCHAR(120)  NOT NULL,
        [source_display]              NVARCHAR(500)  NULL,
        [relationship]               NVARCHAR(50)   NOT NULL,
        [target_code]                 NVARCHAR(100)  NOT NULL,
        [target_display]              NVARCHAR(500)  NULL,
        [dw_ek_Okosted]             INT            NULL,
        [dw_ek_Organisation_target]  INT            NULL,
        [dw_timestamp_insert]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedToSOR_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_OkostedToSOR_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_conceptmap_OkostedToSOR] PRIMARY KEY CLUSTERED ([source_code], [target_code])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_OkostedToSOR_target_code]
        ON [conceptmap].[OkostedToSOR] ([target_code]) INCLUDE ([source_code], [relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]') AND type = 'U'
)
BEGIN
    -- DW_EK_* are resolved AFTER the MERGE (update_conceptmap_dw_ek joins map.SHAK
    -- and ref.Organisation), so they must be nullable at insert time — matching the
    -- other conceptmap.*ToSOR staging tables. The dedup view vw_shak_to_sor filters
    -- out unresolved (NULL/-1) rows before the DSO sync.
    CREATE TABLE [conceptmap].[SHAKToSOR] (
        [source_code]                 NVARCHAR(100)  NOT NULL,
        [source_display]              NVARCHAR(500)  NULL,
        [relationship]               NVARCHAR(50)   NOT NULL,
        [target_code]                 NVARCHAR(100)  NOT NULL,
        [target_display]              NVARCHAR(500)  NULL,
        [dw_ek_SHAK]                 INT            NULL,
        [dw_ek_Organisation_target]  INT            NULL,
        [dw_timestamp_insert]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKToSOR_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKToSOR_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_conceptmap_SHAKToSOR] PRIMARY KEY CLUSTERED ([source_code], [target_code])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_SHAKToSOR_target_code]
        ON [conceptmap].[SHAKToSOR] ([target_code]) INCLUDE ([source_code], [relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKAlikeToSOR]') AND type = 'U'
)
BEGIN
    -- DW_EK_* resolved AFTER MERGE (update_conceptmap_dw_ek). Nullable at insert.
    CREATE TABLE [conceptmap].[SHAKAlikeToSOR] (
        [source_code]                 NVARCHAR(100)  NOT NULL,
        [source_display]              NVARCHAR(500)  NULL,
        [relationship]               NVARCHAR(50)   NOT NULL,
        [target_code]                 NVARCHAR(100)  NOT NULL,
        [target_display]              NVARCHAR(500)  NULL,
        [dw_ek_SHAKAlike]            INT            NULL,
        [dw_ek_Organisation_target]  INT            NULL,
        [dw_timestamp_insert]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeToSOR_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]        DATETIME2(0)   NOT NULL CONSTRAINT [DF_SHAKAlikeToSOR_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_conceptmap_SHAKAlikeToSOR] PRIMARY KEY CLUSTERED ([source_code], [target_code])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_SHAKAlikeToSOR_target_code]
        ON [conceptmap].[SHAKAlikeToSOR] ([target_code]) INCLUDE ([source_code], [relationship]);
END
GO
