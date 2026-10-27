-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ConceptMap: conceptmap.* tables on DSO (physical, deduped sync targets)
--
-- PK is the DW_EK join column (one row per dimension member). The staging
-- conceptmap.vw_* views guarantee uniqueness via relationship filter + ROW_NUMBER.
-- Distinct shape from the staging-raw conceptmap.* tables (PK source_code,target_code),
-- so this DDL lives in the dso/ subfolder and runs on DSO only.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[RekvirentToSOR] (
        [source_code]                NVARCHAR(100) NOT NULL,
        [source_display]             NVARCHAR(500) NULL,
        [relationship]              NVARCHAR(50)  NOT NULL,
        [target_code]                NVARCHAR(100) NOT NULL,
        [target_display]             NVARCHAR(500) NULL,
        [dw_ek_Rekvirent]           INT           NOT NULL,
        [dw_ek_Organisation_target] INT           NOT NULL,
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_RekvirentToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_RekvirentToSOR_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_conceptmap_RekvirentToSOR] PRIMARY KEY CLUSTERED ([dw_ek_Rekvirent])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[LagerToSOR] (
        [source_code]                NVARCHAR(100) NOT NULL,
        [source_display]             NVARCHAR(500) NULL,
        [relationship]              NVARCHAR(50)  NOT NULL,
        [target_code]                NVARCHAR(100) NOT NULL,
        [target_display]             NVARCHAR(500) NULL,
        [dw_ek_Lager]               INT           NOT NULL,
        [dw_ek_Organisation_target] INT           NOT NULL,
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_LagerToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_LagerToSOR_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_conceptmap_LagerToSOR] PRIMARY KEY CLUSTERED ([dw_ek_Lager])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[EANToSOR] (
        [source_code]                NVARCHAR(100) NOT NULL,
        [source_display]             NVARCHAR(500) NULL,
        [relationship]              NVARCHAR(50)  NOT NULL,
        [target_code]                NVARCHAR(100) NOT NULL,
        [target_display]             NVARCHAR(500) NULL,
        [dw_ek_EAN]                 INT           NOT NULL,
        [dw_ek_Organisation_target] INT           NOT NULL,
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_EANToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_EANToSOR_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_conceptmap_EANToSOR] PRIMARY KEY CLUSTERED ([dw_ek_EAN])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[OkostedToSOR] (
        [source_code]                NVARCHAR(120) NOT NULL,
        [source_display]             NVARCHAR(500) NULL,
        [relationship]              NVARCHAR(50)  NOT NULL,
        [target_code]                NVARCHAR(100) NOT NULL,
        [target_display]             NVARCHAR(500) NULL,
        [dw_ek_Okosted]             INT           NOT NULL,
        [dw_ek_Organisation_target] INT           NOT NULL,
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_OkostedToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_OkostedToSOR_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_conceptmap_OkostedToSOR] PRIMARY KEY CLUSTERED ([dw_ek_Okosted])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[SHAKToSOR] (
        [source_code]                NVARCHAR(100) NOT NULL,
        [source_display]             NVARCHAR(500) NULL,
        [relationship]              NVARCHAR(50)  NOT NULL,
        [target_code]                NVARCHAR(100) NOT NULL,
        [target_display]             NVARCHAR(500) NULL,
        [dw_ek_SHAK]                INT           NOT NULL,
        [dw_ek_Organisation_target] INT           NOT NULL,
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_SHAKToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_SHAKToSOR_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_conceptmap_SHAKToSOR] PRIMARY KEY CLUSTERED ([dw_ek_SHAK])
    );
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKAlikeToSOR]') AND type = 'U'
)
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
GO
