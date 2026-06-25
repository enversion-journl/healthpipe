-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ConceptMap: Persistent FHIR ConceptMap mapping tables
-- conceptmap.{source}_to_{target} - SourceCode/TargetCode relationships
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[RekvirentToSOR] (
        [SourceCode]                NVARCHAR(100)  NOT NULL,
        [SourceDisplay]             NVARCHAR(500)  NULL,
        [Relationship]              NVARCHAR(50)   NOT NULL,
        [TargetCode]                NVARCHAR(100)  NOT NULL,
        [TargetDisplay]             NVARCHAR(500)  NULL,
        [DW_EK_Rekvirent]           INT            NOT NULL,
        [DW_EK_Organisation_Target] INT            NOT NULL,
        [DW_Timestamp_Insert]       DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]       DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_conceptmap_RekvirentToSOR] PRIMARY KEY CLUSTERED ([SourceCode], [TargetCode])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_RekvirentToSOR_TargetCode]
        ON [conceptmap].[RekvirentToSOR] ([TargetCode]) INCLUDE ([SourceCode], [Relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[LagerToSOR] (
        [SourceCode]                NVARCHAR(100)  NOT NULL,
        [SourceDisplay]             NVARCHAR(500)  NULL,
        [Relationship]              NVARCHAR(50)   NOT NULL,
        [TargetCode]                NVARCHAR(100)  NOT NULL,
        [TargetDisplay]             NVARCHAR(500)  NULL,
        [DW_EK_Lager]               INT            NOT NULL,
        [DW_EK_Organisation_Target] INT            NOT NULL,
        [DW_Timestamp_Insert]       DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]       DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_conceptmap_LagerToSOR] PRIMARY KEY CLUSTERED ([SourceCode], [TargetCode])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_LagerToSOR_TargetCode]
        ON [conceptmap].[LagerToSOR] ([TargetCode]) INCLUDE ([SourceCode], [Relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[EANToSOR] (
        [SourceCode]                NVARCHAR(100)  NOT NULL,
        [SourceDisplay]             NVARCHAR(500)  NULL,
        [Relationship]              NVARCHAR(50)   NOT NULL,
        [TargetCode]                NVARCHAR(100)  NOT NULL,
        [TargetDisplay]             NVARCHAR(500)  NULL,
        [DW_EK_EAN]                 INT            NOT NULL,
        [DW_EK_Organisation_Target] INT            NOT NULL,
        [DW_Timestamp_Insert]       DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]       DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_conceptmap_EANToSOR] PRIMARY KEY CLUSTERED ([SourceCode], [TargetCode])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_EANToSOR_TargetCode]
        ON [conceptmap].[EANToSOR] ([TargetCode]) INCLUDE ([SourceCode], [Relationship]);
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND type = 'U'
)
BEGIN
    CREATE TABLE [conceptmap].[OkostedToSOR] (
        [SourceCode]                 NVARCHAR(100)  NOT NULL,
        [SourceDisplay]              NVARCHAR(500)  NULL,
        [Relationship]               NVARCHAR(50)   NOT NULL,
        [TargetCode]                 NVARCHAR(100)  NOT NULL,
        [TargetDisplay]              NVARCHAR(500)  NULL,
        [DW_EK_Okosted]             INT            NOT NULL,
        [DW_EK_Organisation_Target]  INT            NOT NULL,
        [DW_Timestamp_Insert]        DATETIME2(0)   NOT NULL,
        [DW_Timestamp_Update]        DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_conceptmap_OkostedToSOR] PRIMARY KEY CLUSTERED ([SourceCode], [TargetCode])
    );

    CREATE NONCLUSTERED INDEX [IX_conceptmap_OkostedToSOR_TargetCode]
        ON [conceptmap].[OkostedToSOR] ([TargetCode]) INCLUDE ([SourceCode], [Relationship]);
END
GO
