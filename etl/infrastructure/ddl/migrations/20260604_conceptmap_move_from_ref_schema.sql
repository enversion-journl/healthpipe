-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Move ConceptMap from ref.ConceptMap_* to conceptmap.* on DSO; drop legacy ref objects.
-- Staging: drop ref.ConceptMap_* views (replaced by conceptmap.vw_* in 03_ref_views.sql).

-- ---- DSO: migrate data from ref.* to conceptmap.* then drop ref tables ----

IF OBJECT_ID(N'[ref].[ConceptMap_Rekvirent_To_SOR]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[Rekvirent_To_SOR]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [conceptmap].[Rekvirent_To_SOR])
BEGIN
    INSERT INTO [conceptmap].[Rekvirent_To_SOR] (
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        [DW_EK_Rekvirent], [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    SELECT
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        [DW_EK_Rekvirent], [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    FROM [ref].[ConceptMap_Rekvirent_To_SOR];
    PRINT 'Migrated ref.ConceptMap_Rekvirent_To_SOR -> conceptmap.Rekvirent_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_Lager_To_SOR]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[Lager_To_SOR]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [conceptmap].[Lager_To_SOR])
BEGIN
    INSERT INTO [conceptmap].[Lager_To_SOR] (
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        [DW_EK_Lager], [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    SELECT
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        [DW_EK_Lager], [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    FROM [ref].[ConceptMap_Lager_To_SOR];
    PRINT 'Migrated ref.ConceptMap_Lager_To_SOR -> conceptmap.Lager_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_EAN_To_SOR]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[EAN_To_SOR]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [conceptmap].[EAN_To_SOR])
BEGIN
    INSERT INTO [conceptmap].[EAN_To_SOR] (
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        [DW_EK_EAN], [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    SELECT
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        [DW_EK_EAN], [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    FROM [ref].[ConceptMap_EAN_To_SOR];
    PRINT 'Migrated ref.ConceptMap_EAN_To_SOR -> conceptmap.EAN_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[Okosted_To_SOR]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [conceptmap].[Okosted_To_SOR])
BEGIN
    DECLARE @okosted_src_col SYSNAME;
    SELECT @okosted_src_col = c.name
    FROM sys.columns c
    WHERE c.object_id = OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]')
      AND c.name IN (N'DW_EK_Okosted', N'DW_EK_Organisation_Source');

    IF @okosted_src_col IS NOT NULL
    BEGIN
        DECLARE @okosted_migrate_sql NVARCHAR(MAX) = N'
            INSERT INTO [conceptmap].[Okosted_To_SOR] (
                [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
                [DW_EK_Okosted], [DW_EK_Organisation_Target],
                [DW_Timestamp_Insert], [DW_Timestamp_Update]
            )
            SELECT
                [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
                ' + QUOTENAME(@okosted_src_col) + N', [DW_EK_Organisation_Target],
                [DW_Timestamp_Insert], [DW_Timestamp_Update]
            FROM [ref].[ConceptMap_Okosted_To_SOR]';
        EXEC sp_executesql @okosted_migrate_sql;
        PRINT 'Migrated ref.ConceptMap_Okosted_To_SOR -> conceptmap.Okosted_To_SOR';
    END
END
GO

-- Drop legacy ref.ConceptMap_* tables (DSO)
IF OBJECT_ID(N'[ref].[ConceptMap_Rekvirent_To_SOR]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [ref].[ConceptMap_Rekvirent_To_SOR];
    PRINT 'Dropped ref.ConceptMap_Rekvirent_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_Lager_To_SOR]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [ref].[ConceptMap_Lager_To_SOR];
    PRINT 'Dropped ref.ConceptMap_Lager_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_EAN_To_SOR]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [ref].[ConceptMap_EAN_To_SOR];
    PRINT 'Dropped ref.ConceptMap_EAN_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [ref].[ConceptMap_Okosted_To_SOR];
    PRINT 'Dropped ref.ConceptMap_Okosted_To_SOR';
END
GO

-- Drop legacy ref.ConceptMap_* views (staging)
IF OBJECT_ID(N'[ref].[ConceptMap_Rekvirent_To_SOR]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [ref].[ConceptMap_Rekvirent_To_SOR];
    PRINT 'Dropped view ref.ConceptMap_Rekvirent_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_Lager_To_SOR]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [ref].[ConceptMap_Lager_To_SOR];
    PRINT 'Dropped view ref.ConceptMap_Lager_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_EAN_To_SOR]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [ref].[ConceptMap_EAN_To_SOR];
    PRINT 'Dropped view ref.ConceptMap_EAN_To_SOR';
END
GO

IF OBJECT_ID(N'[ref].[ConceptMap_Okosted_To_SOR]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [ref].[ConceptMap_Okosted_To_SOR];
    PRINT 'Dropped view ref.ConceptMap_Okosted_To_SOR';
END
GO
