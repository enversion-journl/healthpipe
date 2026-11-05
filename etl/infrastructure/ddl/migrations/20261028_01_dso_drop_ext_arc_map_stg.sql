-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20261028_01_dso_drop_ext_arc_map_stg
--
-- Drops the schemas ext, arc, map and stg from the DSO database. On DSO they
-- hold no synced table, no view over synced data and no reader: arc and map
-- are copies of the staging tables, ext and stg are empty. The DSO
-- infrastructure skips these schemas (DSO_EXCLUDED_SCHEMAS in
-- infrastructure/assets/dso_infrastructure.py), so they are not created again.
--
-- Staging keeps all four schemas: the DB_NAME() gate makes the file a no-op
-- there. Each block drops only the objects that exist, so the file runs on
-- DEV, QA and PROD with different contents and is a no-op when the schemas
-- are absent. Order: foreign keys, views, procedures and functions, tables,
-- synonyms and sequences, user-defined types, then the schemas.
-- A foreign key or a schemabound object in another schema that points into
-- the four schemas makes the DROP fail instead of being dropped.
-- ============================================================================

IF DB_NAME() LIKE N'%dso%'
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(
            N'ALTER TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
            + N' DROP CONSTRAINT ' + QUOTENAME(fk.name) + N';'
            AS NVARCHAR(MAX)),
        NCHAR(10))
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.tables AS t ON t.object_id = fk.parent_object_id
    INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
    WHERE s.name IN (N'ext', N'arc', N'map', N'stg');

    IF @sql IS NOT NULL
        EXEC sp_executesql @sql;
END
GO

IF DB_NAME() LIKE N'%dso%'
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(N'DROP VIEW ' + QUOTENAME(s.name) + N'.' + QUOTENAME(v.name) + N';' AS NVARCHAR(MAX)),
        NCHAR(10))
    FROM sys.views AS v
    INNER JOIN sys.schemas AS s ON s.schema_id = v.schema_id
    WHERE s.name IN (N'ext', N'arc', N'map', N'stg');

    IF @sql IS NOT NULL
        EXEC sp_executesql @sql;
END
GO

IF DB_NAME() LIKE N'%dso%'
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(
            CASE WHEN o.type IN ('P', 'PC') THEN N'DROP PROCEDURE ' ELSE N'DROP FUNCTION ' END
            + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';'
            AS NVARCHAR(MAX)),
        NCHAR(10))
    FROM sys.objects AS o
    INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    WHERE s.name IN (N'ext', N'arc', N'map', N'stg')
      AND o.type IN ('P', 'PC', 'FN', 'IF', 'TF', 'FS', 'FT');

    IF @sql IS NOT NULL
        EXEC sp_executesql @sql;
END
GO

IF DB_NAME() LIKE N'%dso%'
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';' AS NVARCHAR(MAX)),
        NCHAR(10))
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
    WHERE s.name IN (N'ext', N'arc', N'map', N'stg');

    IF @sql IS NOT NULL
        EXEC sp_executesql @sql;
END
GO

IF DB_NAME() LIKE N'%dso%'
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(
            CASE o.type WHEN 'SN' THEN N'DROP SYNONYM ' ELSE N'DROP SEQUENCE ' END
            + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N';'
            AS NVARCHAR(MAX)),
        NCHAR(10))
    FROM sys.objects AS o
    INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    WHERE s.name IN (N'ext', N'arc', N'map', N'stg')
      AND o.type IN ('SN', 'SO');

    IF @sql IS NOT NULL
        EXEC sp_executesql @sql;
END
GO

IF DB_NAME() LIKE N'%dso%'
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(N'DROP TYPE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(ty.name) + N';' AS NVARCHAR(MAX)),
        NCHAR(10))
    FROM sys.types AS ty
    INNER JOIN sys.schemas AS s ON s.schema_id = ty.schema_id
    WHERE ty.is_user_defined = 1
      AND s.name IN (N'ext', N'arc', N'map', N'stg');

    IF @sql IS NOT NULL
        EXEC sp_executesql @sql;
END
GO

IF DB_NAME() LIKE N'%dso%'
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(N'DROP SCHEMA ' + QUOTENAME(s.name) + N';' AS NVARCHAR(MAX)),
        NCHAR(10))
    FROM sys.schemas AS s
    WHERE s.name IN (N'ext', N'arc', N'map', N'stg');

    IF @sql IS NOT NULL
        EXEC sp_executesql @sql;
END
GO
