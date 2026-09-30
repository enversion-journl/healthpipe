-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: VareudleveringEgenskaber explicit FK relationship (#27574)
--
-- Goals:
--   1. Rename dim.VareudleveringEgenskaber columns to align with the Egenskaber pattern
--      (DW_EK_Vareudlevering -> DW_EK_VareudleveringEgenskaber, CleanCode_* likewise).
--   2. Convert dim PK from BIGINT to INT (matches map.Vareudlevering INT IDENTITY).
--   3. Replace fct.Vareudlevering.DW_EK_Vareudlevering BIGINT IDENTITY with INT NOT NULL,
--      sourced from map.Vareudlevering (matches the fct.Lagerbeholdning pattern).
--   4. Add fct.Vareudlevering.DW_EK_VareudleveringEgenskaber INT FK column (same value as
--      fact PK since they share map.Vareudlevering).
--   5. Add supporting index on the new FK column.
--
-- NOTE: The fact's DW_EK_Vareudlevering values WILL CHANGE — they are renumbered from the
-- map. Any external consumer that cached the old IDENTITY values must rerun their joins
-- via CleanCode_Vareudlevering.

-- ===========================================================================
-- Section A: dim.VareudleveringEgenskaber column rename + type narrow
-- ===========================================================================

-- A.1: Rename DW_EK_Vareudlevering -> DW_EK_VareudleveringEgenskaber
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'DW_EK_Vareudlevering'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'DW_EK_VareudleveringEgenskaber'
)
BEGIN
    EXEC sp_rename
        N'[dim].[VareudleveringEgenskaber].[DW_EK_Vareudlevering]',
        N'DW_EK_VareudleveringEgenskaber',
        N'COLUMN';
    PRINT 'Renamed dim.VareudleveringEgenskaber.DW_EK_Vareudlevering -> DW_EK_VareudleveringEgenskaber';
END
GO

-- A.2: Rename CleanCode_Vareudlevering -> CleanCode_VareudleveringEgenskaber
-- The unique filtered index IX_Dim_VareudleveringEgenskaber_CleanCode references
-- this column in its WHERE clause; sp_rename does not propagate to filtered
-- index expressions, so we drop the index first and recreate it in A.2b.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'CleanCode_Vareudlevering'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'CleanCode_VareudleveringEgenskaber'
)
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
          AND name = N'IX_Dim_VareudleveringEgenskaber_CleanCode'
    )
    BEGIN
        DROP INDEX [IX_Dim_VareudleveringEgenskaber_CleanCode]
            ON [dim].[VareudleveringEgenskaber];
        PRINT 'Dropped IX_Dim_VareudleveringEgenskaber_CleanCode for column rename';
    END

    EXEC sp_rename
        N'[dim].[VareudleveringEgenskaber].[CleanCode_Vareudlevering]',
        N'CleanCode_VareudleveringEgenskaber',
        N'COLUMN';
    PRINT 'Renamed dim.VareudleveringEgenskaber.CleanCode_Vareudlevering -> CleanCode_VareudleveringEgenskaber';
END
GO

-- A.2b: Recreate the filtered unique index against the renamed column
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'CleanCode_VareudleveringEgenskaber'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND name = N'IX_Dim_VareudleveringEgenskaber_CleanCode'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_VareudleveringEgenskaber_CleanCode]
        ON [dim].[VareudleveringEgenskaber] ([CleanCode_VareudleveringEgenskaber] ASC)
        WHERE [CleanCode_VareudleveringEgenskaber] IS NOT NULL
        ON [PRIMARY];
    PRINT 'Recreated IX_Dim_VareudleveringEgenskaber_CleanCode on renamed column';
END
GO

-- A.3: Narrow dim PK from BIGINT to INT (values come from map.Vareudlevering INT IDENTITY).
-- The PK constraint may have an auto-generated name on older servers, so look it up
-- dynamically and drop it by whatever name it has, then re-add with the canonical name.
IF EXISTS (
    SELECT 1 FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
      AND c.name = N'DW_EK_VareudleveringEgenskaber'
      AND t.name = N'bigint'
)
BEGIN
    DECLARE @pk_name NVARCHAR(128) = (
        SELECT name FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]')
          AND type = 'PK'
    );
    IF @pk_name IS NOT NULL
    BEGIN
        DECLARE @drop_pk_sql NVARCHAR(400) =
            N'ALTER TABLE [dim].[VareudleveringEgenskaber] DROP CONSTRAINT ' + QUOTENAME(@pk_name);
        EXEC sp_executesql @drop_pk_sql;
    END

    ALTER TABLE [dim].[VareudleveringEgenskaber] ALTER COLUMN [DW_EK_VareudleveringEgenskaber] INT NOT NULL;
    ALTER TABLE [dim].[VareudleveringEgenskaber]
        ADD CONSTRAINT [PK_Dim_VareudleveringEgenskaber]
        PRIMARY KEY CLUSTERED ([DW_EK_VareudleveringEgenskaber] ASC);
    PRINT 'Narrowed dim.VareudleveringEgenskaber.DW_EK_VareudleveringEgenskaber from BIGINT to INT';
END
GO

-- ===========================================================================
-- Section B: fct.Vareudlevering — replace IDENTITY BIGINT PK with INT sourced from map
-- ===========================================================================

-- B.1: Add transitional column for the new PK value (nullable for backfill)
IF EXISTS (
    SELECT 1 FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND c.name = N'DW_EK_Vareudlevering'
      AND t.name = N'bigint'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'DW_EK_Vareudlevering_new'
)
BEGIN
    ALTER TABLE [fct].[Vareudlevering] ADD [DW_EK_Vareudlevering_new] INT NULL;
    PRINT 'Added transitional column DW_EK_Vareudlevering_new INT NULL';
END
GO

-- B.2: Backfill the new PK from map.Vareudlevering via CleanCode
-- Dynamic SQL: DW_EK_Vareudlevering_new is renamed by B.4, so static DML
-- would fail compile-time validation on re-run.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'DW_EK_Vareudlevering_new'
)
BEGIN
    EXEC(N'UPDATE f
    SET f.DW_EK_Vareudlevering_new = m.DW_EK_Vareudlevering
    FROM [fct].[Vareudlevering] f
    INNER JOIN [map].[Vareudlevering] m
        ON m.CleanCode_Vareudlevering = f.CleanCode_Vareudlevering
    WHERE f.DW_EK_Vareudlevering_new IS NULL');
    PRINT 'Backfilled DW_EK_Vareudlevering_new from map.Vareudlevering';
END
GO

-- B.3: Verify backfill is complete (any unmapped row would block the swap)
-- Dynamic SQL: DW_EK_Vareudlevering_new is renamed by B.4, so static DML
-- would fail compile-time validation on re-run.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'DW_EK_Vareudlevering_new'
)
BEGIN
    DECLARE @nulls INT;
    EXEC sp_executesql
        N'SELECT @n = COUNT(*) FROM [fct].[Vareudlevering] WHERE [DW_EK_Vareudlevering_new] IS NULL',
        N'@n INT OUTPUT',
        @n = @nulls OUTPUT;
    IF @nulls > 0
    BEGIN
        DECLARE @msg NVARCHAR(400) = CONCAT(
            'fct.Vareudlevering: ', @nulls,
            ' rows have no match in map.Vareudlevering — populate map_vareudlevering first.'
        );
        THROW 50000, @msg, 1;
    END
END
GO

-- B.4: Drop old PK + IDENTITY column, then promote the new column into its place.
-- The PK may have an auto-generated name on older servers; look it up dynamically.
IF EXISTS (
    SELECT 1 FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND c.name = N'DW_EK_Vareudlevering'
      AND t.name = N'bigint'
)
BEGIN
    DECLARE @fct_pk_name NVARCHAR(128) = (
        SELECT name FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
          AND type = 'PK'
    );
    IF @fct_pk_name IS NOT NULL
    BEGIN
        DECLARE @drop_fct_pk_sql NVARCHAR(400) =
            N'ALTER TABLE [fct].[Vareudlevering] DROP CONSTRAINT ' + QUOTENAME(@fct_pk_name);
        EXEC sp_executesql @drop_fct_pk_sql;
    END

    ALTER TABLE [fct].[Vareudlevering] DROP COLUMN [DW_EK_Vareudlevering];
    EXEC sp_rename
        N'[fct].[Vareudlevering].[DW_EK_Vareudlevering_new]',
        N'DW_EK_Vareudlevering',
        N'COLUMN';
    ALTER TABLE [fct].[Vareudlevering] ALTER COLUMN [DW_EK_Vareudlevering] INT NOT NULL;
    ALTER TABLE [fct].[Vareudlevering]
        ADD CONSTRAINT [PK_Fct_Vareudlevering]
        PRIMARY KEY CLUSTERED ([DW_EK_Vareudlevering] ASC);
    PRINT 'Replaced fct.Vareudlevering.DW_EK_Vareudlevering BIGINT IDENTITY with INT (from map)';
END
GO

-- ===========================================================================
-- Section C: fct.Vareudlevering — add DW_EK_VareudleveringEgenskaber FK column
-- ===========================================================================

-- C.1: Add the FK column
IF OBJECT_ID(N'[fct].[Vareudlevering]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
          AND name = N'DW_EK_VareudleveringEgenskaber'
    )
BEGIN
    ALTER TABLE [fct].[Vareudlevering] ADD [DW_EK_VareudleveringEgenskaber] INT NOT NULL DEFAULT -1;
    PRINT 'Added column DW_EK_VareudleveringEgenskaber to fct.Vareudlevering';
END
GO

-- C.2: Backfill — same value as DW_EK_Vareudlevering since both come from map.Vareudlevering
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
      AND name = N'DW_EK_VareudleveringEgenskaber'
)
BEGIN
    UPDATE [fct].[Vareudlevering]
    SET DW_EK_VareudleveringEgenskaber = DW_EK_Vareudlevering
    WHERE DW_EK_VareudleveringEgenskaber = -1;
    PRINT CONCAT('Backfilled DW_EK_VareudleveringEgenskaber for ', @@ROWCOUNT, ' rows');
END
GO

-- C.3: Add supporting index
IF OBJECT_ID(N'[fct].[Vareudlevering]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
          AND name = N'IX_Fct_Vareudlevering_VareudleveringEgenskaber'
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Vareudlevering_VareudleveringEgenskaber] ON [fct].[Vareudlevering]
    ([DW_EK_VareudleveringEgenskaber] ASC) ON [PRIMARY];
    PRINT 'Created index IX_Fct_Vareudlevering_VareudleveringEgenskaber';
END
GO
