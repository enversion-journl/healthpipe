-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- dim.Aftale from SCD Type 2 to Type 1.
--
-- dim.Aftale was the only Type 2 dimension in the ERP model, while
-- fct.AftaleVarePris.dw_ek_Aftale is an FK to a dimension whose declared PK is
-- dw_ek_Aftale alone. A join on that key multiplied the fact rows by the
-- number of versions.
--
-- The history carried almost no information: of 18 superseded rows, 14 were
-- noise (the same value out and back within two runs) and the remaining 4 were
-- hoved_slut extensions that the current row already describes.
--
-- This migration keeps the newest version per dw_ek_Aftale and drops the
-- versioning and soft-delete columns. Expired and deleted agreements remain as
-- ordinary rows — historical purchases reference them, and the source removes
-- agreements once they expire.
--
-- Idempotent: gated on whether dw_valid_from still exists.

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Aftale'
      AND COLUMN_NAME = 'dw_valid_from'
)
BEGIN
    -- 1) Keep the newest version per dw_ek_Aftale. Ordering on dw_valid_from
    --    DESC rather than filtering on dw_valid_to = '9999-12-31' so that a key
    --    holding only soft-deleted rows also keeps one row.
    ;WITH ranked AS (
        SELECT
            dw_ek_Aftale,
            dw_valid_from,
            ROW_NUMBER() OVER (
                PARTITION BY dw_ek_Aftale
                ORDER BY dw_valid_from DESC
            ) AS rn
        FROM [dim].[Aftale]
    )
    DELETE FROM ranked WHERE rn > 1;

    PRINT 'dim.Aftale: collapsed to one row per dw_ek_Aftale';

    -- 2) The filtered index references dw_is_deleted and dw_valid_to
    IF EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'IX_Dim_Aftale_Current')
        DROP INDEX [IX_Dim_Aftale_Current] ON [dim].[Aftale];

    -- 3) The PK contains dw_valid_from
    IF EXISTS (SELECT 1 FROM sys.key_constraints
               WHERE parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'PK_Dim_Aftale')
        ALTER TABLE [dim].[Aftale] DROP CONSTRAINT [PK_Dim_Aftale];

    -- 4) Default constraints must go before the columns can be dropped. Names
    --    are looked up dynamically since they may differ between environments.
    DECLARE @df NVARCHAR(256);

    SELECT @df = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND c.name = 'dw_valid_to';
    IF @df IS NOT NULL
        EXEC('ALTER TABLE [dim].[Aftale] DROP CONSTRAINT [' + @df + ']');

    SET @df = NULL;
    SELECT @df = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND c.name = 'dw_is_deleted';
    IF @df IS NOT NULL
        EXEC('ALTER TABLE [dim].[Aftale] DROP CONSTRAINT [' + @df + ']');

    SET @df = NULL;
    SELECT @df = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND c.name = 'dw_valid_from';
    IF @df IS NOT NULL
        EXEC('ALTER TABLE [dim].[Aftale] DROP CONSTRAINT [' + @df + ']');

    -- 5) Drop the versioning and soft-delete columns
    ALTER TABLE [dim].[Aftale] DROP COLUMN [dw_valid_from];
    ALTER TABLE [dim].[Aftale] DROP COLUMN [dw_valid_to];
    ALTER TABLE [dim].[Aftale] DROP COLUMN [dw_is_deleted];

    PRINT 'dim.Aftale: dropped dw_valid_from, dw_valid_to and dw_is_deleted';

    -- 6) New PK on the entity key alone
    ALTER TABLE [dim].[Aftale]
        ADD CONSTRAINT [PK_Dim_Aftale] PRIMARY KEY CLUSTERED ([dw_ek_Aftale] ASC);

    PRINT 'dim.Aftale: PK_Dim_Aftale recreated on dw_ek_Aftale';
END
GO

-- Covering index replacing the filtered IX_Dim_Aftale_Current
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = 'IX_Dim_Aftale_Nummer')
   AND EXISTS (SELECT 1 FROM sys.objects
               WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_Aftale_Nummer] ON [dim].[Aftale]
    ([aftale_nummer] ASC)
    INCLUDE ([aftaletype], [hoved_start], [hoved_slut]);
    PRINT 'dim.Aftale: IX_Dim_Aftale_Nummer created';
END
GO

-- The UNKNOWN row must exist after the rebuild
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[Aftale] WHERE [dw_ek_Aftale] = -1)
BEGIN
    EXEC('
        INSERT INTO [dim].[Aftale] (
            [dw_ek_Aftale], [clean_code_Aftale], [aftale_nummer], [dw_timestamp_insert]
        ) VALUES (
            -1, N''UNKNOWN'', N''UNKNOWN'', CAST(SYSDATETIME() AS DATETIME2)
        )
    ');
    PRINT 'dim.Aftale: UNKNOWN row reinserted';
END
GO
