-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Dato calendar expansion (ADO 29286)
--
-- Brings existing dim.Dato tables in line with the new base DDL:
-- - Renames legacy columns to the new naming contract.
-- - Adds new calendar attributes and relative-period columns.
-- - Drops the legacy [weekend] flag in favour of [hverdag_weekend].
-- - Adds the BLANK sentinel row (dw_ek_Dato = 99999998).
-- - Backfills all static calendar attributes for real dates.
--
-- Runs on BOTH staging and DSO databases.
-- Skips entirely when dim.Dato does not exist yet (greenfield / unbaselined DB).
-- ============================================================================

-- --------------------------------------------------------------------------
-- Rename legacy columns (guard both old and new names)
-- --------------------------------------------------------------------------
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'Dato'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'dato'
)
    EXEC sp_rename 'dim.Dato.Dato', 'dato', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'aar_id'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'aar'
)
    EXEC sp_rename 'dim.Dato.aar_id', 'aar', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'kvartal_navn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'kvartal'
)
    EXEC sp_rename 'dim.Dato.kvartal_navn', 'kvartal', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'kvartal_aarnavn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'kvartal_aar'
)
    EXEC sp_rename 'dim.Dato.kvartal_aarnavn', 'kvartal_aar', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'maaned_navn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'maaned_lang'
)
    EXEC sp_rename 'dim.Dato.maaned_navn', 'maaned_lang', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'maaned_aarnavn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'maaned_aar'
)
    EXEC sp_rename 'dim.Dato.maaned_aarnavn', 'maaned_aar', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'uge_navn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'uge'
)
    EXEC sp_rename 'dim.Dato.uge_navn', 'uge', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'uge_aarnavn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'uge_aar'
)
    EXEC sp_rename 'dim.Dato.uge_aarnavn', 'uge_aar', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'dag_i_aar_id'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'dag_aar'
)
    EXEC sp_rename 'dim.Dato.dag_i_aar_id', 'dag_aar', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'dag_i_maaned_id'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'dag_maaned'
)
    EXEC sp_rename 'dim.Dato.dag_i_maaned_id', 'dag_maaned', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'ugedag_id'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'dag_uge'
)
    EXEC sp_rename 'dim.Dato.ugedag_id', 'dag_uge', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'ugedag_navn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'ugedag_lang'
)
    EXEC sp_rename 'dim.Dato.ugedag_navn', 'ugedag_lang', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'hverdag'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'er_hverdag'
)
    EXEC sp_rename 'dim.Dato.hverdag', 'er_hverdag', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'helligdag_dk'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'er_helligdag'
)
    EXEC sp_rename 'dim.Dato.helligdag_dk', 'er_helligdag', 'COLUMN';
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'helligdag_dk_navn'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
      AND name COLLATE Latin1_General_100_CS_AS = 'helligdag'
)
    EXEC sp_rename 'dim.Dato.helligdag_dk_navn', 'helligdag', 'COLUMN';
GO

-- --------------------------------------------------------------------------
-- Add new columns from ADO 29286 (idempotent)
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
    IF COL_LENGTH('dim.Dato', 'aar_startdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [aar_startdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'aar_slutdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [aar_slutdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'aar_afsluttet') IS NULL
        ALTER TABLE [dim].[Dato] ADD [aar_afsluttet] BIT NULL;
    IF COL_LENGTH('dim.Dato', 'aar_relativ') IS NULL
        ALTER TABLE [dim].[Dato] ADD [aar_relativ] SMALLINT NULL;

    IF COL_LENGTH('dim.Dato', 'kvartals_startdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [kvartals_startdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'kvartals_slutdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [kvartals_slutdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'kvartal_afsluttet') IS NULL
        ALTER TABLE [dim].[Dato] ADD [kvartal_afsluttet] BIT NULL;
    IF COL_LENGTH('dim.Dato', 'kvartal_relativ') IS NULL
        ALTER TABLE [dim].[Dato] ADD [kvartal_relativ] SMALLINT NULL;

    IF COL_LENGTH('dim.Dato', 'maaned') IS NULL
        ALTER TABLE [dim].[Dato] ADD [maaned] NVARCHAR(50) NULL;
    IF COL_LENGTH('dim.Dato', 'maaned_startdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [maaned_startdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'maaned_slutdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [maaned_slutdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'maaned_afsluttet') IS NULL
        ALTER TABLE [dim].[Dato] ADD [maaned_afsluttet] BIT NULL;
    IF COL_LENGTH('dim.Dato', 'maaned_relativ') IS NULL
        ALTER TABLE [dim].[Dato] ADD [maaned_relativ] SMALLINT NULL;

    IF COL_LENGTH('dim.Dato', 'uge_startdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [uge_startdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'uge_slutdato') IS NULL
        ALTER TABLE [dim].[Dato] ADD [uge_slutdato] DATE NULL;
    IF COL_LENGTH('dim.Dato', 'uge_afsluttet') IS NULL
        ALTER TABLE [dim].[Dato] ADD [uge_afsluttet] BIT NULL;
    IF COL_LENGTH('dim.Dato', 'uge_relativ') IS NULL
        ALTER TABLE [dim].[Dato] ADD [uge_relativ] SMALLINT NULL;

    IF COL_LENGTH('dim.Dato', 'dag_kvartal') IS NULL
        ALTER TABLE [dim].[Dato] ADD [dag_kvartal] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'dag_relativ') IS NULL
        ALTER TABLE [dim].[Dato] ADD [dag_relativ] SMALLINT NULL;
    IF COL_LENGTH('dim.Dato', 'ugedag') IS NULL
        ALTER TABLE [dim].[Dato] ADD [ugedag] NVARCHAR(50) NULL;

    IF COL_LENGTH('dim.Dato', 'hverdag_weekend') IS NULL
        ALTER TABLE [dim].[Dato] ADD [hverdag_weekend] NVARCHAR(50) NULL;
    IF COL_LENGTH('dim.Dato', 'er_beredskabshelligdag') IS NULL
        ALTER TABLE [dim].[Dato] ADD [er_beredskabshelligdag] BIT NULL;
    IF COL_LENGTH('dim.Dato', 'beredskabshelligdag') IS NULL
        ALTER TABLE [dim].[Dato] ADD [beredskabshelligdag] NVARCHAR(50) NULL;
END
GO

-- --------------------------------------------------------------------------
-- Drop legacy weekend column (remove default first if present)
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
AND COL_LENGTH('dim.Dato', 'weekend') IS NOT NULL
BEGIN
    DECLARE @sql_drop_weekend_default NVARCHAR(MAX) = N'';

    SELECT @sql_drop_weekend_default = STRING_AGG(
        N'ALTER TABLE [dim].[Dato] DROP CONSTRAINT [' + dc.name + N'];',
        CHAR(10)
    )
    FROM sys.default_constraints dc
    INNER JOIN sys.columns c
        ON c.object_id = dc.parent_object_id
       AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Dato]')
      AND c.name = N'weekend';

    IF @sql_drop_weekend_default <> N''
        EXEC sp_executesql @sql_drop_weekend_default;

    ALTER TABLE [dim].[Dato] DROP COLUMN [weekend];
END
GO

-- --------------------------------------------------------------------------
-- Align datatypes
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
    IF COL_LENGTH('dim.Dato', 'kvartal_aar_id') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [kvartal_aar_id] INT NULL;
    IF COL_LENGTH('dim.Dato', 'kvartal_id') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [kvartal_id] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'maaned_id') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [maaned_id] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'uge_id') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [uge_id] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'dag_kvartal') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [dag_kvartal] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'dag_maaned') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [dag_maaned] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'dag_uge') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [dag_uge] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'kvartal_antal_dage') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [kvartal_antal_dage] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'maaned_antal_dage') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [maaned_antal_dage] TINYINT NULL;
    IF COL_LENGTH('dim.Dato', 'er_hverdag') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [er_hverdag] BIT NULL;
    IF COL_LENGTH('dim.Dato', 'er_helligdag') IS NOT NULL
        ALTER TABLE [dim].[Dato] ALTER COLUMN [er_helligdag] BIT NULL;
END
GO

-- --------------------------------------------------------------------------
-- Ensure BLANK sentinel row exists
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM [dim].[Dato] WHERE [dw_ek_Dato] = 99999998)
BEGIN
    INSERT INTO [dim].[Dato] (
        [dw_ek_Dato],
        [kvartal], [kvartal_aar],
        [maaned_lang], [maaned], [maaned_aar],
        [uge], [uge_aar],
        [ugedag_lang], [ugedag],
        [hverdag_weekend],
        [helligdag],
        [beredskabshelligdag]
    ) VALUES (
        99999998,
        N'Blank', N'Blank',
        N'Blank', N'Blank', N'Blank',
        N'Blank', N'Blank',
        N'Blank', N'Blank',
        N'Blank',
        N'Blank',
        N'Blank'
    );
END
GO

-- --------------------------------------------------------------------------
-- Backfill calendar attributes for real date rows
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
    SET DATEFIRST 1;
    SET LANGUAGE Danish;

    UPDATE d
SET
    d.[aar] = CAST(YEAR(d.[dato]) AS SMALLINT),
    d.[aar_startdato] = DATEFROMPARTS(YEAR(d.[dato]), 1, 1),
    d.[aar_slutdato] = DATEFROMPARTS(YEAR(d.[dato]), 12, 31),
    d.[kvartal_id] = CAST(DATEPART(QUARTER, d.[dato]) AS TINYINT),
    d.[kvartal] = N'K' + CAST(DATEPART(QUARTER, d.[dato]) AS NVARCHAR(1)),
    d.[kvartals_startdato] = CAST(DATEADD(QUARTER, DATEDIFF(QUARTER, 0, d.[dato]), 0) AS DATE),
    d.[kvartals_slutdato] = CAST(
        DATEADD(DAY, -1, DATEADD(QUARTER, DATEDIFF(QUARTER, 0, d.[dato]) + 1, 0)) AS DATE
    ),
    d.[kvartal_aar_id] = YEAR(d.[dato]) * 100 + DATEPART(QUARTER, d.[dato]),
    d.[kvartal_aar] = N'K' + CAST(DATEPART(QUARTER, d.[dato]) AS NVARCHAR(1)) + N' '
        + CAST(YEAR(d.[dato]) AS NVARCHAR(4)),
    d.[maaned_id] = CAST(MONTH(d.[dato]) AS TINYINT),
    d.[maaned_lang] = DATENAME(MONTH, d.[dato]),
    d.[maaned] = LEFT(DATENAME(MONTH, d.[dato]), 3),
    d.[maaned_startdato] = DATEFROMPARTS(YEAR(d.[dato]), MONTH(d.[dato]), 1),
    d.[maaned_slutdato] = EOMONTH(d.[dato]),
    d.[maaned_aar_id] = YEAR(d.[dato]) * 100 + MONTH(d.[dato]),
    d.[maaned_aar] = LEFT(DATENAME(MONTH, d.[dato]), 3) + N' ' + CAST(YEAR(d.[dato]) AS NVARCHAR(4)),
    d.[uge_id] = CAST(DATEPART(ISOWK, d.[dato]) AS TINYINT),
    d.[uge] = N'U' + CAST(DATEPART(ISOWK, d.[dato]) AS NVARCHAR(2)),
    d.[uge_startdato] = DATEADD(DAY, 1 - DATEPART(WEEKDAY, d.[dato]), d.[dato]),
    d.[uge_slutdato] = DATEADD(DAY, 7 - DATEPART(WEEKDAY, d.[dato]), d.[dato]),
    d.[uge_aar_id] = (
        CASE
            WHEN DATEPART(DAYOFYEAR, d.[dato]) > 350 AND DATEPART(ISOWK, d.[dato]) = 1
                THEN YEAR(d.[dato]) + 1
            WHEN DATEPART(DAYOFYEAR, d.[dato]) < 10 AND DATEPART(ISOWK, d.[dato]) >= 52
                THEN YEAR(d.[dato]) - 1
            ELSE YEAR(d.[dato])
        END
    ) * 100 + DATEPART(ISOWK, d.[dato]),
    d.[uge_aar] = N'U' + RIGHT(N'00' + CAST(DATEPART(ISOWK, d.[dato]) AS NVARCHAR(2)), 2) + N' '
        + CAST(
            CASE
                WHEN DATEPART(DAYOFYEAR, d.[dato]) > 350 AND DATEPART(ISOWK, d.[dato]) = 1
                    THEN YEAR(d.[dato]) + 1
                WHEN DATEPART(DAYOFYEAR, d.[dato]) < 10 AND DATEPART(ISOWK, d.[dato]) >= 52
                    THEN YEAR(d.[dato]) - 1
                ELSE YEAR(d.[dato])
            END AS NVARCHAR(4)
        ),
    d.[dag_aar] = CAST(DATEPART(DAYOFYEAR, d.[dato]) AS SMALLINT),
    d.[dag_maaned] = CAST(DAY(d.[dato]) AS TINYINT),
    d.[dag_uge] = CAST(DATEPART(WEEKDAY, d.[dato]) AS TINYINT),
    d.[dag_kvartal] = CAST(
        DATEDIFF(DAY, DATEADD(QUARTER, DATEDIFF(QUARTER, 0, d.[dato]), 0), d.[dato]) + 1 AS TINYINT
    ),
    d.[ugedag_lang] = DATENAME(WEEKDAY, d.[dato]),
    d.[ugedag] = LEFT(DATENAME(WEEKDAY, d.[dato]), 3),
    d.[er_hverdag] = CASE WHEN DATEPART(WEEKDAY, d.[dato]) BETWEEN 1 AND 5 THEN 1 ELSE 0 END,
    d.[hverdag_weekend] = CASE WHEN DATEPART(WEEKDAY, d.[dato]) BETWEEN 1 AND 5
        THEN N'Hverdag'
        ELSE N'Weekend'
    END,
    d.[er_helligdag] = COALESCE(d.[er_helligdag], 0),
    d.[helligdag] = COALESCE(NULLIF(d.[helligdag], N''), N'Ikke en helligdag')
    FROM [dim].[Dato] d
    WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231;

    UPDATE d
    SET [aar_antal_dage] = q.cnt
    FROM [dim].[Dato] d
    INNER JOIN (
        SELECT [aar], COUNT(*) AS cnt
        FROM [dim].[Dato]
        WHERE [dw_ek_Dato] BETWEEN 19700101 AND 20501231
        GROUP BY [aar]
    ) q ON q.[aar] = d.[aar]
    WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231;

    UPDATE d
    SET [kvartal_antal_dage] = CAST(q.cnt AS TINYINT)
    FROM [dim].[Dato] d
    INNER JOIN (
        SELECT [kvartal_aar_id], COUNT(*) AS cnt
        FROM [dim].[Dato]
        WHERE [dw_ek_Dato] BETWEEN 19700101 AND 20501231
        GROUP BY [kvartal_aar_id]
    ) q ON q.[kvartal_aar_id] = d.[kvartal_aar_id]
    WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231;

    UPDATE d
    SET [maaned_antal_dage] = CAST(q.cnt AS TINYINT)
    FROM [dim].[Dato] d
    INNER JOIN (
        SELECT [maaned_aar_id], COUNT(*) AS cnt
        FROM [dim].[Dato]
        WHERE [dw_ek_Dato] BETWEEN 19700101 AND 20501231
        GROUP BY [maaned_aar_id]
    ) q ON q.[maaned_aar_id] = d.[maaned_aar_id]
    WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231;

    UPDATE d
    SET
        d.[kvartal] = s.[label],
        d.[kvartal_aar] = s.[label],
        d.[maaned_lang] = s.[label],
        d.[maaned] = s.[label],
        d.[maaned_aar] = s.[label],
        d.[uge] = s.[label],
        d.[uge_aar] = s.[label],
        d.[ugedag_lang] = s.[label],
        d.[ugedag] = s.[label],
        d.[hverdag_weekend] = s.[label],
        d.[helligdag] = s.[label],
        d.[beredskabshelligdag] = s.[label]
    FROM [dim].[Dato] d
    INNER JOIN (
        VALUES
            (-1, N'Ukendt'),
            (-2, N'Før min dato'),
            (99999998, N'Blank'),
            (99999999, N'Efter max dato')
    ) s([dw_ek_Dato], [label])
        ON s.[dw_ek_Dato] = d.[dw_ek_Dato];

    UPDATE d
    SET
        d.[er_beredskabshelligdag] = CASE
            WHEN d.[er_helligdag] = 1 OR (MONTH(d.[dato]) = 12 AND DAY(d.[dato]) IN (24, 31))
                THEN 1
            ELSE 0
        END,
        d.[beredskabshelligdag] = CASE
            WHEN d.[er_helligdag] = 1
                THEN d.[helligdag]
            WHEN MONTH(d.[dato]) = 12 AND DAY(d.[dato]) = 24
                THEN N'Juleaften'
            WHEN MONTH(d.[dato]) = 12 AND DAY(d.[dato]) = 31
                THEN N'Nytårsaften'
            ELSE N'Ikke en beredskabshelligdag'
        END
    FROM [dim].[Dato] d
    WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231;
END
GO
