-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: fjern udbudskategori-implementeringen. Sletter først de rækker i
-- dim.UNSPSC og map.UNSPSC som KUN kom fra udbud-filen (ingen ILS/KMD-
-- observation), dropper derefter udbud-kolonnerne på begge tabeller samt
-- arkivtabellen arc.unspsc_udbudskategori (CSV-seedet findes kun i staging;
-- guarderne gør filen til en no-op på DSO for arc-tabellen). Auto-applied til
-- BÅDE staging og DSO af migrationsassets. vw_dim.UNSPSC er SELECT * og
-- refreshes til sidst, hvor view'et findes.

-- dim.UNSPSC: slet udbud-only rækker mens flagene stadig findes. Dynamisk SQL
-- fordi dw_source_* er omdøbte kolonner (kompileringstids-binding). Guarden på
-- dw_source_udbud gør genkørsel efter kolonnedrop til en no-op.
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'dw_source_udbud'
   )
BEGIN
    EXEC(N'DELETE FROM [dim].[UNSPSC]
           WHERE [dw_source_udbud] = 1 AND [dw_source_ils] = 0 AND [dw_source_kmd] = 0');
    PRINT 'Deleted udbud-only rows from dim.UNSPSC';
END
GO

-- map.UNSPSC: slet udbud-only rækker mens flagene stadig findes
IF OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]') AND name = 'dw_source_udbud'
   )
BEGIN
    EXEC(N'DELETE FROM [map].[UNSPSC]
           WHERE [dw_source_udbud] = 1 AND [dw_source_ils] = 0 AND [dw_source_kmd] = 0');
    PRINT 'Deleted udbud-only rows from map.UNSPSC';
END
GO

-- dim.UNSPSC: dw_source_udbud (NOT NULL med DEFAULT — drop constraint først)
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'dw_source_udbud'
   )
BEGIN
    DECLARE @df_dim NVARCHAR(128);
    SELECT @df_dim = dc.name
    FROM sys.default_constraints dc
    INNER JOIN sys.columns c
        ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[UNSPSC]')
      AND c.name = 'dw_source_udbud';

    IF @df_dim IS NOT NULL
        EXEC('ALTER TABLE [dim].[UNSPSC] DROP CONSTRAINT [' + @df_dim + ']');

    ALTER TABLE [dim].[UNSPSC] DROP COLUMN [dw_source_udbud];
    PRINT 'Dropped dim.UNSPSC.dw_source_udbud';
END
GO

-- dim.UNSPSC: beskrivelse_udbud
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'beskrivelse_udbud'
   )
BEGIN
    ALTER TABLE [dim].[UNSPSC] DROP COLUMN [beskrivelse_udbud];
    PRINT 'Dropped dim.UNSPSC.beskrivelse_udbud';
END
GO

-- dim.UNSPSC: udbudstype
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'udbudstype'
   )
BEGIN
    ALTER TABLE [dim].[UNSPSC] DROP COLUMN [udbudstype];
    PRINT 'Dropped dim.UNSPSC.udbudstype';
END
GO

-- dim.UNSPSC: udbudskategori_niveau_1
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'udbudskategori_niveau_1'
   )
BEGIN
    ALTER TABLE [dim].[UNSPSC] DROP COLUMN [udbudskategori_niveau_1];
    PRINT 'Dropped dim.UNSPSC.udbudskategori_niveau_1';
END
GO

-- dim.UNSPSC: udbudskategori_niveau_2
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'udbudskategori_niveau_2'
   )
BEGIN
    ALTER TABLE [dim].[UNSPSC] DROP COLUMN [udbudskategori_niveau_2];
    PRINT 'Dropped dim.UNSPSC.udbudskategori_niveau_2';
END
GO

-- dim.UNSPSC: udbudskategori_niveau_3
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'udbudskategori_niveau_3'
   )
BEGIN
    ALTER TABLE [dim].[UNSPSC] DROP COLUMN [udbudskategori_niveau_3];
    PRINT 'Dropped dim.UNSPSC.udbudskategori_niveau_3';
END
GO

-- dim.UNSPSC: udbudskategori_niveau_4
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = 'udbudskategori_niveau_4'
   )
BEGIN
    ALTER TABLE [dim].[UNSPSC] DROP COLUMN [udbudskategori_niveau_4];
    PRINT 'Dropped dim.UNSPSC.udbudskategori_niveau_4';
END
GO

-- map.UNSPSC: dw_source_udbud (NOT NULL med DEFAULT — drop constraint først)
IF OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]') AND name = 'dw_source_udbud'
   )
BEGIN
    DECLARE @df_map NVARCHAR(128);
    SELECT @df_map = dc.name
    FROM sys.default_constraints dc
    INNER JOIN sys.columns c
        ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[map].[UNSPSC]')
      AND c.name = 'dw_source_udbud';

    IF @df_map IS NOT NULL
        EXEC('ALTER TABLE [map].[UNSPSC] DROP CONSTRAINT [' + @df_map + ']');

    ALTER TABLE [map].[UNSPSC] DROP COLUMN [dw_source_udbud];
    PRINT 'Dropped map.UNSPSC.dw_source_udbud';
END
GO

-- arc.unspsc_udbudskategori: CSV-arkivet bag de fjernede kolonner
IF OBJECT_ID(N'[arc].[unspsc_udbudskategori]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [arc].[unspsc_udbudskategori];
    PRINT 'Dropped arc.unspsc_udbudskategori';
END
GO

-- vw_dim.UNSPSC er SELECT * — refresh så view-metadata matcher det nye kolonnesæt
IF OBJECT_ID(N'[vw_dim].[UNSPSC]', 'V') IS NOT NULL
    EXEC sp_refreshview N'[vw_dim].[UNSPSC]';
GO
