-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20261103b_01_arc_ils_pmdm_fjern_kolonner
--
-- ILS har fjernet 18 kolonner fra dbo.pmdm: de dobbelte brede*/gtinydre ved
-- siden af bredde*/gtinyder, enhedstype*, aftale-felterne og
-- liniestart-/linieslutdatotid. Udtrækket bygger ext.ils_pmdm ud fra kildens
-- kolonner ved hver kørsel, så arkivets MERGE fejlede fra 03-11 med "Invalid
-- column name 'enhedstypemindste'".
--
-- Ingen DSA-, DQA- eller DSO-kode læser kolonnerne (nedstrøms bruges kun lnr,
-- lvnr og vnr), og intet view læser arc.ils_pmdm. De fjernes derfor fra
-- tabellen her og fra base-DDL'en (sources/ils/ils_pmdm.sql) og
-- arkivkonfigurationen (_NON_KEY_COLUMNS i ils_archive_configs.py) i samme
-- commit.
--
-- Kolonnerne findes via sys.columns, så en gentaget kørsel finder ingen og
-- gør intet. DEFAULT-constraints på kolonnerne droppes først, selv om base-
-- DDL'en ikke har nogen. arc findes ikke på DSO, så filen er en no-op dér.
-- ============================================================================
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[arc].[ils_pmdm]', 'U') IS NOT NULL
BEGIN
    DECLARE @kolonner TABLE (navn sysname PRIMARY KEY);
    INSERT INTO @kolonner (navn) VALUES
        (N'enhedstypemindste'), (N'enhedstypeinder'), (N'enhedstypeyder'),
        (N'enhedstypepalle'), (N'gtinydre'), (N'bredemindste'),
        (N'bredeinder'), (N'bredeyder'), (N'bredepalle'), (N'aftale'),
        (N'aftalenavn'), (N'aftdto'), (N'aftstart'), (N'aftslut'),
        (N'afttype'), (N'aftansvar'), (N'liniestartdatotid'),
        (N'linieslutdatotid');

    DECLARE @constraints NVARCHAR(MAX);
    DECLARE @drop NVARCHAR(MAX);

    SELECT @constraints = STRING_AGG(QUOTENAME(dc.name), N', ')
    FROM sys.default_constraints AS dc
    JOIN sys.columns AS c
        ON c.object_id = dc.parent_object_id
       AND c.column_id = dc.parent_column_id
    JOIN @kolonner AS k ON k.navn = c.name
    WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_pmdm]');

    IF @constraints IS NOT NULL
        EXEC (N'ALTER TABLE [arc].[ils_pmdm] DROP CONSTRAINT ' + @constraints);

    SELECT @drop = STRING_AGG(QUOTENAME(c.name), N', ')
    FROM sys.columns AS c
    JOIN @kolonner AS k ON k.navn = c.name
    WHERE c.object_id = OBJECT_ID(N'[arc].[ils_pmdm]');

    IF @drop IS NOT NULL
    BEGIN
        EXEC (N'ALTER TABLE [arc].[ils_pmdm] DROP COLUMN ' + @drop);
        PRINT N'Droppede fra arc.ils_pmdm: ' + @drop;
    END
    ELSE
        PRINT N'arc.ils_pmdm har ingen af de udgåede kolonner - intet at droppe.';
END
GO
