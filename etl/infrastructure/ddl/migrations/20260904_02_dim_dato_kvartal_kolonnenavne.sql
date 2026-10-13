-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Dato: rename kvartals_startdato/kvartals_slutdato -> kvartal_startdato/
-- kvartal_slutdato (navnekonsistens).
--
-- Alle andre perioder i dim.Dato bruger <periode>_startdato/<periode>_slutdato
-- (aar_startdato, maaned_startdato, uge_startdato). Kun kvartal havde et
-- ekstra 's'.
--
-- Præsentationsaliasserne i vw_dim.Dato ([Kvartals startdato]/[Kvartals
-- slutdato]) er korrekt dansk genitiv og er bevidst UÆNDRET — det er alene de
-- tekniske snake_case-navne, der rettes her.
--
-- Kolonnerne blev tilføjet af 20260819_dim_dato_calendar_expansion.sql, som er
-- bevaret uændret som historik. Databaser bygget fra den nuværende
-- tables/-DDL får de rigtige navne direkte og har migrationerne baselinet, så
-- denne fil kun rammer ældre, allerede udrullede databaser.
--
-- Safe to re-run: renames kun når det gamle navn findes OG det nye endnu ikke
-- er taget.
-- Bug: #29715
-- ============================================================================

IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[Dato]') AND name = 'kvartals_startdato'
   )
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[Dato]') AND name = 'kvartal_startdato'
   )
BEGIN
    EXEC sp_rename '[dim].[Dato].[kvartals_startdato]', 'kvartal_startdato', 'COLUMN';
    PRINT 'Renamed dim.Dato.kvartals_startdato -> kvartal_startdato';
END
GO

IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[Dato]') AND name = 'kvartals_slutdato'
   )
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[Dato]') AND name = 'kvartal_slutdato'
   )
BEGIN
    EXEC sp_rename '[dim].[Dato].[kvartals_slutdato]', 'kvartal_slutdato', 'COLUMN';
    PRINT 'Renamed dim.Dato.kvartals_slutdato -> kvartal_slutdato';
END
GO
