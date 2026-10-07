-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: nye kolonner paa fhir.Procedure - knivtid, kildenoegle og
-- deloperations-reference. Alle tre var felter, der kun laa i JSON paa
-- FHIR-ressourcen og derfor ikke kunne bruges i en opgoerelse.
--
-- Knivtiden laa hidtil kun i kilde-metadata paa FHIR-ressourcen og kunne derfor
-- ikke bruges til statistik eller operationsplanlaegning uden at aabne JSON.
-- Kolonnerne er DATETIME2 og ikke NVARCHAR som performed_period, fordi knivtid
-- skal kunne regnes paa; varigheden beregnes i byggelaget af de to tidspunkter,
-- saa aftagerne ikke skal parse strenge i SQL.
--
-- Knivtid er ikke det samme som performed_period: performed_period er
-- kilderaekkens administrative aktivitetsperiode, knivtid er det tidsrum der
-- reelt blev opereret. Ingen af dem overskriver den anden.
--
-- Idempotent; safe on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', 'knivtid_start') IS NULL
BEGIN
    ALTER TABLE [fhir].[Procedure] ADD [knivtid_start] DATETIME2 NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', 'knivtid_slut') IS NULL
BEGIN
    ALTER TABLE [fhir].[Procedure] ADD [knivtid_slut] DATETIME2 NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', 'knivtid_minutter') IS NULL
BEGIN
    ALTER TABLE [fhir].[Procedure] ADD [knivtid_minutter] INT NULL;
END
GO

-- =============================================================================
-- Samme migrering: kildenoegle og deloperations-reference paa fhir.Procedure.
--
-- source_identifier_value: ifdb_aktivitet_id daekker kun SFI. Uden en noegle,
-- der virker for alle tre procedurekilder, kan en aftager ikke spore en
-- procedure tilbage til kilderaekken, medmindre den kommer fra SFI - en
-- asymmetri, ingen forventer. Navnet er det samme som paa fhir.Condition og
-- child-tabellerne, saa join-skabeloner kan genbruges paa tvaers.
-- Miljoeer hvor kolonnen naaede at hedde source_key, omdoebes med data i behold.
--
-- part_of_id: uden den kan 1+N-strukturen (hovedoperation plus
-- deloperationer) ikke opgoeres fladt, og man kan ikke taelle operationer uden
-- at blande hoved og dele sammen.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', 'source_identifier_value') IS NULL
BEGIN
    IF COL_LENGTH(N'[fhir].[Procedure]', 'source_key') IS NOT NULL
        EXEC sp_rename N'[fhir].[Procedure].[source_key]', N'source_identifier_value', N'COLUMN';
    ELSE
        ALTER TABLE [fhir].[Procedure] ADD [source_identifier_value] NVARCHAR(255) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', 'part_of_id') IS NULL
BEGIN
    ALTER TABLE [fhir].[Procedure] ADD [part_of_id] NVARCHAR(255) NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', 'lpr3_ignored') IS NULL
BEGIN
    ALTER TABLE [fhir].[Procedure] ADD [lpr3_ignored] BIT NULL;
END
GO
