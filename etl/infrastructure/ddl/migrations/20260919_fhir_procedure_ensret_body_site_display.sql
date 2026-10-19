-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Ensret Procedure-skemaet: body_site_display skal vaere vaek i begge databaser.
--
-- Synkroniseringen sammenligner staging og DSO kolonne for kolonne og stopper
-- ved forskel. Den fandt kolonnen i staging og ikke i DSO. De to foregaaende
-- migrationer dropper netop den kolonne og er registreret som anvendt begge
-- steder, saa ingen af dem koerer igen — en migration, der er registreret uden
-- at have kunnet udfoere sit arbejde, ligner for altid en, der har gjort det.
--
-- Derfor en ny fil. Den er betinget og goer intet, hvor kolonnen allerede er
-- vaek, saa den er ufarlig i de miljoeer, der er i orden.
--
-- Teksten er ikke tabt: vw_fhir.Procedure joiner den paa fra kodelisten under
-- samme navn. To kolonner kan ikke hedde det samme, saa den flade tabel maa
-- ikke baere den.

IF COL_LENGTH('fhir.Procedure', 'body_site_display') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [body_site_display];
GO
