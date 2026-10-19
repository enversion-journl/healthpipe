-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Fjern body_site_display, hvor den er blevet genskabt efter sin egen migration.
--
-- Grund-DDL'en havde en efterreparationsblok, der tilfoejede kolonnen til en
-- eksisterende Procedure-tabel. Blokken er fjernet, men i miljoeer hvor et
-- infrastrukturtrin naaede at koere mellem migrationen og fjernelsen, staar
-- kolonnen tilbage. Den forrige migration er registreret som anvendt og koerer
-- ikke igen, saa oprydningen skal have sin egen fil.
--
-- Uden dette fejler vw_fhir.Procedure: viewet joiner teksten paa fra kodelisten
-- under samme navn, og to kolonner kan ikke hedde det samme.

IF COL_LENGTH('fhir.Procedure', 'body_site_display') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [body_site_display];
GO
