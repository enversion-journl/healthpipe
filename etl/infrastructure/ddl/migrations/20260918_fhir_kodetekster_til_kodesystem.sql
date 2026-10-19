-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Loeft IFDB's egne kodetekster ind i codesystem.IFDBConcept.
--
-- Seks tekstfelter er maalt entydigt bestemt af deres kode og fuldt kvalificerede
-- i kilden. Teksten hoerer derfor ét sted og ikke paa hver faktaraekke: kilden har
-- ti statusvaerdier fordelt paa 3,4 mio. henvisninger og én enkelt statustekst paa
-- 322.665 administrative procedurer.
--
-- Raekkefoelge ved udrulning: codesystem-tabellerne skal findes og vaere fyldt af
-- cs_ifdb_kodelister og synkroniseret til DSO, FOER vw_fhir-viewene genskabes.
-- Ellers joiner viewene mod en tom tabel, og teksten bliver NULL uden at noget
-- fejler. Koden bliver staaende paa raekken uanset hvad.

IF COL_LENGTH('fhir.ServiceRequest', 'status_kodetekst') IS NOT NULL
    ALTER TABLE [fhir].[ServiceRequest] DROP COLUMN [status_kodetekst];
IF COL_LENGTH('fhir.ServiceRequest', 'henvisningsmaade_kodetekst') IS NOT NULL
    ALTER TABLE [fhir].[ServiceRequest] DROP COLUMN [henvisningsmaade_kodetekst];
IF COL_LENGTH('fhir.ServiceRequest', 'prioritet_kodetekst') IS NOT NULL
    ALTER TABLE [fhir].[ServiceRequest] DROP COLUMN [prioritet_kodetekst];
IF COL_LENGTH('fhir.ServiceRequest', 'henvist_til_muni_kodetekst') IS NOT NULL
    ALTER TABLE [fhir].[ServiceRequest] DROP COLUMN [henvist_til_muni_kodetekst];
IF COL_LENGTH('fhir.Procedure', 'body_site_display') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [body_site_display];
IF COL_LENGTH('fhir.Procedure', 'staff_category_text') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [staff_category_text];
GO
