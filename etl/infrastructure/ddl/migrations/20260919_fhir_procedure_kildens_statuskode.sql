-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Kildens egen statuskode paa administrative procedurer.
--
-- Aftageren har bedt om baade statuskode og statustekst. Teksten blev
-- projiceret som status_text, men koden blev bygget ind i ressourcen og aldrig
-- projiceret ned i den flade tabel. Kolonnen status daekker den ikke: den er en
-- FHIR-oversaettelse af kildens sletteflag og skelner kun mellem gennemfoert og
-- tilbagekaldt registrering.
--
-- Kode og tekst bliver staaende som raa kolonner. Parret er maalt sammen med de
-- oevrige kode/tekst-par og hoerer ikke til dem, hvor teksten er entydigt givet
-- af koden, saa det kan ikke loeftes til kodesystemet.
--
-- Vaerdien ligger allerede paa FHIR-ressourcen i procedureSourceMeta, saa der
-- kraeves ingen genpostering; kun ny ViewDefinition og genbehandling af
-- projektionen.

IF COL_LENGTH('fhir.Procedure', 'status_code') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [status_code] NVARCHAR(512) NULL;
GO
