-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Kildesystemets egne afdelingskoder paa procedurer.
--
-- Aftageren har bedt om dem. De findes hverken i SOR-kodesystemet, i
-- SHAK-kodesystemet eller paa en Organization — maalt: nul traeffere for alle
-- seks — saa de kan ikke hentes ved opslag og maa staa som egne kolonner.
-- Det er ikke en kopi af opslagsdata, men kildens egne fakta.
--
-- Vaerdierne ligger allerede paa FHIR-ressourcen i procedureLocationMeta og
-- risYdelseLocationMeta, saa der kraeves ingen genpostering; kun ny
-- ViewDefinition og genbehandling af projektionen.

IF COL_LENGTH('fhir.Procedure', 'location_code') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [location_code] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.Procedure', 'location_orgcode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [location_orgcode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_kode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [stamafdeling_kode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_orgkode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [stamafdeling_orgkode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_kode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [udfoert_enhed_kode] NVARCHAR(512) NULL;
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_orgkode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [udfoert_enhed_orgkode] NVARCHAR(512) NULL;
GO
