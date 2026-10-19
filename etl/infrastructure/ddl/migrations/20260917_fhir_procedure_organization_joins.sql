-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Organisationens navn, SOR-kode, type og hierarki hentes via Organization.
-- Fjern kun de parallelle kildeattributter fra den flade Procedure-tabel.
-- Kildens oprindelige vaerdier bevares i IFDB-arkivet og FHIR-extensions.
-- Tidligere anvendte migrationer aendres ikke. Koer denne paa staging og DSO
-- med Procedure-loads stoppet og den nye procedure_view materialiseret.

IF COL_LENGTH('fhir.Procedure', 'location_code') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [location_code];
IF COL_LENGTH('fhir.Procedure', 'location_name') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [location_name];
IF COL_LENGTH('fhir.Procedure', 'location_orgcode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [location_orgcode];
IF COL_LENGTH('fhir.Procedure', 'location_sorcode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [location_sorcode];
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_kode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [stamafdeling_kode];
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_navn') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [stamafdeling_navn];
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_orgkode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [stamafdeling_orgkode];
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_sorcode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [stamafdeling_sorcode];
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_kode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [udfoert_enhed_kode];
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_navn') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [udfoert_enhed_navn];
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_orgkode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [udfoert_enhed_orgkode];
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_sorcode') IS NOT NULL
    ALTER TABLE [fhir].[Procedure] DROP COLUMN [udfoert_enhed_sorcode];
GO

-- SELECT *-viewets gemte kolonnemetadata skal foelge tabellen straks.
IF OBJECT_ID(N'[vw_fhir].[Procedure]', N'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[Procedure]';
GO
