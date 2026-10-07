-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Opfølgning på dm-splittet (review): encounter-relations-NØGLERNE flyttes fra
-- dm.HAIBA_Cases_*_Egenskaber til selve faktatabellerne
--
-- encounter_id_* og location_id_* er join-nøgler ud til fhir-kontakten og
-- hører hjemme på fakta, så joinet er direkte uden omvej over egenskaberne.
-- Matchets kvalitet (match_kvalitet_*, match_dage_*) bliver på egenskaberne.
--
-- dm er fuldt genopbyggeligt fra arc (TRUNCATE + INSERT + kontakt-stempling),
-- så migrationen dropper blot de fire case-tabeller i den gamle skikkelse og
-- lader end-state-DDL'en (tables/haiba/02_dm.sql) oprette den nye — første
-- kørsel efter deploy genskaber indholdet. Guarden er shape-baseret, så
-- migrationen er idempotent, rører aldrig den nye form, og virker på både
-- staging og DSO. Fakta og egenskaber droppes parvis, så de to skikkelser
-- aldrig blandes — og HVER tabel har sin egen shape-test, så et parvist drop
-- også udløses når kun den ene tabel står tilbage i den gamle skikkelse
-- (fx efter en delvis manuel oprydning): gammel egenskabstabel har
-- encounter_id-kolonner, gammel faktatabel MANGLER dem.
-- ============================================================================

IF (OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion_Egenskaber]', 'U') IS NOT NULL
    AND COL_LENGTH('[dm].[HAIBA_Cases_Infektion_Egenskaber]', 'encounter_id_kontaktansvar') IS NOT NULL)
   OR (OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion]', 'U') IS NOT NULL
    AND COL_LENGTH('[dm].[HAIBA_Cases_Infektion]', 'encounter_id_kontaktansvar') IS NULL)
BEGIN
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_Infektion_Egenskaber];
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_Infektion];
END
GO

IF (OBJECT_ID(N'[dm].[HAIBA_Cases_SSI_Egenskaber]', 'U') IS NOT NULL
    AND COL_LENGTH('[dm].[HAIBA_Cases_SSI_Egenskaber]', 'encounter_id_producent') IS NOT NULL)
   OR (OBJECT_ID(N'[dm].[HAIBA_Cases_SSI]', 'U') IS NOT NULL
    AND COL_LENGTH('[dm].[HAIBA_Cases_SSI]', 'encounter_id_producent') IS NULL)
BEGIN
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_SSI_Egenskaber];
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_SSI];
END
GO
