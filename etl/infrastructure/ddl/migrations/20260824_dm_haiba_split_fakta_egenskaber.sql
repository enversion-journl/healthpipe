-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Split af dm.HAIBA_* i map -> fakta + transaktionsdimensioner
--
-- De fire brede dm-tabeller (1:1 med arc plus opslåede nøgler) erstattes af
-- slanke faktatabeller (kun DW-nøgler + typede measures) med en
-- _Egenskaber-transaktionsdimension ved siden af, og et map-lag der minter
-- dw_ek. Organisatoriske tekstkolonner udgår (findes i dim.Organisation),
-- og rå CPR forlader dm-laget — den lever herefter kun i map + arc.
--
-- Denne migration DROPPER de gamle tabeller. Det er tabsfrit: dm rebuildes
-- fuldt fra arc ved hver kørsel (TRUNCATE + INSERT), så indholdet genskabes
-- i den nye struktur af første kørsel efter deploy.
--
-- Ny form: faktatabellerne genbruger navnene dm.HAIBA_*. Guarden skelner
-- derfor på skikkelse, ikke på eksistens: en kolonne der KUN findes i den
-- gamle brede form (Sundhedsinstitution på agg, cprnr på cases) identificerer
-- en tabel der skal væk. Migrationen er dermed idempotent og rører aldrig
-- den nye form. End-state-DDL'en (tables/haiba/01_maps.sql + 02_dm.sql)
-- opretter de nye tabeller — staging_infra_haiba kører efter migrations.
-- ============================================================================

IF OBJECT_ID(N'[dm].[HAIBA_Agg_Infektion]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Agg_Infektion]', 'Sundhedsinstitution') IS NOT NULL
BEGIN
    DROP TABLE [dm].[HAIBA_Agg_Infektion];
END
GO

IF OBJECT_ID(N'[dm].[HAIBA_Agg_Ortopaedi]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Agg_Ortopaedi]', 'Sundhedsinstitution') IS NOT NULL
BEGIN
    DROP TABLE [dm].[HAIBA_Agg_Ortopaedi];
END
GO

IF OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Cases_Infektion]', 'cprnr') IS NOT NULL
BEGIN
    DROP TABLE [dm].[HAIBA_Cases_Infektion];
END
GO

IF OBJECT_ID(N'[dm].[HAIBA_Cases_SSI]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Cases_SSI]', 'cprnr') IS NOT NULL
BEGIN
    DROP TABLE [dm].[HAIBA_Cases_SSI];
END
GO
