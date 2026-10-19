-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ryd leverance-checkpoints skrevet under den gamle noegle.
--
-- Tier-1-checkpointet blev foerst skrevet som
--     target_type = 'dfm', table_name = 'drg_leverance_<leverancetype>_<takst>_<kadence>'
-- og skrives nu som
--     target_type = 'archive', table_name = '<leverancetype>_leverance_<takst>_<kadence>'
--
-- target_type: 'dfm' udledt af 'dfm' siger intet. Raekken rykkes foerst naar
-- leverancen ER landet i arc.*, saa 'archive' er det, den faktisk lover.
-- table_name: leverancetypen ER praefikset ('drg_leverance_T26_M',
-- 'drgpsyk_leverance_T26_M') i stedet for at staa to gange.
--
-- Begge felter indgaar i DFMState's primaernoegle, saa de gamle raekker bliver
-- ikke opdateret — de bliver liggende som forviltrede raekker, der hverken
-- laeses eller skrives. De slettes her, saa meta.DFMState kun viser noegler,
-- der er i brug.
--
-- At slette dem koster intet: 20260917_drg_reset_for_rebuild.sql toemmer alle
-- DRG-tabeller i samme opsaetning, saa loesningen genindlaeses fra
-- leverancerne under alle omstaendigheder. Denne fil findes udelukkende for de
-- miljoer, hvor 20260917 allerede var stemplet, foer noeglen blev rettet.
--
-- Praedikatet rammer BEGGE navneformer (gammel og ny) og er uafhaengigt af
-- target_type. '_' escapes som [_], da det ellers er et LIKE-jokertegn.
--
-- Kun staging har meta.DFMState-raekker for DRG-leverancer, men filen koerer
-- mod begge databaser og no-op'er, hvor tabellen ikke findes.
-- ============================================================================

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
    DELETE FROM [meta].[DFMState]
    WHERE [schema_name] = 'dfm'
      AND ([table_name] LIKE 'drg[_]leverance[_]%'
           OR [table_name] LIKE 'drgpsyk[_]leverance[_]%');
GO
