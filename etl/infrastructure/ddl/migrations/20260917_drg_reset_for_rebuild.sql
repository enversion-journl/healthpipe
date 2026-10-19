-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- toem DRG-loesningen, saa den genindlaeses fra leverancerne.
--
-- DRG er stand-alone: intet uden for loesningen refererer dens tabeller (ingen
-- fremmednoegler, intet ikke-DRG-objekt), og hver raekke kan reproduceres fra
-- tar.bz2-filerne paa NGC-sharen. Efter leverancedetektionen
-- er en fuld genindlaesning derfor den supporterede vej
-- til en ren tilstand.
--
-- KOERER MOD BEGGE DATABASER, og TRUNCATE er det rigtige paa dem begge:
--   * staging: arc/map/dim/fct/bridge toemmes, saa naeste ingest genopbygger
--     dem fra leverancerne. Tabellerne selv beholdes — base-DDL'en er
--     IF NOT EXISTS-guardet og ville ikke genskabe en droppet tabel med et
--     aendret skema alligevel, og denne aendring roerer ikke deres skema.
--   * DSO: kun dim/fct/bridge findes der (map er staging-internt), og de
--     toemmes, saa den foerste sync efter genopbygningen ikke efterlader en
--     blanding af gamle og nye dw_ek. TRUNCATE frem for DROP bevarer viewene
--     over dem og deres grants.
--
-- Hver saetning er guardet paa sit eget objekt, saa filen no-op'er pr. tabel
-- der ikke findes i den database, den koerer i. Ingen CURSOR, ingen procedurale
-- batches.
--
-- meta.DRGLeverance droppes IKKE her — det gjorde
-- 20260914_drg_leverance_skive_grain.sql, som koerer foer denne fil, og
-- base-DDL'en genskaber den med den nye grain.
--
-- TRUNCATE nulstiller ogsaa IDENTITY paa map.DRG*, saa dw_ek mintes forfra fra
-- 1. UNKNOWN-sentinellen (-1) genindsaettes af tables/drg/05_seeds.sql, der
-- koerer efter migrationerne.
--
-- VIGTIGT: dw_ek aendrer sig ved genmintning. Alt der laeser DRG-noegler
-- gennem DPA_DRG-viewene (datamarter-repoet) ser nye vaerdier efter
-- genopbygningen.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Arkivet: de 16 datasaet + prisrevisionsloggen (kun staging)
-- ----------------------------------------------------------------------------

IF OBJECT_ID(N'[arc].[drg_kontakt]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_kontakt];
GO

IF OBJECT_ID(N'[arc].[drg_sghforlob]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_sghforlob];
GO

IF OBJECT_ID(N'[arc].[drg_diagnose]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_diagnose];
GO

IF OBJECT_ID(N'[arc].[drg_diagnose_tillaeg]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_diagnose_tillaeg];
GO

IF OBJECT_ID(N'[arc].[drg_procedure]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_procedure];
GO

IF OBJECT_ID(N'[arc].[drg_procedure_tillaeg]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_procedure_tillaeg];
GO

IF OBJECT_ID(N'[arc].[drg_interne_koder]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_interne_koder];
GO

IF OBJECT_ID(N'[arc].[drg_takstbeskrivelse]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_takstbeskrivelse];
GO

IF OBJECT_ID(N'[arc].[drg_leveranceinfo]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_leveranceinfo];
GO

IF OBJECT_ID(N'[arc].[drgpsyk_kontakt]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drgpsyk_kontakt];
GO

IF OBJECT_ID(N'[arc].[drgpsyk_forloeb]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drgpsyk_forloeb];
GO

IF OBJECT_ID(N'[arc].[drgpsyk_diagnose]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drgpsyk_diagnose];
GO

IF OBJECT_ID(N'[arc].[drgpsyk_diagnose_tillaeg]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drgpsyk_diagnose_tillaeg];
GO

IF OBJECT_ID(N'[arc].[drgpsyk_procedure]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drgpsyk_procedure];
GO

IF OBJECT_ID(N'[arc].[drgpsyk_procedure_tillaeg]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drgpsyk_procedure_tillaeg];
GO

IF OBJECT_ID(N'[arc].[drgpsyk_betalingsoplysninger]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drgpsyk_betalingsoplysninger];
GO

IF OBJECT_ID(N'[arc].[drg_pris_revision]', 'U') IS NOT NULL
    TRUNCATE TABLE [arc].[drg_pris_revision];
GO


-- ----------------------------------------------------------------------------
-- Noeglemintningen (kun staging — map.DRG* naar aldrig DSO)
-- ----------------------------------------------------------------------------

IF OBJECT_ID(N'[map].[DRGKontakt]', 'U') IS NOT NULL
    TRUNCATE TABLE [map].[DRGKontakt];
GO

IF OBJECT_ID(N'[map].[DRGKontaktKlassifikation]', 'U') IS NOT NULL
    TRUNCATE TABLE [map].[DRGKontaktKlassifikation];
GO

IF OBJECT_ID(N'[map].[DRGDiagnose]', 'U') IS NOT NULL
    TRUNCATE TABLE [map].[DRGDiagnose];
GO

IF OBJECT_ID(N'[map].[DRGProcedure]', 'U') IS NOT NULL
    TRUNCATE TABLE [map].[DRGProcedure];
GO

IF OBJECT_ID(N'[map].[DRGDiagnoseGruppe]', 'U') IS NOT NULL
    TRUNCATE TABLE [map].[DRGDiagnoseGruppe];
GO

IF OBJECT_ID(N'[map].[DRGProcedureGruppe]', 'U') IS NOT NULL
    TRUNCATE TABLE [map].[DRGProcedureGruppe];
GO


-- ----------------------------------------------------------------------------
-- Bridges, fact og dimensioner — findes i BEGGE databaser.
-- Bridges foerst: de baerer dw_ek-referencer, saa de toemmes foer det, de
-- peger paa. Der er ingen fremmednoegler i dag, saa raekkefoelgen er kosmetisk
-- — men den er korrekt, hvis nogen tilfoejes senere.
-- ----------------------------------------------------------------------------

IF OBJECT_ID(N'[bridge].[DRGDiagnoseGruppe]', 'U') IS NOT NULL
    TRUNCATE TABLE [bridge].[DRGDiagnoseGruppe];
GO

IF OBJECT_ID(N'[bridge].[DRGProcedureGruppe]', 'U') IS NOT NULL
    TRUNCATE TABLE [bridge].[DRGProcedureGruppe];
GO

IF OBJECT_ID(N'[fct].[DRGKontakt]', 'U') IS NOT NULL
    TRUNCATE TABLE [fct].[DRGKontakt];
GO

IF OBJECT_ID(N'[dim].[DRGKontaktEgenskaber]', 'U') IS NOT NULL
    TRUNCATE TABLE [dim].[DRGKontaktEgenskaber];
GO

IF OBJECT_ID(N'[dim].[DRGKontaktKlassifikation]', 'U') IS NOT NULL
    TRUNCATE TABLE [dim].[DRGKontaktKlassifikation];
GO

IF OBJECT_ID(N'[dim].[DRGDiagnose]', 'U') IS NOT NULL
    TRUNCATE TABLE [dim].[DRGDiagnose];
GO

IF OBJECT_ID(N'[dim].[DRGProcedure]', 'U') IS NOT NULL
    TRUNCATE TABLE [dim].[DRGProcedure];
GO


-- ----------------------------------------------------------------------------
-- Checkpointene. Uden dette ser genopbygningen OEDELAGT ud frem for at fejle:
--
--   * tier-1-checkpointene ("dfm", "drg_leverance_<enhed>") holder hvilken fil
--     hver enhed sidst indlaeste. Bliver de staaende, beslutter extracten at
--     alt allerede er indlaest og laeser INTET.
--   * arc/map/dim/fct/bridge-checkpointene driver delta-vinduer paa
--     dw_timestamp_update. Bliver de staaende, behandler en delta-koersel kun
--     raekker aendret siden et forlaengst forkert checkpoint, og tabellerne
--     kommer tilbage HALVE.
--
-- Begge scopes ligger i samme tabel: DSO-synken bygger sin DfmManager paa
-- staging-ressourcen, saa scope='dso'-raekkerne staar ogsaa her.
--
-- Praedikatet rammer kun DRG-navne: begge leverance-praefikser under 'dfm',
-- drg_/drgpsyk_ under 'arc', og DRG% i de navngivne DSA-skemaer. Bemaerk at
-- '_' er et LIKE-jokertegn, saa praefikserne escapes med [_] — ellers ville
-- 'drg_leverance%' hverken ramme psyk-raekkerne eller kun ramme DRG's egne.
-- ----------------------------------------------------------------------------

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
    DELETE FROM [meta].[DFMState]
    WHERE ([schema_name] = 'dfm'
           AND ([table_name] LIKE 'drg[_]leverance[_]%'
                OR [table_name] LIKE 'drgpsyk[_]leverance[_]%'))
       OR ([schema_name] = 'arc'
           AND ([table_name] LIKE 'drg[_]%' OR [table_name] LIKE 'drgpsyk[_]%'))
       OR ([schema_name] IN ('map', 'dim', 'fct', 'bridge')
           AND [table_name] LIKE 'DRG%');
GO
