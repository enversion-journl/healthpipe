-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Prisloese aftalelinjer + bridge.AftaleLager (spec
-- 2026-08-31-prisloese-linjer-og-aftalelager):
--   1) fct.AftaleVarePris.har_prisversion tilfoejes (1 = rigtig prisversion,
--      0 = syntetisk daekningsraekke for en prisloes linje).
--   2) dim.Aftale.alle_maa_benytte tilfoejes (1 = ingen lafthsgh-styring =
--      alle sygehuse/lagre maa koebe ind).
--   3) IX_Fct_AftaleVarePris_linjenoegle tilfoejes (understoetter
--      pre-delete-sweepens EXISTS-scope paa linjenoeglen).
--   4) fct/AftaleVarePris-checkpointet nulstilles: bk_prrowid faar
--      kildepraefiks ('LAFTDPRIS_'/'LAFTD_') paa BEGGE raekketyper, saa ALLE
--      eksisterende raekker fejler key-match paa foerste ordinaere koersel
--      efter deploy og maa genopbygges under det nye praefiks-format.
--   5) IX_Bridge_AftaleLager_Aftale tilfoejes (review-fund: dw_ek_Aftale er
--      den primaere join-vej fra dim.Aftale, base-DDL'en manglede indekset).
--      Guardet paa BAADE tabel- og indeks-eksistens: en dev-DB kan allerede
--      have oprettet bridge.AftaleLager fra en tidligere commit paa denne
--      branch (foer indekset fandtes i 05_bridges.sql), i hvilket tilfaelde
--      base-DDL'ens CREATE-blok ikke koerer igen — denne guardede migration
--      daekker det tilfaelde.
-- Koerer mod BAADE staging og DSO (begge tabeller findes i begge). Idempotent:
-- alle guards taaler gentagen koersel og no-op'er paa friske databaser
-- (basis-DDL'en baerer netto-tilstanden).
-- INGEN bridge.AftaleLager CREATE TABLE her: tabellen er NY og oprettes
-- udelukkende af den guardede base-DDL (05_bridges.sql), som infra-assets
-- koerer hver gang. Punkt 5) er en guardet indeks-tilfoejelse for allerede
-- oprettede tabeller, ikke en tabel-DDL.
-- Ingen TRUNCATE: fct-reconciliation sker via MERGE + pre-delete-sweep
-- (dw_sk-stabilitet mod DSO), dim-reconciliation via MERGE (dw_ek-stabilitet).

-- 1) fct.AftaleVarePris.har_prisversion
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'har_prisversion') IS NULL
BEGIN
    ALTER TABLE [fct].[AftaleVarePris] ADD [har_prisversion] BIT NOT NULL CONSTRAINT [DF_AftaleVarePris_har_prisversion] DEFAULT 1;
END
GO

-- 2) dim.Aftale.alle_maa_benytte
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH('[dim].[Aftale]', 'alle_maa_benytte') IS NULL
BEGIN
    ALTER TABLE [dim].[Aftale] ADD [alle_maa_benytte] BIT NOT NULL CONSTRAINT [DF_Aftale_alle_maa_benytte] DEFAULT 1;
END
GO

-- 3) Sweep-understoettende indeks (review-fund): pre-delete-sweepen i
-- fct_AftaleVarePris.py (PRE_DELETE_SQL) skopes paa linjenoeglen
-- (bk_leverandoernummer, bk_aftale_nummer, bk_ils_varenummer) — uden dette
-- indeks scanner sweepen hele tabellen hver koersel. Spec'ens "No index
-- changes" veg for denne ruling. Base-DDL'en (03_facts.sql) baerer samme
-- indeks for friske databaser.
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                     AND name = 'IX_Fct_AftaleVarePris_linjenoegle')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_linjenoegle]
    ON [fct].[AftaleVarePris] ([bk_leverandoernummer] ASC, [bk_aftale_nummer] ASC, [bk_ils_varenummer] ASC) ON [PRIMARY];
END
GO

-- 4) Backfill-trigger: nulstil fct/AftaleVarePris-checkpointet igen (dev har
-- allerede konsumeret 20260827c's nulstilling; denne migration er en NY
-- grain-aendring — bk_prrowid'ens format aendres, ikke bare kolonner tilfoejes
-- — saa checkpointet skal nulstilles paa ny, ellers restager en delta-koersel
-- intet og alle eksisterende raekker forbliver haengende under det gamle
-- praefiksloese bk_prrowid-format). Prod har endnu ikke koert nogen af disse
-- migrationer og rammer derfor alt i rigtig raekkefoelge ved foerste koersel.
-- Manglende DFMState-raekke = foerste-koersels-semantik (BEGINNING_OF_TIME);
-- raekken genoprettes automatisk ved naeste checkpoint-avance. Kun
-- scope='staging' beroeres; no-op paa DSO og friske databaser.
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
    EXEC('DELETE FROM meta.DFMState WHERE scope = ''staging'' AND schema_name = ''fct'' AND table_name = ''AftaleVarePris''');
GO

-- 5) IX_Bridge_AftaleLager_Aftale (review-fund): dw_ek_Aftale er den
-- primaere join-vej fra dim.Aftale ind i bridge.AftaleLager. Base-DDL'en
-- (05_bridges.sql) baerer samme indeks for friske databaser; denne guardede
-- tilfoejelse daekker en dev-DB der allerede har oprettet tabellen fra en
-- tidligere commit paa denne branch (foer indekset fandtes i base-DDL'en),
-- hvor base-DDL'ens CREATE-blok derfor ikke koerer igen.
IF OBJECT_ID(N'[bridge].[AftaleLager]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[bridge].[AftaleLager]')
                     AND name = 'IX_Bridge_AftaleLager_Aftale')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Bridge_AftaleLager_Aftale]
    ON [bridge].[AftaleLager] ([dw_ek_Aftale] ASC) ON [PRIMARY];
END
GO
