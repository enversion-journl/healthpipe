-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ref.FakturalinjeMatchKeys: dw_ek_Dato_faktura + watermark-indeks
--
-- fct_apply_fakturalinje_unspsc driver nu HELE sit delta fra
-- ref.FakturalinjeMatchKeys i stedet for at joine fct.Fakturalinje for både
-- watermark og faktura-dato-scope. Match-nøgle-tabellen er den afledte
-- nøglemængde: enhver fakta-ændring der kan påvirke et UNSPSC-match ændrer
-- nødvendigvis en kolonne her (MERGE'en er INTERSECT-beskyttet), så fakta-
-- tabellens eget watermark tilføjer intet — og at læse det tvang en
-- disjunktion på tværs af to tabeller, som ingen indeks kunne dække.
--
-- To ting kræves for det:
--
--   1. dw_ek_Dato_faktura på match-nøgle-tabellen, så
--      APPLY_PASS_MIN_FAKTURA_DATE kan anvendes uden at røre fct.Fakturalinje.
--   2. IX_ref_fakturalinje_matchkeys_timestamp, så delta-udtrækket bliver et
--      seek i stedet for en clustered scan af hver fakturalinje nogensinde set.
--
-- Opgraderingssti for EKSISTERENDE databaser. På en frisk DB deklarerer basis-
-- DDL'en (tables/ref/01_tables.sql) allerede begge, og apply_migrations
-- baseliner (stempler uden at køre) i stedet for at migrere — så denne fil
-- rører aldrig en frisk database. Guards holdes alligevel, fordi migrations
-- kører FØR ref-tabellerne oprettes (staging_infra_ref deps på
-- staging_migrations) på en database der ér blevet migreret før.
-- Kan gentages sikkert: hvert trin er guardet.
-- ============================================================================

-- 1) Kolonnen. DEFAULT -1 matcher dim.Dato's UNKNOWN-sentinel, som apply-passets
--    scope-filter eksplicit lukker ind (jf. "OR = -1"), så en endnu ikke
--    backfill'et række aldrig falder ud af scope ved et uheld.
IF OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
         AND name = 'dw_ek_Dato_faktura'
   )
BEGIN
    ALTER TABLE [ref].[FakturalinjeMatchKeys]
        ADD [dw_ek_Dato_faktura] INT NOT NULL
            CONSTRAINT [DF_Ref_FakturalinjeMatchKeys_dw_ek_Dato_faktura] DEFAULT -1;
    PRINT 'Added dw_ek_Dato_faktura to ref.FakturalinjeMatchKeys';
END
GO

-- 2) Backfill fra fakta-tabellen. Uden dette ville dw_ek_Dato_faktura først
--    blive korrekt for rækker der tilfældigvis rammes af et staging-delta,
--    fordi kolonnen nu indgår i MERGE'ens INTERSECT-ændringsdetektion.
--
--    dw_timestamp_update bumpes MED VILJE IKKE: backfill'et ændrer ikke hvilket
--    UNSPSC en linje resolver til, så at bumpe watermarket ville tvinge apply-
--    passet gennem en fuld re-resolve af hver fakturalinje uden nogen effekt.
--
--    Rækker uden fakta-modstykke beholder -1 (UNKNOWN) og bliver dermed i
--    scope — samme adfærd som den tidligere "OR f.[dw_ek_Dato_faktura] = -1".
--
--    EXEC, fordi batch'en refererer en kolonne der først findes efter trin 1.
IF OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
         AND name = 'dw_ek_Dato_faktura'
   )
BEGIN
    EXEC('
        UPDATE mk
        SET mk.[dw_ek_Dato_faktura] = f.[dw_ek_Dato_faktura]
        FROM [ref].[FakturalinjeMatchKeys] mk
        INNER JOIN [fct].[Fakturalinje] f
            ON f.[dw_ek_Fakturalinje] = mk.[dw_ek_Fakturalinje]
        WHERE mk.[dw_ek_Dato_faktura] <> f.[dw_ek_Dato_faktura];
    ');
    PRINT 'Backfilled ref.FakturalinjeMatchKeys.dw_ek_Dato_faktura from fct.Fakturalinje';
END
GO

-- 3) Watermark-indekset. Parret med tables/ref/01_tables.sql, så en frisk DB
--    får det fra basis-DDL'en og dette trin no-op'er.
IF OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
         AND name = 'dw_ek_Dato_faktura'
   )
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
         AND name = 'IX_ref_fakturalinje_matchkeys_timestamp'
   )
BEGIN
    EXEC('
        CREATE NONCLUSTERED INDEX [IX_ref_fakturalinje_matchkeys_timestamp]
            ON [ref].[FakturalinjeMatchKeys] ([dw_timestamp_update])
            INCLUDE ([dw_ek_Dato_faktura]);
    ');
    PRINT 'Created IX_ref_fakturalinje_matchkeys_timestamp';
END
GO
