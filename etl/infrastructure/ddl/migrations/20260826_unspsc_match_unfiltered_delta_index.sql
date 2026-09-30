-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ref.KMDFakturalinjeUNSPSCMatch: ufiltreret delta-indeks
--
-- fct_apply_fakturalinje_unspsc bygger #RefChangedKeys som:
--
--     SELECT DISTINCT clean_code_kreditor, clean_code_faktura
--     FROM ref.KMDFakturalinjeUNSPSCMatch
--     WHERE dw_timestamp_update > <checkpoint>
--
-- Der er MED VILJE ikke noget dw_is_deleted-filter: MERGE'en i
-- ref_kmd_fakturalinje_unspsc_match bumper dw_timestamp_update når den
-- soft-deleter (WHEN NOT MATCHED BY SOURCE), og apply-passet har ikke længere
-- et separat demote-pass — tilbagefaldet til -1 afhænger fuldstændigt af, at
-- den soft-deletede række kommer med i deltaet.
--
-- Alle tre eksisterende nonclustered indeks på tabellen er filtreret
-- `WHERE dw_is_deleted = 0`. Et filtreret indeks kan kun bruges, når dets
-- prædikat er implicit i forespørgslens, og SQL Server samler ikke selv et
-- filtreret indeks med en scan for komplementet. Eneste tilbageværende
-- adgangsvej var derfor den clustered PK på identity-kolonnen: en fuld scan af
-- hele tabellen ved HVER kørsel.
--
-- Løsning: erstat det filtrerede IX_..._timestamp med et ufiltreret indeks på
-- dw_timestamp_update med de to clean_code-kolonner som INCLUDE. Så bliver
-- udtrækket et dækkende seek.
--
-- Hvorfor ERSTAT og ikke tilføj: IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp
-- har ingen tilbageværende forbruger. Den eneste watermark-læsning af denne
-- tabel i hele kodebasen er #RefChangedKeys ovenfor, og den kan ikke bruge det
-- filtrerede indeks. Ref-builderens egen checkpoint-forespørgsel læser MAX på
-- arc-tabellerne, ikke på denne. Et ufiltreret supersæt dækker alt det gamle
-- indeks kunne, så at beholde begge ville kun koste skrivearbejde.
--
-- Skriveomkostningen er afgrænset: MERGE'ens WHEN MATCHED er INTERSECT-beskyttet,
-- så dw_timestamp_update kun ændres på reelle ændringer.
--
-- INCLUDE frem for nøglekolonner: forespørgslen projicerer kun de to
-- clean_code-kolonner. Som nøgle ville NVARCHAR(200)+NVARCHAR(500) lægge
-- 1400 bytes i en nonclustered nøgle med en grænse på 1700.
--
-- Opgraderingssti for EKSISTERENDE databaser. På en frisk DB deklarerer basis-
-- DDL'en (tables/ref/01_tables.sql) allerede det nye indeks, og apply_migrations
-- baseliner (stempler uden at køre) i stedet for at migrere — så denne fil rører
-- aldrig en frisk database. Guards holdes alligevel, fordi migrations kører FØR
-- ref-tabellerne oprettes (staging_infra_ref deps på staging_migrations) på en
-- database der ér blevet migreret før.
-- Kan gentages sikkert: hvert trin er guardet.
-- ============================================================================

-- 1) Det ufiltrerede watermark-indeks.
IF OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
         AND name = 'IX_ref_KMDFakturalinjeUNSPSCMatch_delta'
   )
BEGIN
    EXEC('
        CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_delta]
            ON [ref].[KMDFakturalinjeUNSPSCMatch] ([dw_timestamp_update])
            INCLUDE ([clean_code_kreditor], [clean_code_faktura]);
    ');
    PRINT 'Created IX_ref_KMDFakturalinjeUNSPSCMatch_delta';
END
GO

-- 2) Drop det nu overflødige filtrerede indeks. Sker FØRST efter at trin 1 har
--    bekræftet at erstatningen findes, så tabellen aldrig står uden et
--    watermark-indeks — heller ikke hvis migrationen afbrydes mellem trinnene.
IF OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
         AND name = 'IX_ref_KMDFakturalinjeUNSPSCMatch_delta'
   )
   AND EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
         AND name = 'IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp'
   )
BEGIN
    DROP INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp]
        ON [ref].[KMDFakturalinjeUNSPSCMatch];
    PRINT 'Dropped redundant IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp';
END
GO

-- 3) Samme drop under det gamle snake_case-navn. 20260708_rename_90_indexes.sql
--    omdøber normalt til det kanoniske navn før dette punkt, men sp_rename på en
--    tabel omdøber ikke dens indeks, og der er tidligere observeret indeks der
--    overlevede under et før-omdøbning-navn på en deployet server. Uden dette trin
--    ville et sådant indeks blive et permanent forældreløst, redundant filtreret
--    indeks som ingen forespørgsel kan bruge — kun skriveomkostning.
IF OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
         AND name = 'IX_ref_KMDFakturalinjeUNSPSCMatch_delta'
   )
   AND EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
         AND name = 'IX_ref_kmd_fakturalinje_unspsc_match_timestamp'
   )
BEGIN
    DROP INDEX [IX_ref_kmd_fakturalinje_unspsc_match_timestamp]
        ON [ref].[KMDFakturalinjeUNSPSCMatch];
    PRINT 'Dropped legacy-named IX_ref_kmd_fakturalinje_unspsc_match_timestamp';
END
GO
