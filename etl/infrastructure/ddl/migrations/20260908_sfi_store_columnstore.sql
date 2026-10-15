-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.SFI_datapoint_store: overgang til månedspartitioneret clustered columnstore
--
-- Målt på DSO-dev: refreshen af storen kostede 71 -> 277 -> 579 min for tre kvartaler i
-- træk, fordi hver indsat række vedligeholdt fire brede, tilfældigt nøglede indeks i en
-- stadigt større tabel. Storen bliver en clustered columnstore på månedspartitioner med
-- to smalle B-træer; backfill-vinduer skiftes ind med SWITCH (DSO/load/sfi_store_
-- partitioning.py).
--
-- HVAD: en eksisterende, UPARTITIONERET fhir.SFI_datapoint_store droppes (fuldtekstindeks
-- og tomt katalog med), og storens watermarks slettes. Basis-DDL'en
-- tables/fhir/02_sfi_datapoint_store.sql, som infrastrukturjobbet kører EFTER
-- migrationerne, opretter den partitionerede columnstore og arbejdstabellen. Første
-- refresh finder intet watermark og genopbygger storen fra QuestionnaireResponse/
-- Observation via SWITCH-stien. Storen er en ren afledning af tabeller, der allerede
-- ligger i databasen, så intet går tabt; en kopi ville koste det samme som
-- genopbygningen plus dobbelt plads.
--
-- Aftagervisningen fhir.SFI_datapoint er TOM fra migrationen kører, til første refresh er
-- færdig (timer ved 53 mio. rækker). Kør den i et vindue, hvor det er acceptabelt.
--
-- KØRER PÅ BEGGE DATABASER (samme migrations-mappe); på staging er storen tom og ubrugt.
-- Idempotent: vogtet på, at storen findes og IKKE er partitioneret; efter basis-DDL'en
-- springes alt over. Friske databaser baselines og kører ikke filen.
-- ============================================================================

-- Drop den upartitionerede store og dens watermarks. Vogtet på, at storen
-- findes og IKKE ligger på et partitionsskema (efter basis-DDL'en gør den, og trinnet
-- springes over). Fuldtekstindekset droppes eksplicit først, så et fejlslag her ikke
-- efterlader et halvt trin; watermarks slettes sidst, så de kun forsvinder sammen med
-- tabellen.
IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.indexes i
       JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
       WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
         AND i.index_id IN (0, 1)
   )
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.fulltext_indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
    )
    BEGIN
        EXEC(N'DROP FULLTEXT INDEX ON [fhir].[SFI_datapoint_store];');
        PRINT 'Droppede fuldtekstindekset på fhir.SFI_datapoint_store';
    END

    DROP TABLE [fhir].[SFI_datapoint_store];
    PRINT 'Droppede den upartitionerede fhir.SFI_datapoint_store; basis-DDL opretter den partitionerede store';

    IF OBJECT_ID(N'[meta].[SyncWatermarks]', 'U') IS NOT NULL
    BEGIN
        DELETE FROM [meta].[SyncWatermarks]
        WHERE [schema_name] = N'sfi_datapoint';
        PRINT 'Slettede sfi_datapoint-watermarks; første refresh genopbygger storen';
    END
END
GO

-- Fuldtekstkataloget droppes, hvis det ikke længere bærer indeks. Kataloget var oprettet
-- AS DEFAULT, så et fremmed indeks kan ligge i det; vogteren beskytter det.
IF EXISTS (
    SELECT 1
    FROM sys.fulltext_catalogs c
    WHERE c.name = N'ftc_SFI_datapoint'
      AND NOT EXISTS (
          SELECT 1 FROM sys.fulltext_indexes fi
          WHERE fi.fulltext_catalog_id = c.fulltext_catalog_id
      )
)
BEGIN
    DROP FULLTEXT CATALOG [ftc_SFI_datapoint];
    PRINT 'Droppede det tomme fuldtekstkatalog ftc_SFI_datapoint';
END
GO
