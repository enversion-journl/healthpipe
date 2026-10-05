-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.*.dw_is_deleted: NOT NULL DEFAULT 0 + ensartet datatype
--
-- Fortsaetter arbejdet fra 20260820_fhir_patient_is_deleted_not_null.sql, som
-- kun tog fhir.Patient og eksplicit lod resten ligge. Her strammes de otte
-- resterende nullable kolonner (sektion 1-4), datatypen samles paa INT, og
-- kolonnen tilfoejes de tretten tabeller der slet ikke havde den (sektion 5).
-- Efter denne migration har ALLE 24 fhir.*-tabeller dw_is_deleted
-- INT NOT NULL DEFAULT 0.
--
-- Hvorfor NOT NULL: NULL i et slettet-flag er en faelde, ikke kosmetik. Et helt
-- almindeligt `WHERE dw_is_deleted = 0` giver UNKNOWN for NULL og dropper
-- raekken lydloest. Paa fhir.Encounter_Location og fhir.Location er ALLE
-- raekker NULL i dag (viewet projicerer ikke feltet, og kolonnen havde ingen
-- DEFAULT), saa `= 0` returnerer intet overhovedet. Konsumenter har maattet
-- skrive ISNULL(dw_is_deleted, 0) = 0 udenom -- se DSA/SQL/dm/haiba_kontakt.py.
--
-- Hvorfor INT og ikke BIT: vaerdien kommer fra extension(...).valueInteger i
-- SQL-on-FHIR-viewene, ti af elleve tabeller er allerede INT, og alle
-- data_lineage/fhir/*.yml dokumenterer INT. BIT ville kraeve ALTER COLUMN paa
-- ti tabeller (heraf en paa ~20 mio. raekker) plus drop/genskab af det
-- filtrerede unique index UQ_FHIR_Organization_IdentifierValueSor_Live.
-- Kun fhir.Patient konverteres (BIT -> INT).
--
-- RAEKKEFOELGE mod koden: denne migration og kodeaendringerne hoerer til SAMME
-- deployment. Asset-grafen sekventerer dem selv:
--   staging_migrations -> staging_infra_fhir -> fhir_sof_infrastructure
--   (PUT ViewDefinition + $materialize) -> SoF-assets
-- og tilsvarende dso_migrations -> dso_infra_fhir paa DSO. Migrationen koerer
-- altsaa FOER Aidbox rematerialiserer viewet, hvilket er det der goer
-- omdoebningen i sektion 1 sikker: bliver infra-jobbet ikke koert, er BAADE det
-- materialiserede view og MSSQL-kolonnen den gamle (is_deleted), og loadet
-- virker fortsat.
--
-- FAELDE -- det daglige SoF-job koerer IKKE migrationerne:
-- fhir_sof_daily_job og fhir_sof_monthly_job selecter praecis
-- {staging_infra_fhir, fhir_sof_infrastructure} + deres asset-gruppe (se
-- jobs/fhir_sof_jobs.py). staging_migrations er IKKE med, og Dagster koerer
-- ikke uselecterede upstream-assets. staging_infra_fhir er desuden
-- CREATE TABLE IF NOT EXISTS -- den omdoeber ingenting paa en eksisterende
-- tabel.
--
-- Deployer man denne branch og lader det daglige SoF-job koere UDEN foerst at
-- koere full_infrastructure_setup, sker foelgende: fhir_sof_infrastructure
-- rematerialiserer viewet i Aidbox, saa det emitterer dw_is_deleted, mens MSSQL
-- stadig har is_deleted -- og MERGE-statementet doer paa "Invalid column name".
-- _validate_sof_columns fanger det ikke; den validerer kun primaernoeglerne.
--
-- SIKKER RAEKKEFOELGE ved deployment:
--   1. full_infrastructure_setup (eller som minimum staging_migrations og
--      dso_migrations) -- FOER naeste SoF-job.
--   2. Derefter fhir_sof_daily_job / fhir_sof_monthly_job som normalt.
--   3. Se "EFTER MIGRATIONEN" nedenfor om Observation og Procedure.
--
-- Kodeaendringer i samme deployment:
--   * documentreference_view.json: is_deleted -> dw_is_deleted
--   * observation_view.json + procedure_view.json: dw_is_deleted tilfoejet
--     (resourcerne baerer allerede urn:fhir:isDeleted)
--   * sof_utils.py: dw_is_deleted i boolean_false_default_columns for
--     encounter/condition/episode_of_care/servicerequest/appointment/
--     documentreference/observation/procedure (fallback til 0 naar
--     isDeleted-extensionen mangler)
--
-- DRIFTSNOTE -- omkostning og transaktionsstoerrelse:
-- execute_sql() koerer HELE denne fil i én transaktion (autocommit=False, ét
-- commit til sidst). Det har to konsekvenser, som UPDATE TOP-loekkerne
-- IKKE afhjaelper:
--   * Loggen vokser med hele filens arbejde -- loekken deler statements op,
--     men intet committes undervejs.
--   * Laase frigives foerst ved slut-commit, og 100k raekker pr. statement er
--     langt over grænsen for laasoptrapning, saa der tages bordlaas alligevel.
-- Loekken er der altsaa kun for at holde det enkelte statement (og dets plan)
-- i en fornuftig stoerrelse -- ikke for at begraense laase eller log.
--
-- EDITION OG OMKOSTNING -- maalt 2026-08-25, ikke antaget:
-- Dev DSA (og iflg. drift ogsaa Dev DSO og QA DSA) koerer *Developer Edition*
-- (EngineEdition 3, 16.0.4255.1 CU25), ikke Standard. QA DSO er Enterprise.
-- Developer har alle Enterprise-features, saa
-- "ADD kolonne NOT NULL DEFAULT" er METADATA-ONLY i alle fire databaser:
-- eksisterende raekker skrives IKKE. Verificeret med
-- sys.system_internals_partition_columns.has_default = 1 og 0 ms for 200.000
-- raekker.
--
-- Derfor koster sektion 4 reelt ingenting, og den faktiske omkostning er
-- backfill + Patient-typeskiftet (BIT -> INT aendrer kolonnestoerrelsen og
-- omskriver derfor raekkerne uanset edition):
--
--   database    backfill      Patient BIT->INT    i alt
--   QA DSA        35,8 mio.          2,0 mio.     ca. 38 mio.
--   Dev DSA       20,0 mio.          2,0 mio.     ca. 22 mio.
--   Dev DSO       20,0 mio.          2,0 mio.     ca. 22 mio.
--   QA DSO      (ikke maalt endnu -- Enterprise, saa samme moenster)
--
-- Backfillen er den ENESTE store post, og den kan for-koeres i autocommit
-- (se nedenfor). Gennemfoert saadan er migrationen naesten ren metadata plus
-- en 2 mio. raekkers Patient-omskrivning.
--
-- ADVARSEL om PROD: Developer Edition er ikke licenseret til produktion, saa
-- prod er sandsynligvis Standard -- og DER skriver ADD COLUMN hver raekke.
-- Koer edition/adfaerds-proben paa prod FOER du sizer den koersel; med
-- prod-raekketal bliver ADD COLUMN-delen den dominerende post igen.
--
-- Tungest pr. tabel i backfillen: Encounter_Location (~20 mio. i alle tre
-- maalte databaser), dernaest Encounter (11,3 mio. paa QA DSA, 0 paa Dev DSA).
--
-- DSO er ikke et spejl af DSA: Dev DSO har 3.989 NULL paa fhir.Encounter hvor
-- Dev DSA har 0, og Encounter_Location afviger med et par tusind raekker.
-- Hver database skal sizes og koeres for sig.
--
-- Tabellerne staar i vidt forskellig tilstand pr. database -- derfor de
-- metadata-drevne sweeps frem for navnelister:
--   fhir.DocumentReference:  Dev (DSA+DSO) har is_deleted (NULL-bar, ingen
--                            default); QA DSA har SLET INGEN kolonne
--   fhir.Patient:            Dev bit, NOT NULL, MED default (ALTER COLUMN
--                            kraever at defaulten droppes foerst)
--                            QA DSA bit, NULL-bar, UDEN default, 2 NULL-raekker
--   4 child-tabeller:        Dev har kolonnen med default; QA DSA har den ikke
--
-- IKKE beroert -- tabeller i fhir-skemaet som ikke staar i base-DDL:
-- fhir.SFI_datapoint_store (18,1 mio. raekker paa Dev DSO), EncounterLocations
-- (49.034), encounter_locations, og de tomme CarePlanActivity*/CarePlanAlle*.
-- Sektion 4 naevner dem ikke, og sektion 5 rammer dem ikke, da de ikke har
-- kolonnen.
--
-- Backfillen er idempotent (WHERE ... IS NULL) og kan derfor koeres i forvejen
-- i autocommit -- og da ADD COLUMN er metadata-only i de maalte databaser, er
-- backfillen den ENESTE store post. For-koerer man den, er der reelt kun
-- Patient-omskrivningen (2 mio.) tilbage i transaktionen.
--
-- Paa en Standard-instans (fx sandsynligvis prod) gaelder det ikke: der kan
-- ADD COLUMN ikke for-koeres, og gennemskrivningen ligger uundgaaeligt i
-- migrationens egen transaktion. Koer proben foerst og planlaeg vinduet efter
-- resultatet.
--
-- For de tabeller det gaelder, finder migrationen bagefter 0 raekker, og kun
-- DDL-delen ligger i transaktionen:
--
--   DECLARE @rows INT = 1;
--   WHILE @rows > 0
--   BEGIN
--       UPDATE TOP (100000) [fhir].[Encounter_Location]
--       SET [dw_is_deleted] = 0 WHERE [dw_is_deleted] IS NULL;
--       SET @rows = @@ROWCOUNT;
--   END
--
-- Idempotent + guarded (safe to re-run):
--   * Alle blokke er wrappet i IF OBJECT_ID(...) IS NOT NULL.
--   * Kolonne-navngivende statements koeres via EXEC(N'...'), saa batchen kan
--     kompileres selv naar kolonnen endnu hedder noget andet.
--   * ALTER COLUMN ... NOT NULL guardes paa sys.columns.is_nullable = 1.
--   * ADD DEFAULT guardes paa NOT EXISTS mod sys.default_constraints.
--   * sp_rename guardes paa at kildenavnet findes og maalnavnet ikke goer.
--
-- EFTER MIGRATIONEN -- paakraevet ad hoc full load af Observation og Procedure:
-- Sektion 5 tilfoejer kolonnen med DEFAULT 0, saa ALLE eksisterende raekker i
-- fhir.Observation og fhir.Procedure paastaar "live" -- ogsaa de IFDB-slettede.
-- Et delta-load roerer ikke uaendrede raekker, saa de bliver staaende paa 0.
-- Indtil et full load er koert, er `WHERE dw_is_deleted = 0` FORKERT paa netop
-- de to tabeller, og konsumenter skal ikke stole paa flaget der.
--
-- Et full load retter det fuldstaendigt, fordi begge resourcer saetter
-- urn:fhir:isDeleted UBETINGET (altid extension[0], vaerdi 0 eller 1 fra kildens
-- slettet-flag -- observation_sfi_builder / observation_dict_builder og
-- procedure_aktivitetsresultat_builder / Procedure_IFDB_AdminProc_dfm /
-- Procedure_IFDB_RisYdelse_dfm). Koer derfor Observation- og Procedure-kaeden
-- med mode="full_load" efter migrationen.
--
-- Fresh DB: tables/fhir/01_tables.sql opretter kolonnerne NOT NULL DEFAULT 0
-- direkte, og denne fil bliver baselinet (stemplet, ikke koert).
-- ============================================================================

-- === 1. fhir.DocumentReference: is_deleted -> dw_is_deleted ===
--
-- Tabellen blev overset i navnesweepet 20260708_rename_05_fhir_audit.sql, som
-- omdoebte is_deleted -> dw_is_deleted paa de oevrige otte tabeller.
--
-- Guarden bruger COL_LENGTH (identifier-opslag), ikke strengsammenligning med
-- en hardcodet collation: miljoeerne har ikke samme collation. Sweepet i
-- 20260708 havde brug for en case-sensitiv sammenligning, fordi det omdoebte
-- case-only (DW_Timestamp_Insert -> dw_timestamp_insert); is_deleted ->
-- dw_is_deleted adskiller sig ved mere end case, saa det behov findes ikke her.
-- Samme idiom som 20260629_fhir_encounter_location_add_is_deleted.sql.

IF OBJECT_ID(N'[fhir].[DocumentReference]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference]', N'is_deleted') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC sp_rename 'fhir.DocumentReference.is_deleted', 'dw_is_deleted', 'COLUMN';
END
GO

-- === 2. Backfill NULL -> 0 ===
--
-- ÉN GO-batch PR. TABEL, ikke en cursor-loekke over alle. Det er med vilje:
-- paa Dev DSA konvergerede sektion 4 (én batch pr. tabel) alle tabeller, mens
-- de daværende loekke-sektioner naaede én tabel og stoppede -- migrationen
-- rapporterede alligevel succes. En afbrudt batch tager alt efter afbrydelsen
-- med sig; uafhaengige batches goer ikke. Derfor er hver tabel sin egen batch,
-- hver med sin egen guard, saa delvis udfoerelse aldrig kan skjule sig.
--
-- Batchet UPDATE holder det enkelte statement i en fornuftig stoerrelse. Det
-- begraenser IKKE laase eller log -- se driftsnoten i filhovedet.

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Patient]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Patient]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Patient_Address]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Patient_Address]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Patient_Address]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Encounter]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Encounter]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Encounter_Location]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Encounter_Location]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Encounter_Location]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[DocumentReference]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[DocumentReference]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[DocumentReference_Recipient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference_Recipient]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[DocumentReference_Recipient]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Condition]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Condition]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition_tillaegskode]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Condition_tillaegskode]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Observation]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Observation]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[EpisodeOfCare]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[EpisodeOfCare]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Location]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Location]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Location]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Organization]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Organization]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[CarePlan]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[CarePlan_Activity]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[ServiceRequest]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_tillaegskode]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[ServiceRequest_tillaegskode]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_Recipient]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[ServiceRequest_Recipient]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Procedure]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure_tillaegskode]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Procedure_tillaegskode]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Questionnaire]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Questionnaire]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Questionnaire_item]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Questionnaire_item]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[QuestionnaireResponse]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[QuestionnaireResponse]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[QuestionnaireResponse_item]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[QuestionnaireResponse_item]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Appointment]', N'dw_is_deleted') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    EXEC(N'
        DECLARE @rows INT = 1;
        WHILE @rows > 0
        BEGIN
            UPDATE TOP (100000) [fhir].[Appointment]
            SET [dw_is_deleted] = 0
            WHERE [dw_is_deleted] IS NULL;
            SET @rows = @@ROWCOUNT;
        END');
END
GO

-- === 3. fhir.Patient: BIT -> INT ===
--
-- Eneste tabel der historisk er BIT. Sektion 2 har backfillet, saa NOT NULL kan
-- saettes uanset om miljoeet havde NULL-raekker (QA DSA havde 2).
--
-- DEFAULT-constrainten skal DROPPES foerst. SQL Server afviser ALTER COLUMN paa
-- en kolonne som en DEFAULT-constraint peger paa:
--   "The object 'DF_Patient_dw_is_deleted' is dependent on column
--    'dw_is_deleted'"
-- Det er praecis forskellen mellem miljoeerne: paa QA DSA er Patient NULL-bar
-- UDEN default, saa ALTER gaar igennem; paa Dev DSA er den NOT NULL MED default,
-- saa ALTER fejler. Sektion 5 tilfoejer defaulten igen bagefter -- cursoren
-- vaelger netop tabeller der mangler en.
--
-- Kolonnen indgaar ikke i noget indeks, saa ALTER COLUMN kan koere direkte naar
-- defaulten er vaek.

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns c
               JOIN sys.types ty ON ty.user_type_id = c.user_type_id
               WHERE c.object_id = OBJECT_ID(N'[fhir].[Patient]')
                 AND c.name = N'dw_is_deleted'
                 AND ty.name = N'bit')
BEGIN
    DECLARE @pat_df SYSNAME, @pat_sql NVARCHAR(MAX);

    SELECT @pat_df = dc.name
    FROM sys.default_constraints AS dc
    JOIN sys.columns AS c
      ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient]')
      AND c.name = N'dw_is_deleted';

    IF @pat_df IS NOT NULL
    BEGIN
        SET @pat_sql = N'ALTER TABLE [fhir].[Patient] DROP CONSTRAINT '
                     + QUOTENAME(@pat_df) + N';';
        EXEC sys.sp_executesql @pat_sql;
    END

    EXEC(N'ALTER TABLE [fhir].[Patient] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

-- === 4. Tilfoej kolonnen hvor den mangler -- ALLE 24 tabeller ===
--
-- Listen er komplet med vilje. Foer var det "de tretten der manglede den", og
-- fhir.DocumentReference faldt igennem: den skulle omdoebes fra is_deleted
-- (sektion 1), men paa QA DSA findes HVERKEN is_deleted eller dw_is_deleted --
-- saa sektion 1 blev en no-op, tabellen stod ikke paa ADD-listen, og den endte
-- helt uden kolonne. Med documentreference_view der nu emitterer dw_is_deleted
-- ville SoF-MERGE-statementet doe paa "Invalid column name".
--
-- Hver blok er guarded paa COL_LENGTH ... IS NULL, saa den er en no-op i de
-- miljoeer hvor kolonnen allerede findes. At naevne alle 24 koster ingenting og
-- fjerner hele fejlklassen "glemte en tabel".
--
-- OMKOSTNING: "ADD kolonne NOT NULL DEFAULT" er metadata-only paa Enterprise OG
-- Developer -- maalt til 0 ms / has_default = 1 paa Dev DSA, saa denne sektion er
-- gratis der. Paa en Standard-instans skrives hver raekke; se filhovedet.
--
-- Constraint names follow DF_<table>_<column> (20260825b_name_default_constraints.sql).
-- Section 3 looks up Patient's default by column, not by a hardcoded name, so it
-- still works after 20260825b has renamed DF_FHIR_Patient_DwIsDeleted.

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Patient]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Patient]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Patient_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Patient_Address]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Patient_Address]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Patient_Address]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Patient_Address_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Encounter]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Encounter]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Encounter_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Encounter_Location]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Encounter_Location]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Encounter_Location]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Encounter_Location_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[DocumentReference]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[DocumentReference]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_DocumentReference_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[DocumentReference_Recipient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference_Recipient]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_DocumentReference_Recipient_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Condition]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Condition_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition_tillaegskode]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Condition_tillaegskode_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Observation]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Observation]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Observation_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[EpisodeOfCare]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_EpisodeOfCare_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Location]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Location]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Location_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Organization]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Organization]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Organization_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[CarePlan]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_CarePlan_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_CarePlan_Activity_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[ServiceRequest]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_ServiceRequest_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_tillaegskode]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_ServiceRequest_tillaegskode_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_Recipient]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_ServiceRequest_Recipient_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Procedure]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Procedure_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure_tillaegskode]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Procedure_tillaegskode_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Questionnaire]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Questionnaire]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Questionnaire_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Questionnaire_item]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Questionnaire_item]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Questionnaire_item_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[QuestionnaireResponse]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_QuestionnaireResponse_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[QuestionnaireResponse_item]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_QuestionnaireResponse_item_dw_is_deleted] DEFAULT 0;');
END
GO

IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Appointment]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [fhir].[Appointment]
           ADD [dw_is_deleted] INT NOT NULL
               CONSTRAINT [DF_Appointment_dw_is_deleted] DEFAULT 0;');
END
GO

-- === 5. DEFAULT + NOT NULL paa alt der har kolonnen ===
--
-- Igen én batch pr. tabel, af samme grund som sektion 2. Hver blok er guarded
-- paa COL_LENGTH, paa NOT EXISTS mod sys.default_constraints og paa
-- sys.columns.is_nullable, saa den er en no-op hvor tilstanden allerede er
-- rigtig -- og uafhaengig af om naboblokken naaede at koere.
--
-- DEFAULT er ikke valgfri: for de views der ikke projicerer feltet naevner
-- INSERT-grenen i SoF-MERGE ikke kolonnen, saa NOT NULL uden DEFAULT = hver
-- INSERT fejler.

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Patient]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Patient]
               ADD CONSTRAINT [DF_Patient_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Patient_Address]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Patient_Address]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Patient_Address]
               ADD CONSTRAINT [DF_Patient_Address_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Encounter]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Encounter]
               ADD CONSTRAINT [DF_Encounter_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Encounter_Location]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Encounter_Location]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location]
               ADD CONSTRAINT [DF_Encounter_Location_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[DocumentReference]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference]
               ADD CONSTRAINT [DF_DocumentReference_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[DocumentReference_Recipient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[DocumentReference_Recipient]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient]
               ADD CONSTRAINT [DF_DocumentReference_Recipient_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Condition]
               ADD CONSTRAINT [DF_Condition_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Condition]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition_tillaegskode]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode]
               ADD CONSTRAINT [DF_Condition_tillaegskode_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Observation]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Observation]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Observation]
               ADD CONSTRAINT [DF_Observation_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Observation] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[EpisodeOfCare]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare]
               ADD CONSTRAINT [DF_EpisodeOfCare_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Location]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Location]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Location]
               ADD CONSTRAINT [DF_Location_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Location]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Location] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Organization]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Organization]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Organization]
               ADD CONSTRAINT [DF_Organization_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Organization] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[CarePlan]
               ADD CONSTRAINT [DF_CarePlan_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity]
               ADD CONSTRAINT [DF_CarePlan_Activity_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest]
               ADD CONSTRAINT [DF_ServiceRequest_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_tillaegskode]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode]
               ADD CONSTRAINT [DF_ServiceRequest_tillaegskode_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_Recipient]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient]
               ADD CONSTRAINT [DF_ServiceRequest_Recipient_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Procedure]
               ADD CONSTRAINT [DF_Procedure_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure_tillaegskode]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode]
               ADD CONSTRAINT [DF_Procedure_tillaegskode_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Questionnaire]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire]
               ADD CONSTRAINT [DF_Questionnaire_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Questionnaire_item]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Questionnaire_item]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item]
               ADD CONSTRAINT [DF_Questionnaire_item_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[QuestionnaireResponse]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse]
               ADD CONSTRAINT [DF_QuestionnaireResponse_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[QuestionnaireResponse_item]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item]
               ADD CONSTRAINT [DF_QuestionnaireResponse_item_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Appointment]', N'dw_is_deleted') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints AS dc
                   JOIN sys.columns AS c
                     ON c.object_id = dc.parent_object_id
                    AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Appointment]')
                     AND c.name = N'dw_is_deleted')
        EXEC(N'ALTER TABLE [fhir].[Appointment]
               ADD CONSTRAINT [DF_Appointment_dw_is_deleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Appointment] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

-- === 6. Slutkontrol: fejl HOEJT hvis noget ikke naaede at konvergere ===
--
-- Tilfoejet efter at en delvist udfoert koersel paa Dev DSA rapporterede succes
-- mens otte tabeller stadig var NULL-bare uden default. En migration der lyver
-- om sit resultat er farligere end en der fejler. Denne batch scanner de 24
-- tabeller og kaster en fejl med navnene, saa apply_migrations ruller tilbage
-- og IKKE stempler filen som anvendt.

DECLARE @ukonvergeret NVARCHAR(MAX) = N'';

SELECT @ukonvergeret = @ukonvergeret + N' ' + t.name
FROM sys.tables AS t
JOIN sys.columns AS c
  ON c.object_id = t.object_id AND c.name = N'dw_is_deleted'
LEFT JOIN sys.default_constraints AS dc
  ON dc.parent_object_id = t.object_id AND dc.parent_column_id = c.column_id
WHERE t.schema_id = SCHEMA_ID(N'fhir')
  AND t.name IN (N'Patient', N'Patient_Address', N'Encounter', N'Encounter_Location', N'DocumentReference', N'DocumentReference_Recipient', N'Condition', N'Condition_tillaegskode', N'Observation', N'EpisodeOfCare', N'Location', N'Organization', N'CarePlan', N'CarePlan_Activity', N'ServiceRequest', N'ServiceRequest_tillaegskode', N'ServiceRequest_Recipient', N'Procedure', N'Procedure_tillaegskode', N'Questionnaire', N'Questionnaire_item', N'QuestionnaireResponse', N'QuestionnaireResponse_item', N'Appointment')
  AND (c.is_nullable = 1 OR dc.object_id IS NULL OR TYPE_NAME(c.user_type_id) <> N'int');

IF @ukonvergeret <> N''
BEGIN
    DECLARE @msg NVARCHAR(2048) =
        N'dw_is_deleted er ikke konvergeret paa:' + @ukonvergeret
        + N' -- migrationen har IKKE gjort sit arbejde faerdigt.';
    RAISERROR(@msg, 16, 1);
END
GO
