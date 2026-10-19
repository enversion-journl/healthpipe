-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260911_fhir_encounter_child_backfill
--
-- Armerer det historiske efterslaeb paa Encounter-berigelsen i dette miljoe.
--
-- Sengeophold og fravaer onboardes som child-grains under Encounter. De
-- kontakter, der ALLEREDE staar i Aidbox, baerer ingen sengeophold og intet
-- fravaer, foer de er re-emitteret én gang. Maalt paa dev: 926.059 kontakter
-- har sengeophold i arkivet, og 915.070 af dem findes i forvejen som Encounter.
-- Dertil kommer de kontakter, der er berigt FOER extension-URL'en blev
-- omdoebt: deres flade raekker staar uden enhedskode, indtil de udsendes igen
-- (maalt paa dev: 1.489 raekker paa 1.054 kontakter).
--
-- HVORFOR EN MARKOER OG IKKE DATAARBEJDE HER:
-- Berigelsen skriver FHIR-ressourcer til Aidbox, ikke raekker i MSSQL, saa
-- arbejdet kan ikke udtrykkes i SQL. Migrationen saetter derfor den tilstand,
-- der faar pipelinen til at goere det -- samme mekanik som
-- 20260903_02_reset_vareudlevering_checkpoints, hvor hele virkningen er en
-- aendring i meta.DFMState, som naeste ordinaere koersel handler paa.
--
-- Markoeren er fremskridtet: NULL betyder "efterslaebet mangler, intet gjort",
-- en OBJECT_ID-vaerdi betyder "naaet hertil", og 'DONE' betyder afviklet.
-- fhir_server_encounter_dfm laeser den under
-- (scope='staging', target_type='fhir_resource', schema_name='fhir_server',
--  table_name='Encounter_ChildBackfill') -- samme noegleform DfmManager bruger.
--
-- KOERSEL EFTER DEPLOY (pr. miljoe, én gang):
--   Koer fhir_server_encounter_dfm med child_affected_backfill=True gentagne
--   gange, indtil den melder faerdig. Hver koersel tager én skive i
--   OBJECT_ID-raekkefoelge og staempler den sidste behandlede noegle, saa en
--   afbrudt koersel genoptages. Naar der ikke er mere tilbage, staemples
--   'DONE', og alle senere koersler er no-ops. Encounter-checkpointet roeres
--   ikke, saa den natlige delta er upaavirket undervejs.
--   Runbooken staar i data_lineage/fhir/fhir_encounter_bed_space.yml.
--
-- Indtil efterslaebet er afviklet, afviser den natlige delta sig selv med det
-- faktiske antal beroerte kontakter -- det er vaernet mod at feje hele
-- historikken i ét hug, ikke en fejl i koerslen.
--
-- Idempotent: raekken indsaettes kun, hvis den ikke findes, saa en re-run er en
-- no-op, og en allerede paabegyndt eller afviklet markoer roeres ikke. Det er
-- ikke kosmetik: en MERGE eller en ubetinget DELETE+INSERT ville saette et
-- halvt afviklet efterslaeb tilbage til start eller genaabne et afsluttet.
-- Samme migrationsmappe koerer mod baade staging og DSO, og meta.DFMState
-- findes kun i staging -- derfor OBJECT_ID-gaten, som i praecedensen.
--
-- FRISK DATABASE: apply_migrations sender en uprovisioneret database til
-- baseline_migrations, som staempler filen som anvendt UDEN at udfoere den.
-- Markoerraekken bliver altsaa aldrig indsat i et helt nyt miljoe. Det er
-- korrekt -- men det foelger af tre ting, der passer sammen, og som ikke kan
-- laeses her, saa de staar eksplicit:
--   1. Et nyt miljoe har intet efterslaeb: foerste Encounter-koersel er en
--      full_load (der er intet checkpoint), og sengeophold og fravaer hentes
--      pr. batch uanset mode, saa kontakterne oprettes MED dem fra starten.
--   2. should_advance_checkpoint er sand for full_load, saa den koersel
--      staempler baade Encounter-checkpointet og child-vandmaerket
--      (fhir_server/Encounter_ChildDelta). Vaernet fejrer derfor ikke.
--   3. Forsoeger nogen alligevel at koere efterslaebet der, svarer koerslen
--      "ikke armeret" -- det rigtige svar, for der er intet at hente.
-- Aendres punkt 2 (hvornaar vandmaerket saettes), falder denne kaede, og et
-- nyt miljoe vil staa med en spaerret natlig delta uden en vej ud.
-- ============================================================================

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM [meta].[DFMState]
        WHERE [scope] = 'staging'
          AND [target_type] = 'fhir_resource'
          AND [schema_name] = 'fhir_server'
          AND [table_name] = 'Encounter_ChildBackfill'
    )
    BEGIN
        INSERT INTO [meta].[DFMState]
            ([scope], [target_type], [schema_name], [table_name],
             [checkpoint_value], [last_mode])
        VALUES
            ('staging', 'fhir_resource', 'fhir_server', 'Encounter_ChildBackfill',
             NULL, 'armed');
    END
END
GO
