-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Encounter-berigelsen tages med af det ordinaere delta: child-vandmaerket
-- nulstilles med meta.usp_dfm_reset
--
-- Kontakterne ligger allerede i Aidbox, men uden sengeophold og fravaer. Det
-- ordinaere Encounter-delta tager en kontakt, naar dens egen raekke er aendret
-- ELLER naar et sengeophold/fravaer er aendret efter child-vandmaerket
-- (meta.DFMState: fhir_server / Encounter_ChildDelta). Nulstilles vandmaerket
-- til 1900, tager naeste ordinaere koersel derfor alle kontakter med
-- sengeophold eller fravaer og genposterer dem beriget -- den vanlige vej, som
-- 20260812_fhir_patient_address_typed_period_columns og
-- 20260813a_borger_checkpoint_null_and_protection_flag ogsaa gaar.
--
-- Vaernet i fhir_server_encounter_dfm (CHILD_TOUCHED_ENCOUNTERS_CEILING)
-- lader et vandmaerke med last_mode = 'reset' passere, fordi en nulstilling er
-- en bevidst og logget handling -- ikke den utilsigtede flodboelge fra et
-- genindlaest arkiv, som loftet skal fange. Efter koerslen staar vandmaerket
-- med last_mode = 'delta', og loftet gaelder igen.
--
-- Tre tilfaelde paa den database, hvor DFM-tilstanden bor:
--   * Er efterslaebets markoer (fhir_server / Encounter_ChildBackfill)
--     stemplet DONE, er kontakterne beriget, og intet roeres.
--   * Har Encounter-assetet koert, nulstilles vandmaerket, og markoeren
--     stemples DONE, fordi det ordinaere delta nu er den vej, berigelsen
--     tages med ad.
--   * Har Encounter-assetet IKKE koert, er dets foerste koersel en fuld
--     indlaesning, der beriger alt; kun markoeren stemples DONE.
-- I alle tre er det flagstyrede efterslaeb (child_affected_backfill=True)
-- dermed en no-op bagefter.
--
-- Filen koerer paa begge databaser, men DFM-tilstanden for fhir_server bor paa
-- staging. Nulstillingen er derfor hegnet paa, at Encounter-checkpointet findes
-- (assetet har koert her), saa den er en no-op paa DSO og paa en tom database.
-- Samme moenster som 20260812: dynamisk SQL og en hoej fejl, hvis tilstanden
-- findes, men proceduren mangler.
-- ============================================================================
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL
    THROW 50012,
        N'meta.DFMState findes, men meta.usp_dfm_reset gør ikke - materialisér staging_infra_dfm før denne migration. Uden nulstillingen bliver kontakterne aldrig beriget med sengeophold og fravær.',
        1;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NULL
    PRINT 'Ingen meta.DFMState på denne database (forventet på DSO) - intet vandmærke at nulstille.';
ELSE
    EXEC sp_executesql N'
        DECLARE @reason NVARCHAR(200) = N''Encounter beriget med sengeophold og fravær: alle kontakter med child-rækker genposteres af det ordinære delta.'';

        IF NOT EXISTS (SELECT 1 FROM [meta].[DFMState]
                       WHERE [scope] = ''staging'' AND [target_type] = ''fhir_resource''
                         AND [schema_name] = ''fhir_server'' AND [table_name] = ''Encounter'')
        BEGIN
            -- Encounter har aldrig koert her: dets foerste koersel har intet
            -- checkpoint og er derfor en fuld indlaesning, der beriger alle
            -- kontakter. Markoeren stemples DONE, saa det flagstyrede efterslaeb
            -- ogsaa er en no-op her og ikke koerer historikken en gang til.
            IF EXISTS (SELECT 1 FROM [meta].[DFMState]
                       WHERE [scope] = ''staging'' AND [target_type] = ''fhir_resource''
                         AND [schema_name] = ''fhir_server'' AND [table_name] = ''Encounter_ChildBackfill''
                         AND ISNULL([checkpoint_value], '''') <> ''DONE'')
                EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir_server'', @table_name = ''Encounter_ChildBackfill'',
                     @new_value = ''DONE'', @reason = @reason, @scope = ''staging'';
            PRINT ''Encounter-assetet har ikke kørt på denne database - dets første kørsel er en fuld indlæsning.'';
        END
        ELSE IF EXISTS (SELECT 1 FROM [meta].[DFMState]
                        WHERE [scope] = ''staging'' AND [target_type] = ''fhir_resource''
                          AND [schema_name] = ''fhir_server'' AND [table_name] = ''Encounter_ChildBackfill''
                          AND [checkpoint_value] = ''DONE'')
            PRINT ''Kontakterne er allerede beriget (Encounter_ChildBackfill = DONE) - intet at nulstille.'';
        ELSE
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir_server'', @table_name = ''Encounter_ChildDelta'',
                 @new_value = ''1900-01-01 00:00:00'', @reason = @reason, @scope = ''staging'';
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir_server'', @table_name = ''Encounter_ChildBackfill'',
                 @new_value = ''DONE'', @reason = @reason, @scope = ''staging'';
            PRINT ''Child-vandmærket nulstillet til 1900-01-01; næste ordinære kørsel beriger alle kontakter med sengeophold/fravær.'';
        END';
GO
