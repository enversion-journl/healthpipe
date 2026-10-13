-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260903_01_reset_vareudlevering_checkpoints
--
-- pre_stg_Vareudlevering_ils byggede clean_code_Lager med
-- zero-padding (map.Lager er rå) og clean_code_Rekvirent med
-- RIGHT('0000' + rnr, 4), som trunkerer rnr længere end 4 tegn. Begge
-- nøgleformer ramte derfor -1 i fct.Vareudlevering selvom map-medlemmerne
-- fandtes.
--
-- Nøgleudtrykkene er rettet, men de akkumulerede -1 skal reposteres.
-- pre_stg_Vareudlevering_ils læser MIN af DFM-checkpointet for
-- dim.Vareudlevering og fct.Vareudlevering; når begge rækker er væk, læser
-- næste ordinære kørsel hele arkivet igen, og fct.Vareudlevering's MERGE
-- opdaterer dw_ek_Lager/dw_ek_Rekvirent på de eksisterende rækker (WHEN
-- MATCHED ... UPDATE SET) — ingen manuel full_load, ingen delete/reload.
--
-- Arkiv- og extract-checkpoints røres ikke: arc.ils_udlev er uændret, det er
-- kun nøgleudledningen ovenpå arkivet der var forkert.
--
-- Idempotent: rækkerne genskabes af næste kørsel, og en gentagen DELETE på et
-- tomt sæt er en no-op. Samme migrationsmappe kører mod både staging og DSO,
-- og meta.DFMState findes kun i staging — derfor OBJECT_ID-gaten.
--
-- ---------------------------------------------------------------------------
-- Forældreløse map/dim-medlemmer: håndteres af [internal ticket], ikke her
--
-- clean_code_Rekvirent er gået fra zero-padded til rå rnr, så de 542
-- master-rækker med rnr kortere end 4 tegn får et nyt map-medlem. Det gamle
-- padded medlem bliver forældreløst: map.Rekvirent er INSERT-only, og
-- dim_Rekvirent's MERGE har ingen WHEN NOT MATCHED BY SOURCE (og dim.Rekvirent
-- har ingen dw_is_deleted), så den gamle dim-række bliver også stående, og
-- entiteten optræder derefter to gange i dimensionsvisningerne.
--
-- Det ryddes IKKE op herfra. En oprydning i denne migration ville være
-- ordensafhængig: migrationer kører én gang fra staging_migrations-asset'et og
-- registreres i meta.MigrationHistory, dvs. ved deploy FØR første ordinære
-- kørsel har mintet de rå medlemmer og reposteret fakta. En NOT EXISTS-guard
-- ville derfor gøre snippet'et til en permanent no-op, og uden guard ville den
-- fjerne medlemmer som fakta stadig peger på.
--
-- [internal ticket] ("Reload ERP") tømmer hele DSA-laget og nulstiller checkpoints, og
-- fjerner dermed de ophobede forældreløse medlemmer og dim-dubletter fra alle
-- nøgleformsændringer på én gang — også dem fra [internal ticket] og [internal ticket].
-- Bemærk at DSO-kopierne skal med: dim_Rekvirent synkroniseres med
-- mode="watermark", som ikke propagerer deletes.
--
-- ============================================================================

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
BEGIN
    DELETE FROM [meta].[DFMState]
    WHERE [scope] = 'staging'
      AND [table_name] = 'Vareudlevering'
      AND (
            ([target_type] = 'dimension' AND [schema_name] = 'dim')
         OR ([target_type] = 'fact'      AND [schema_name] = 'fct')
      );
END
GO
