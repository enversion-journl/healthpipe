-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Backfill-trigger for lvnr-fanout (spec 2026-08-27-alternate-lvnr-rows):
-- nulstil DFM-checkpoints saa FOERSTE ordinaere koersel efter deploy selv
-- genopbygger fuldt — ingen manuel full_load noedvendig:
--   dim.Leverandoervare + bridge.LeverandoervareILSVare -> hub'ens Vare-akse og
--     bridge-scope ser hele arkivbilledet; alternative lvnr-entiteter/-par
--     backfilles i map.Leverandoervare, dim.Leverandoervare og bridgen.
--   fct.AftaleVarePris -> staging genopbygger alle prisversioner under det nye
--     grain (bk_prrowid x clean_code_AftaleVarePris) og backfiller dw_valid_to;
--     soft delete pensionerer de gamle basisformat-raekker i samme MERGE-pas.
-- Asset-afhaengighederne sikrer raekkefoelgen (map_Leverandoervare foer
-- fct_AftaleVarePris) inden for samme run — nulstillingen sikrer INDHOLDET:
-- en delta-koersel restager ellers intet, da alternativernes arc-raekker har
-- uaendret dw_timestamp_update.
-- Manglende DFMState-raekke = foerste-koersels-semantik (BEGINNING_OF_TIME);
-- raekkerne genoprettes automatisk ved naeste checkpoint-avance. Ingen
-- TRUNCATE: fct-reconciliation sker via MERGE + soft delete (dw_sk-stabilitet
-- mod DSO), og map-tabellernes dw_ek-stabilitet maa aldrig brydes.
-- Kun scope='staging'-raekker beroeres; no-op paa DSO og friske databaser.
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
    EXEC('DELETE FROM meta.DFMState WHERE scope = ''staging'' AND schema_name = ''dim'' AND table_name = ''Leverandoervare''');
GO
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
    EXEC('DELETE FROM meta.DFMState WHERE scope = ''staging'' AND schema_name = ''bridge'' AND table_name = ''LeverandoervareILSVare''');
GO
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
    EXEC('DELETE FROM meta.DFMState WHERE scope = ''staging'' AND schema_name = ''fct'' AND table_name = ''AftaleVarePris''');
GO
