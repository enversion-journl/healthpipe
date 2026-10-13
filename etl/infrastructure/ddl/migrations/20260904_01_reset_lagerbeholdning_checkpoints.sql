-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260904_01_reset_lagerbeholdning_checkpoints
--
-- pre_stg_Lagerbeholdning_ils og pre_stg_AfdelingLagerbeholdning_ils byggede
-- deres clean_code_Lager/clean_code_Rekvirent med hver sin håndkopierede CASE.
-- De er nu flyttet over på det fælles udtryk i
-- DSA/SQL/common/ils_clean_code_shared, hvilket ændrer to nøgleformer:
--
--   * clean_code_Rekvirent i afdlgrbeh gik fra 'RNR_{sgh}_{rnr}' (og
--     'RNR__{rnr}' uden sgh) til 'SGH_{sgh}_RNR_{rnr}'. Kun den sidste form
--     findes i map.Rekvirent, så dw_ek_Rekvirent ramte -1 på hver eneste
--     række i fct.AfdelingLagerbeholdning.
--   * De bare 'LGR_{lgr}'-grene (uden sgh) er væk, og tom/blank kode
--     normaliseres nu til NULL i stedet for at blive en del af nøglen.
--
-- Nøglerne udledes i pre_stg, så de akkumulerede -1 skal reposteres.
-- pre_stg læser MIN af checkpointet for sin dim og sin fct, så begge par
-- fjernes; næste ordinære kørsel genlæser arkivet, og faktatabellernes MERGE
-- opdaterer dw_ek-kolonnerne på de eksisterende rækker.
--
-- Idempotent: rækkerne genskabes af næste kørsel, og en gentagen DELETE på et
-- tomt sæt er en no-op. Samme migrationsmappe kører mod både staging og DSO,
-- og meta.DFMState findes kun i staging — derfor OBJECT_ID-gaten.
-- ============================================================================

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
BEGIN
    DELETE FROM [meta].[DFMState]
    WHERE [scope] = 'staging'
      AND (
            ([target_type] = 'dimension' AND [schema_name] = 'dim'
             AND [table_name] IN ('LagerbeholdningEgenskaber',
                                  'AfdelingLagerbeholdningEgenskaber'))
         OR ([target_type] = 'fact'      AND [schema_name] = 'fct'
             AND [table_name] IN ('Lagerbeholdning',
                                  'AfdelingLagerbeholdning'))
      );
END
GO
