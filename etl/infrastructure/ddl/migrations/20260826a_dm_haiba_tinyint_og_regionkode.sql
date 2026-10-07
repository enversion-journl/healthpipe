-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Opfølgning på dm-splittet (review): case-tabellerne skifter skikkelse
--
--   1. SSI_acute_90/365 skifter BIT -> TINYINT: TRY_CONVERT(BIT, '2') = 1,
--      så konverteringstælleren kan bevisligt ikke se en afvigende numerisk
--      kildekodning bag BIT.
--   2. Measure-kolonnerne på dm.HAIBA_Cases_SSI får kildens navne verbatim
--      (kamme_positive_biopsies -> HAI_kamme_positive_biopsies osv.), samme
--      regel som agg-tabellerne.
--   3. HAI_RegionKode føjes til begge case-egenskabstabeller: den bærer
--      landsaggregat-skellet og er den eneste vej til region når SOR-opslaget
--      lander på -1 — samme princip som RegionKode på agg-siden.
--
-- dm er fuldt genopbyggeligt fra arc, så migrationen dropper parvis i den
-- gamle skikkelse og lader end-state-DDL'en (tables/haiba/02_dm.sql) oprette
-- den nye; første kørsel efter deploy genskaber indholdet. Hver tabel har sin
-- egen shape-test (COL_LENGTH er CI-sikker og uafhængig af collation), så et
-- parvist drop også udløses efter en delvis manuel oprydning:
--   gammel faktatabel:       MANGLER HAI_kamme_positive_biopsies
--   gammel egenskabstabel:   MANGLER HAI_RegionKode
-- ============================================================================

IF (OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion_Egenskaber]', 'U') IS NOT NULL
    AND COL_LENGTH('[dm].[HAIBA_Cases_Infektion_Egenskaber]', 'HAI_RegionKode') IS NULL)
BEGIN
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_Infektion_Egenskaber];
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_Infektion];
END
GO

IF (OBJECT_ID(N'[dm].[HAIBA_Cases_SSI_Egenskaber]', 'U') IS NOT NULL
    AND COL_LENGTH('[dm].[HAIBA_Cases_SSI_Egenskaber]', 'HAI_RegionKode') IS NULL)
   OR (OBJECT_ID(N'[dm].[HAIBA_Cases_SSI]', 'U') IS NOT NULL
    AND COL_LENGTH('[dm].[HAIBA_Cases_SSI]', 'HAI_kamme_positive_biopsies') IS NULL)
BEGIN
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_SSI_Egenskaber];
    DROP TABLE IF EXISTS [dm].[HAIBA_Cases_SSI];
END
GO
