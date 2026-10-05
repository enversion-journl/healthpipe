-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- FHIR-kontaktrelation på dm's HAIBA-case-tabeller
--
-- Én rolle = én SOR-kolonne på casen, og hver rolle får sin egen relation og
-- sin egen matchkvalitet. Kvaliteten er ikke pynt: matchraten falder fra
-- ~68% (2022+) til 0% i 2020, fordi Cosmic ikke var udrullet. En opgørelse
-- der ikke filtrerer på kvalitet, vil læse EPJ-udrulningen som et fald i
-- infektioner. Se DSA/SQL/dm/haiba_kontakt.py.
--
-- match_dage_* er fortegnsbærende: negativ når casen ligger før kontakten,
-- positiv når den ligger efter, 0 når den ligger indeni. Der er bevidst ikke
-- hardkodet et tolerancevindue — case-definitionerne ejes af infektionshygiejne,
-- ikke af ETL'en.
--
-- Landes både her og i tables/haiba/01_dm.sql.
-- Kvalitetskolonnerne er NOT NULL DEFAULT 'ikke_koert', så en række aldrig kan
-- læses som "intet match" alene fordi relationen endnu ikke er beregnet.
-- ============================================================================

IF OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Cases_Infektion]', 'match_kvalitet_kontaktansvar') IS NULL
BEGIN
    ALTER TABLE [dm].[HAIBA_Cases_Infektion] ADD
        [encounter_id_kontaktansvar]     NVARCHAR(255) NULL,
        [match_kvalitet_kontaktansvar]   VARCHAR(20)   NOT NULL
            CONSTRAINT [DF_dm_HCI_Kval_Kontaktansvar] DEFAULT ('ikke_koert'),
        [match_dage_kontaktansvar]       INT           NULL,
        [encounter_id_opholdsadresse]    NVARCHAR(255) NULL,
        [location_id_opholdsadresse]     NVARCHAR(255) NULL,
        [match_kvalitet_opholdsadresse]  VARCHAR(20)   NOT NULL
            CONSTRAINT [DF_dm_HCI_Kval_Opholdsadresse] DEFAULT ('ikke_koert'),
        [match_dage_opholdsadresse]      INT           NULL;
END
GO

IF OBJECT_ID(N'[dm].[HAIBA_Cases_SSI]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Cases_SSI]', 'match_kvalitet_producent') IS NULL
BEGIN
    ALTER TABLE [dm].[HAIBA_Cases_SSI] ADD
        [encounter_id_producent]                NVARCHAR(255) NULL,
        [match_kvalitet_producent]              VARCHAR(20)   NOT NULL
            CONSTRAINT [DF_dm_HCS_Kval_Producent] DEFAULT ('ikke_koert'),
        [match_dage_producent]                  INT           NULL,
        [encounter_id_kontaktansvarlig]         NVARCHAR(255) NULL,
        [match_kvalitet_kontaktansvarlig]       VARCHAR(20)   NOT NULL
            CONSTRAINT [DF_dm_HCS_Kval_Kontaktansvarlig] DEFAULT ('ikke_koert'),
        [match_dage_kontaktansvarlig]           INT           NULL,
        [encounter_id_reo_producent]            NVARCHAR(255) NULL,
        [match_kvalitet_reo_producent]          VARCHAR(20)   NOT NULL
            CONSTRAINT [DF_dm_HCS_Kval_ReoProducent] DEFAULT ('ikke_koert'),
        [match_dage_reo_producent]              INT           NULL,
        [encounter_id_reo_kontaktansvarlig]     NVARCHAR(255) NULL,
        [match_kvalitet_reo_kontaktansvarlig]   VARCHAR(20)   NOT NULL
            CONSTRAINT [DF_dm_HCS_Kval_ReoKontaktansvarlig] DEFAULT ('ikke_koert'),
        [match_dage_reo_kontaktansvarlig]       INT           NULL;
END
GO

-- Konsumenter filtrerer typisk "eksakte matches i et årsinterval".
IF OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = N'IX_dm_HAIBA_Cases_Infektion_Kontakt'
                     AND object_id = OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion]'))
BEGIN
    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_Infektion_Kontakt]
        ON [dm].[HAIBA_Cases_Infektion] ([match_kvalitet_kontaktansvar])
        INCLUDE ([encounter_id_kontaktansvar], [HAI_date], [dw_ek_Borger]);
END
GO
