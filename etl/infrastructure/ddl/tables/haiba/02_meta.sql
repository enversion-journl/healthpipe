-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Styretabel: HAIBA-leverancer
--
--   meta.HAIBALeverance — hvilken leverance arkivet står på, pr. kildetabel.
--
-- Hvorfor den findes: "hvilken leverance er arkivet nået til" er en egenskab
-- ved TABELLEN, ikke ved rækken. Blev den udledt af MAX(arc.leverance_dato),
-- skulle den kolonne skrives om på hver række hver uge for at holde sig
-- ajour — 6,7 mio. UPDATE'er for at bære ét tal. Arkivet opdaterer derfor kun
-- leverance_dato når rækkens værdier faktisk ændrer sig (se
-- change_detect_columns i DSI/csv_files/haiba_archive.py), og vandmærket bor
-- her i stedet.
--
-- Det er en styretabel, ikke en arkivtabel: den spejler ingen kildefil, og
-- den indgår ikke i MERGE'en. Derfor meta og ikke arc.
--
-- guard_delivery_direction læser MAX(leverance_dato) herfra for at nægte at
-- merge en ældre leverance oven på et nyere arkiv. En backfill indsætter en
-- ældre dato, men flytter aldrig MAX, så vandmærket kan kun gå fremad.
--
-- Én række pr. (kildetabel, leverance_dato) — en genkørsel af samme leverance
-- opdaterer tallene frem for at tilføje en række. Rækkerne akkumulerer, så
-- historikken kan besvare "hvilke leverancer har vi indlæst, og hvornår", og
-- raekker_soft_slettet gør januar-klippet (et helt kalenderår ruller ud af
-- SDS' vindue) synligt som data frem for som en note i en docstring.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'HAIBALeverance'
)
BEGIN
    CREATE TABLE [meta].[HAIBALeverance] (
        -- Uden skemaprefiks: 'haiba_agg_infektion' er både ext- og arc-tabellen.
        [kildetabel]            NVARCHAR(50)  NOT NULL,
        [leverance_dato]        DATE          NOT NULL,
        -- 'merge' (normal kørsel) eller 'backfill' (genskabt historik,
        -- indsat med dw_is_deleted = 1).
        [indlaesningstype]      NVARCHAR(20)  NOT NULL,
        [raekker_indsat]        BIGINT        NULL,
        [raekker_opdateret]     BIGINT        NULL,
        [raekker_soft_slettet]  BIGINT        NULL,
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_HAIBALeverance_dw_timestamp_insert] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_HAIBALeverance_dw_timestamp_update] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        CONSTRAINT [PK_haiba_leverance]
            PRIMARY KEY CLUSTERED ([kildetabel], [leverance_dato])
    );
END
GO
