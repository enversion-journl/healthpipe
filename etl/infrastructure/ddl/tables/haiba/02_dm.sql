-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Datamart: HAIBA (dm-skemaet)
--
-- Dimensionelt split: map -> fakta + transaktionsdimension.
--
--   map.HAIBA_*             minter dw_ek pr. clean_code (01_maps.sql)
--   dm.HAIBA_*              FAKTA: kun DW-nøgler + typede measures
--   dm.HAIBA_*_Egenskaber   TRANSAKTIONSDIMENSION: kildens deskriptive og
--                           degenererede kolonner, 1:1 med fakta på dw_ek
--
-- Organisatoriske tekstkolonner (ejerforhold, region, Sundhedsinstitution,
-- Matrikel, OverliggendeEnhed, AfdelingAfsnit) er BEVIDST udeladt: de findes
-- i dim.Organisation via dw_ek_Organisation og skal ikke stå redundant på
-- hver række. Rå CPR findes IKKE i dm — kun dw_ek_Borger. CPR lever i
-- map.HAIBA_Cases_* (clean_code) og arc, som begge er staging-interne.
-- Dermed bortfalder den GDPR-adgangsbegrænsning der lå på det gamle dm-lag.
--
-- Rå SOR-koder og rå datostrenge bevares i _Egenskaber, så en uopløst nøgle
-- (-1) aldrig koster kildeværdien. Opslag: dim.Organisation.kode bærer
-- SOR-koden; sentinel -1 = UNKNOWN (seedes af organisation/02_dims.sql).
-- Tom SOR-kode er et landsaggregat (RegionKode 8888/9999), ikke et fejlet
-- opslag — op til 85 % af rækkerne i Acute_HIP. dw_ek_Borger slås op i
-- ref.PatientToBorger; ukendte CPR oprettes som stub-patienter og
-- resolver ved næste kørsel.
--
-- Datonøgler er dim.Dato's smart key YYYYMMDD via den fælles dato_key-helper
-- (DSA/SQL/common/dato_dimension.py) med alle fire sentinels: -1 ukendt
-- (ikke-blank der ikke kan parses), -2 før dim.Dato's range, 99999998 blank
-- (kilden har strukturelt ingen dato) og 99999999 efter range. BLANK/UNKNOWN-
-- skellet er bevidst: reo_date/HAI_date er strukturelt blanke på rigtige
-- SSI-rækker, og "SDS har ingen dato her" må aldrig læses som et dataproblem.
--
-- dw_is_deleted føres med fra arc på BÅDE fakta og egenskaber (besluttet
-- 2026-08-24): soft-slettede rækker filtreres ikke ud af rebuilden, så
-- revisionssporet (hvad SDS har trukket tilbage) er synligt i konsumlaget.
-- Konsumenter bruger vw_dm.HAIBA_* på DSO (views/haiba/01_vw_dm.sql), der
-- filtrerer dw_is_deleted = 0 — den sikre vej er default; revisionssporet
-- læses fra dm-tabellerne direkte.
--
-- Measure-typer er fastlagt ud fra leverancedata (risikodoegn har decimaler
-- med punktum, fx '1234.5'); measure-KOLONNER hedder det samme som kilden
-- (antal, risikodoegn, SSI_acute_90 ...) — én regel, ingen omdøbning.
-- SSI_acute_* antages 0/1 men er TINYINT, ikke BIT: TRY_CONVERT(BIT, '2')
-- giver 1, så rebuildens TRY_CONVERT-tæller kan bevisligt ikke se
-- en afvigende numerisk kodning bag BIT — TINYINT holder afvigelsen synlig i
-- selve tallet, indtil kodningen er bekræftet mod en leverance.
--
-- dw_timestamp_insert på dm-tabellerne er SENESTE rebuild-tidspunkt, ikke
-- "først set" — rebuilden er TRUNCATE + INSERT, så alle rækker får nyt
-- timestamp hver kørsel. Først-set ligger i map.HAIBA_*.dw_timestamp_insert
-- (01_maps.sql), som er append-only og dermed stabilt pr. dw_ek.
--
-- Encounter-relationen er delt efter rolle: relations-NØGLERNE
-- (encounter_id_*, location_id_*) bor på FAKTA, så joinet ud til
-- fhir-kontakten er direkte — mens matchets KVALITET (match_kvalitet_*,
-- match_dage_*) bor på _Egenskaber. match_kvalitet_* er NOT NULL DEFAULT
-- 'ikke_koert', så en række aldrig læses som "intet match" alene fordi
-- relationen endnu ikke er beregnet.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- dm.HAIBA_AggInfektion  (BACT, UTI, COHA, HOHA)
-- BEMÆRK: HAI er 'COHA'/'HOHA' her, mens case-tabellerne bruger
-- 'CDI_COHA'/'CDI_HOHA'. Enhver join mellem agg og cases skal mappe.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_AggInfektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_AggInfektion] (
        [dw_ek_HAIBA_AggInfektion] INT           NOT NULL,
        [dw_ek_Organisation]        INT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_ek_Organisation] DEFAULT (-1),
        [dw_ek_Dato]                INT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_ek_Dato] DEFAULT (-1),
        [antal]                     INT           NULL,
        [risikodoegn]               DECIMAL(18,2) NULL,
        [leverance_dato]            DATE          NOT NULL,
        [dw_is_deleted]             BIT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Agg_Infektion]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_AggInfektion])
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Agg_Infektion_Org]
        ON [dm].[HAIBA_AggInfektion] ([dw_ek_Organisation])
        INCLUDE ([dw_ek_Dato], [antal], [risikodoegn], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Agg_Infektion_Dato]
        ON [dm].[HAIBA_AggInfektion] ([dw_ek_Dato]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_AggInfektionEgenskaber
-- Grainet (HAI, HAIBA_dato, Rolle_ansvar, RegionKode, SORIdentifier) bor her
-- som rå kildeværdier. RegionKode beholdes: den bærer landsaggregat-skellet
-- (8888/9999, tom SOR), som rapportering skelner på.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_AggInfektionEgenskaber]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_AggInfektionEgenskaber] (
        [dw_ek_HAIBA_AggInfektion] INT           NOT NULL,
        [HAI]                       NVARCHAR(50)  NOT NULL,
        [HAIBA_dato]                NVARCHAR(30)  NOT NULL,
        [Rolle_ansvar]              NVARCHAR(50)  NOT NULL,
        [RegionKode]                NVARCHAR(10)  NOT NULL,
        [SORIdentifier]             NVARCHAR(30)  NOT NULL,
        [leverance_dato]            DATE          NOT NULL,
        [dw_is_deleted]             BIT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_Egenskaber_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_Egenskaber_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_Egenskaber_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Agg_Infektion_Egenskaber]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_AggInfektion])
    );
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_AggOrtopaedi  (Acute/Elective HIP/KNEE)
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_AggOrtopaedi]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_AggOrtopaedi] (
        [dw_ek_HAIBA_AggOrtopaedi] INT           NOT NULL,
        [dw_ek_Organisation]        INT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_ek_Organisation] DEFAULT (-1),
        [dw_ek_Dato]                INT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_ek_Dato] DEFAULT (-1),
        [antal90]                   INT           NULL,
        [antal365]                  INT           NULL,
        [operationer]               INT           NULL,
        [leverance_dato]            DATE          NOT NULL,
        [dw_is_deleted]             BIT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Agg_Ortopaedi]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_AggOrtopaedi])
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Agg_Ortopaedi_Org]
        ON [dm].[HAIBA_AggOrtopaedi] ([dw_ek_Organisation])
        INCLUDE ([dw_ek_Dato], [antal90], [antal365], [operationer], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Agg_Ortopaedi_Dato]
        ON [dm].[HAIBA_AggOrtopaedi] ([dw_ek_Dato]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_AggOrtopaediEgenskaber
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_AggOrtopaediEgenskaber]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_AggOrtopaediEgenskaber] (
        [dw_ek_HAIBA_AggOrtopaedi] INT           NOT NULL,
        [HAI]                       NVARCHAR(50)  NOT NULL,
        [HAIBA_dato]                NVARCHAR(30)  NOT NULL,
        [Rolle_ansvar]              NVARCHAR(50)  NOT NULL,
        [RegionKode]                NVARCHAR(10)  NOT NULL,
        [SORIdentifier]             NVARCHAR(30)  NOT NULL,
        [leverance_dato]            DATE          NOT NULL,
        [dw_is_deleted]             BIT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_Egenskaber_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_Egenskaber_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_Egenskaber_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Agg_Ortopaedi_Egenskaber]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_AggOrtopaedi])
    );
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_CasesInfektion
-- Ingen egentlige measures — casen selv er tællingen (COUNT(*)).
-- To SOR-roller: opholdsadresse (hvor patienten fysisk var) og kontaktansvar
-- (behandlingsansvarlig enhed). De opløses hver for sig.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_CasesInfektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_CasesInfektion] (
        [dw_ek_HAIBA_CasesInfektion]       INT           NOT NULL,
        [dw_ek_Borger]                      INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_ek_Borger] DEFAULT (-1),
        [dw_ek_Organisation_opholdsadresse] INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_ek_Organisation_opholdsadresse] DEFAULT (-1),
        [dw_ek_Organisation_kontaktansvar]  INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_ek_Organisation_kontaktansvar] DEFAULT (-1),
        [dw_ek_Dato_hai]                    INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_ek_Dato_hai] DEFAULT (-1),
        -- Relations-nøgler til FHIR-kontakten — stemples af
        -- kontakt-assettet efter rebuilden. Kvaliteten bor på _Egenskaber.
        [encounter_id_kontaktansvar]        NVARCHAR(255) NULL,
        [encounter_id_opholdsadresse]       NVARCHAR(255) NULL,
        [location_id_opholdsadresse]        NVARCHAR(255) NULL,
        [leverance_dato]                    DATE          NOT NULL,
        [dw_is_deleted]                     BIT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]               DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]               DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Cases_Infektion]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_CasesInfektion])
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_Infektion_Borger]
        ON [dm].[HAIBA_CasesInfektion] ([dw_ek_Borger]);

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_Infektion_Org]
        ON [dm].[HAIBA_CasesInfektion]
        ([dw_ek_Organisation_kontaktansvar], [dw_ek_Organisation_opholdsadresse]);

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_Infektion_Dato]
        ON [dm].[HAIBA_CasesInfektion] ([dw_ek_Dato_hai]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_CasesInfektionEgenskaber
-- extid/larlabnr er mellemrumsseparerede lab-id-lister uden øvre grænse.
-- Encounter-relationen stemples her af haiba_kontakt-assettet.
-- match_dage_* er fortegnsbærende: negativ før kontakten, positiv efter,
-- 0 indeni. Bevidst intet hardkodet tolerancevindue — case-definitionerne
-- ejes af infektionshygiejne, ikke af ETL'en.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_CasesInfektionEgenskaber]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_CasesInfektionEgenskaber] (
        [dw_ek_HAIBA_CasesInfektion]   INT           NOT NULL,
        [HAI_type]                      NVARCHAR(50)  NOT NULL,
        [HAI_date]                      NVARCHAR(30)  NOT NULL,
        [HAI_Opholdsadresse]            NVARCHAR(30)  NULL,
        [HAI_Kontaktansvar]             NVARCHAR(30)  NULL,
        [extid]                         NVARCHAR(500) NULL,
        [larlabnr]                      NVARCHAR(500) NULL,
        -- Beholdes som eneste org-tekstkolonne (samme princip som RegionKode
        -- på agg): bærer landsaggregat-skellet, og er den eneste vej til
        -- region når SOR-opslaget lander på -1.
        [HAI_RegionKode]                NVARCHAR(10)  NULL,
        [findings1]                     NVARCHAR(500) NULL,
        [findings2]                     NVARCHAR(500) NULL,
        [findings3]                     NVARCHAR(500) NULL,
        [findings4]                     NVARCHAR(500) NULL,
        [findings5]                     NVARCHAR(500) NULL,
        [findings6]                     NVARCHAR(500) NULL,
        [findings7]                     NVARCHAR(500) NULL,
        [match_kvalitet_kontaktansvar]  VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_Egenskaber_match_kvalitet_kontaktansvar] DEFAULT ('ikke_koert'),
        [match_dage_kontaktansvar]      INT           NULL,
        [match_kvalitet_opholdsadresse] VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_Egenskaber_match_kvalitet_opholdsadresse] DEFAULT ('ikke_koert'),
        [match_dage_opholdsadresse]     INT           NULL,
        [leverance_dato]                DATE          NOT NULL,
        [dw_is_deleted]                 BIT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_Egenskaber_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]           DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_Egenskaber_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]           DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_Egenskaber_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Cases_Infektion_Egenskaber]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_CasesInfektion])
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_Infektion_Egenskaber_Kontakt]
        ON [dm].[HAIBA_CasesInfektionEgenskaber] ([match_kvalitet_kontaktansvar]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_CasesSSI  (SSI_HIP, SSI_KNEE)
-- Fire SOR-roller: producent/kontaktansvarlig for indeksoperationen og de
-- tilsvarende for reoperationen. Tre datoer: operation, HAI og reoperation.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_CasesSSI]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_CasesSSI] (
        [dw_ek_HAIBA_CasesSSI]                   INT          NOT NULL,
        [dw_ek_Borger]                            INT          NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Borger] DEFAULT (-1),
        [dw_ek_Organisation_producent]            INT          NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_producent] DEFAULT (-1),
        [dw_ek_Organisation_kontaktansvarlig]     INT          NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_kontaktansvarlig] DEFAULT (-1),
        [dw_ek_Organisation_reo_producent]        INT          NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_reo_producent] DEFAULT (-1),
        [dw_ek_Organisation_reo_kontaktansvarlig] INT          NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_reo_kontaktansvarlig] DEFAULT (-1),
        [dw_ek_Dato_operation]                    INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Dato_operation] DEFAULT (-1),
        [dw_ek_Dato_hai]                          INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Dato_hai] DEFAULT (-1),
        [dw_ek_Dato_reo]                          INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Dato_reo] DEFAULT (-1),
        [HAI_kamme_positive_biopsies]             INT           NULL,
        [HAI_kamme_total_biopsies]                INT           NULL,
        -- TINYINT, ikke BIT: TRY_CONVERT(BIT, '2') = 1, så en afvigende
        -- kildekodning ville være usynlig for konverteringstælleren.
        [SSI_acute_90]                            TINYINT       NULL,
        [SSI_acute_365]                           TINYINT       NULL,
        -- Relations-nøgler til FHIR-kontakten, én pr. rolle —
        -- stemples af kontakt-assettet. Kvaliteten bor på _Egenskaber.
        [encounter_id_producent]                  NVARCHAR(255) NULL,
        [encounter_id_kontaktansvarlig]           NVARCHAR(255) NULL,
        [encounter_id_reo_producent]              NVARCHAR(255) NULL,
        [encounter_id_reo_kontaktansvarlig]       NVARCHAR(255) NULL,
        [leverance_dato]                          DATE          NOT NULL,
        [dw_is_deleted]                           BIT          NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]                     DATETIME2(0) NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]                     DATETIME2(0) NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Cases_SSI]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_CasesSSI])
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_SSI_Borger]
        ON [dm].[HAIBA_CasesSSI] ([dw_ek_Borger]);

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_SSI_Dato_operation]
        ON [dm].[HAIBA_CasesSSI] ([dw_ek_Dato_operation]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_CasesSSIEgenskaber
-- banr er som extid/larlabnr en mellemrumssepareret liste uden øvre grænse
-- (78 tegn observeret i leverance 2026-08-08).
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_CasesSSIEgenskaber]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_CasesSSIEgenskaber] (
        [dw_ek_HAIBA_CasesSSI]               INT           NOT NULL,
        [HAI_type]                            NVARCHAR(50)  NOT NULL,
        [operation_date]                      NVARCHAR(30)  NOT NULL,
        [HAI_date]                            NVARCHAR(30)  NULL,
        [reo_date]                            NVARCHAR(30)  NULL,
        [banr]                                NVARCHAR(500) NULL,
        [larlabnr]                            NVARCHAR(500) NULL,
        [extid]                               NVARCHAR(500) NULL,
        [Index_diagnosis]                     NVARCHAR(50)  NULL,
        [Index_operation]                     NVARCHAR(100) NULL,
        [Index_acute]                         NVARCHAR(10)  NULL,
        [Index_side]                          NVARCHAR(20)  NULL,
        [reo_diagnosis]                       NVARCHAR(50)  NULL,
        [reoperation]                         NVARCHAR(255) NULL,
        [reo_acute]                           NVARCHAR(10)  NULL,
        [reo_side]                            NVARCHAR(20)  NULL,
        [HAI_producent]                       NVARCHAR(30)  NULL,
        [HAI_kontaktansvarlig]                NVARCHAR(30)  NULL,
        [reo_producent]                       NVARCHAR(30)  NULL,
        [reo_kontaktansvarlig]                NVARCHAR(30)  NULL,
        -- Se kommentaren på HAIBA_CasesInfektionEgenskaber.
        [HAI_RegionKode]                      NVARCHAR(10)  NULL,
        [findings1]                           NVARCHAR(500) NULL,
        [findings2]                           NVARCHAR(500) NULL,
        [findings3]                           NVARCHAR(500) NULL,
        [findings4]                           NVARCHAR(500) NULL,
        [findings5]                           NVARCHAR(500) NULL,
        [findings6]                           NVARCHAR(500) NULL,
        [findings7]                           NVARCHAR(500) NULL,
        [findings8]                           NVARCHAR(500) NULL,
        [findings9]                           NVARCHAR(500) NULL,
        [match_kvalitet_producent]            VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_Egenskaber_match_kvalitet_producent] DEFAULT ('ikke_koert'),
        [match_dage_producent]                INT           NULL,
        [match_kvalitet_kontaktansvarlig]     VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_Egenskaber_match_kvalitet_kontaktansvarlig] DEFAULT ('ikke_koert'),
        [match_dage_kontaktansvarlig]         INT           NULL,
        [match_kvalitet_reo_producent]        VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_Egenskaber_match_kvalitet_reo_producent] DEFAULT ('ikke_koert'),
        [match_dage_reo_producent]            INT           NULL,
        [match_kvalitet_reo_kontaktansvarlig] VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_Egenskaber_match_kvalitet_reo_kontaktansvarlig] DEFAULT ('ikke_koert'),
        [match_dage_reo_kontaktansvarlig]     INT           NULL,
        [leverance_dato]                      DATE          NOT NULL,
        [dw_is_deleted]                       BIT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_Egenskaber_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]                 DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_Egenskaber_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]                 DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_Egenskaber_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Cases_SSI_Egenskaber]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_CasesSSI])
    );
END
GO
