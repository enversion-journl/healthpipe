-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Datamart: HAIBA (dm-schemaet)
--
-- Det opløste lag. Kildens egne værdier står side om side med de opslåede
-- DW-nøgler, så en konsument både kan se hvad SDS leverede og hvad det blev
-- opløst til. arc forbliver 1:1 med filerne; dm er det lag der læses.
--
-- Opslaget: dim.Organisation.kode indeholder SOR-koden (kolonnen hedder kode,
-- ikke code). Kan en kode ikke slås op, sættes dw_ek_Organisation = -1 —
-- UNKNOWN-sentinellen, der seedes af organisation/02_dims.sql. Den oprindelige
-- SOR-værdi bevares altid ved siden af nøglen.
--
-- Tom SOR-kode er IKKE en manglende opslagsværdi: SORIdentifier er tom præcis
-- når rækken er et landsaggregat (RegionKode 8888/9999), hvilket er op til
-- 85 % af rækkerne i Acute_HIP. Også de får -1.
--
-- dw_ek_Borger (AB#27833) slås op i ref.PatientToBorger, hvor nøglen mintes.
-- Ukendte borgere får -1. CPR'er som HAIBA kender men FHIR ikke gør, oprettes
-- som stub-patienter af DSA/FHIR/Patient/Patient_HAIBA.py, hvorefter de
-- resolver ved næste kørsel.
--
-- Encounter-relationen tilføjes af AB#27834.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- dm.HAIBA_Agg_Infektion  (BACT, UTI, COHA, HOHA)
-- BEMÆRK: HAI er 'COHA'/'HOHA' her, mens case-tabellerne bruger
-- 'CDI_COHA'/'CDI_HOHA'. Enhver join mellem agg og cases skal mappe.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_Agg_Infektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_Agg_Infektion] (
        [HAI]                  NVARCHAR(50)  NOT NULL,
        [HAIBA_dato]           NVARCHAR(30)  NOT NULL,
        [Rolle_ansvar]         NVARCHAR(50)  NOT NULL,
        [RegionKode]           NVARCHAR(10)  NOT NULL,
        [SORIdentifier]        NVARCHAR(30)  NOT NULL,
        [dw_ek_Organisation]   INT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_ek_Organisation] DEFAULT (-1),
        [ejerforhold]          NVARCHAR(50)  NULL,
        [region]               NVARCHAR(255) NULL,
        [Sundhedsinstitution]  NVARCHAR(255) NULL,
        [Matrikel]             NVARCHAR(255) NULL,
        [OverliggendeEnhed]    NVARCHAR(255) NULL,
        [AfdelingAfsnit]       NVARCHAR(255) NULL,
        [antal]                NVARCHAR(50)  NULL,
        [risikodoegn]          NVARCHAR(50)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Agg_Infektion] PRIMARY KEY CLUSTERED (
            [HAI], [HAIBA_dato], [Rolle_ansvar], [RegionKode], [SORIdentifier]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Agg_Infektion_Org]
        ON [dm].[HAIBA_Agg_Infektion] ([dw_ek_Organisation])
        INCLUDE ([HAI], [HAIBA_dato], [Rolle_ansvar], [RegionKode], [antal]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_Agg_Ortopaedi  (Acute/Elective HIP/KNEE)
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_Agg_Ortopaedi]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_Agg_Ortopaedi] (
        [HAI]                  NVARCHAR(50)  NOT NULL,
        [HAIBA_dato]           NVARCHAR(30)  NOT NULL,
        [Rolle_ansvar]         NVARCHAR(50)  NOT NULL,
        [RegionKode]           NVARCHAR(10)  NOT NULL,
        [SORIdentifier]        NVARCHAR(30)  NOT NULL,
        [dw_ek_Organisation]   INT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_ek_Organisation] DEFAULT (-1),
        [ejerforhold]          NVARCHAR(50)  NULL,
        [region]               NVARCHAR(255) NULL,
        [Sundhedsinstitution]  NVARCHAR(255) NULL,
        [Matrikel]             NVARCHAR(255) NULL,
        [OverliggendeEnhed]    NVARCHAR(255) NULL,
        [AfdelingAfsnit]       NVARCHAR(255) NULL,
        [antal90]              NVARCHAR(50)  NULL,
        [antal365]             NVARCHAR(50)  NULL,
        [operationer]          NVARCHAR(50)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Agg_Ortopaedi] PRIMARY KEY CLUSTERED (
            [HAI], [HAIBA_dato], [Rolle_ansvar], [RegionKode], [SORIdentifier]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Agg_Ortopaedi_Org]
        ON [dm].[HAIBA_Agg_Ortopaedi] ([dw_ek_Organisation]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_Cases_Infektion
--
-- INDEHOLDER RÅ CPR — samme adgangsbegrænsning som arc.haiba_cases_infektion:
-- ETL-servicebrugeren og GDPR-databehandlere. Slutbrugere skal læse et view
-- der eksponerer dw_ek_Borger i stedet.
--
-- CPR er beholdt her, fordi tabellens grain er (HAI_type, cprnr, HAI_date) —
-- uden cprnr kan rækkerne ikke skelnes før AB#27833 har leveret dw_ek_Borger.
-- Skal CPR helt ud af dm, skal nøglen skifte til dw_ek_Borger, og det kræver
-- at 27833 er implementeret først.
--
-- To SOR-roller: HAI_Opholdsadresse (hvor patienten fysisk var) og
-- HAI_Kontaktansvar (behandlingsansvarlig enhed). De opløses hver for sig.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_Cases_Infektion] (
        [HAI_type]                            NVARCHAR(50)  NOT NULL,
        [cprnr]                               NVARCHAR(10)  NOT NULL,
        [HAI_date]                            NVARCHAR(30)  NOT NULL,
        [dw_ek_Borger]                        INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_ek_Borger] DEFAULT (-1),
        [HAI_Opholdsadresse]                  NVARCHAR(30)  NULL,
        [dw_ek_Organisation_opholdsadresse]   INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_ek_Organisation_opholdsadresse] DEFAULT (-1),
        [HAI_Kontaktansvar]                   NVARCHAR(30)  NULL,
        [dw_ek_Organisation_kontaktansvar]    INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_ek_Organisation_kontaktansvar] DEFAULT (-1),
        [extid]                               NVARCHAR(500) NULL,
        [larlabnr]                            NVARCHAR(500) NULL,
        [HAI_RegionKode]                      NVARCHAR(10)  NULL,
        [HAI_Region]                          NVARCHAR(255) NULL,
        [HAI_ejerforhold]                     NVARCHAR(50)  NULL,
        [HAI_Matrikel]                        NVARCHAR(255) NULL,
        [HAI_Sundhedsinstitution]             NVARCHAR(255) NULL,
        [HAI_OverliggendeEnhed]               NVARCHAR(255) NULL,
        [HAI_AfdelingAfsnit]                  NVARCHAR(255) NULL,
        [findings1]                           NVARCHAR(500) NULL,
        [findings2]                           NVARCHAR(500) NULL,
        [findings3]                           NVARCHAR(500) NULL,
        [findings4]                           NVARCHAR(500) NULL,
        [findings5]                           NVARCHAR(500) NULL,
        [findings6]                           NVARCHAR(500) NULL,
        [findings7]                           NVARCHAR(500) NULL,
        -- AB#27834: FHIR-kontaktrelation pr. rolle. match_kvalitet er NOT NULL
        -- CONSTRAINT [DF_HAIBA_Cases_Infektion_findings7] DEFAULT 'ikke_koert', saa en raekke aldrig laeses som "intet match"
        -- alene fordi relationen endnu ikke er beregnet. match_dage er
        -- fortegnsbaerende: negativ foer kontakten, positiv efter, 0 indeni.
        [encounter_id_kontaktansvar]          NVARCHAR(255) NULL,
        [match_kvalitet_kontaktansvar]        VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_match_kvalitet_kontaktansvar] DEFAULT ('ikke_koert'),
        [match_dage_kontaktansvar]            INT           NULL,
        [encounter_id_opholdsadresse]         NVARCHAR(255) NULL,
        [location_id_opholdsadresse]          NVARCHAR(255) NULL,
        [match_kvalitet_opholdsadresse]       VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_match_kvalitet_opholdsadresse] DEFAULT ('ikke_koert'),
        [match_dage_opholdsadresse]           INT           NULL,
        [leverance_dato]                      DATE          NOT NULL,
        [dw_is_deleted]                       BIT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]                 DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]                 DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Cases_Infektion] PRIMARY KEY CLUSTERED (
            [HAI_type], [cprnr], [HAI_date]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_Infektion_Kontakt]
        ON [dm].[HAIBA_Cases_Infektion] ([match_kvalitet_kontaktansvar])
        INCLUDE ([encounter_id_kontaktansvar], [HAI_date], [dw_ek_Borger]);

    CREATE NONCLUSTERED INDEX [IX_dm_HAIBA_Cases_Infektion_Org]
        ON [dm].[HAIBA_Cases_Infektion]
        ([dw_ek_Organisation_kontaktansvar], [dw_ek_Organisation_opholdsadresse]);
END
GO


-- ----------------------------------------------------------------------------
-- dm.HAIBA_Cases_SSI  (SSI_HIP, SSI_KNEE)
--
-- INDEHOLDER RÅ CPR — samme adgangsbegrænsning som ovenfor.
--
-- Fire SOR-roller: producent/kontaktansvarlig for indeksoperationen og de
-- tilsvarende for reoperationen. Alle fire opløses hver for sig.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dm].[HAIBA_Cases_SSI]') AND type = 'U'
)
BEGIN
    CREATE TABLE [dm].[HAIBA_Cases_SSI] (
        [HAI_type]                                  NVARCHAR(50)  NOT NULL,
        [cprnr]                                     NVARCHAR(10)  NOT NULL,
        [operation_date]                            NVARCHAR(30)  NOT NULL,
        [dw_ek_Borger]                              INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Borger] DEFAULT (-1),
        [banr]                                      NVARCHAR(500) NULL,
        [larlabnr]                                  NVARCHAR(500) NULL,
        [extid]                                     NVARCHAR(500) NULL,
        [Index_diagnosis]                           NVARCHAR(50)  NULL,
        [Index_operation]                           NVARCHAR(100) NULL,
        [Index_acute]                               NVARCHAR(10)  NULL,
        [Index_side]                                NVARCHAR(20)  NULL,
        [HAI_producent]                             NVARCHAR(30)  NULL,
        [dw_ek_Organisation_producent]              INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_producent] DEFAULT (-1),
        [HAI_kontaktansvarlig]                      NVARCHAR(30)  NULL,
        [dw_ek_Organisation_kontaktansvarlig]       INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_kontaktansvarlig] DEFAULT (-1),
        [HAI_date]                                  NVARCHAR(30)  NULL,
        [reo_diagnosis]                             NVARCHAR(50)  NULL,
        [reoperation]                               NVARCHAR(255) NULL,
        [reo_date]                                  NVARCHAR(30)  NULL,
        [reo_acute]                                 NVARCHAR(10)  NULL,
        [reo_side]                                  NVARCHAR(20)  NULL,
        [reo_producent]                             NVARCHAR(30)  NULL,
        [dw_ek_Organisation_reo_producent]          INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_reo_producent] DEFAULT (-1),
        [reo_kontaktansvarlig]                      NVARCHAR(30)  NULL,
        [dw_ek_Organisation_reo_kontaktansvarlig]   INT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_ek_Organisation_reo_kontaktansvarlig] DEFAULT (-1),
        [HAI_kamme_positive_biopsies]               NVARCHAR(10)  NULL,
        [HAI_kamme_total_biopsies]                  NVARCHAR(10)  NULL,
        [SSI_acute_365]                             NVARCHAR(10)  NULL,
        [SSI_acute_90]                              NVARCHAR(10)  NULL,
        [HAI_RegionKode]                            NVARCHAR(10)  NULL,
        [HAI_Region]                                NVARCHAR(255) NULL,
        [HAI_ejerforhold]                           NVARCHAR(50)  NULL,
        [HAI_Matrikel]                              NVARCHAR(255) NULL,
        [HAI_Sundhedsinstitution]                   NVARCHAR(255) NULL,
        [HAI_OverliggendeEnhed]                     NVARCHAR(255) NULL,
        [HAI_AfdelingAfsnit]                        NVARCHAR(255) NULL,
        [findings1]                                 NVARCHAR(500) NULL,
        [findings2]                                 NVARCHAR(500) NULL,
        [findings3]                                 NVARCHAR(500) NULL,
        [findings4]                                 NVARCHAR(500) NULL,
        [findings5]                                 NVARCHAR(500) NULL,
        [findings6]                                 NVARCHAR(500) NULL,
        [findings7]                                 NVARCHAR(500) NULL,
        [findings8]                                 NVARCHAR(500) NULL,
        [findings9]                                 NVARCHAR(500) NULL,
        -- AB#27834: en relation pr. rolle. Indeksoperationen og reoperationen
        -- er to haendelser med hver sin dato, og hver sin producent og
        -- kontaktansvarlige.
        [encounter_id_producent]                    NVARCHAR(255) NULL,
        [match_kvalitet_producent]                  VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_match_kvalitet_producent] DEFAULT ('ikke_koert'),
        [match_dage_producent]                      INT           NULL,
        [encounter_id_kontaktansvarlig]             NVARCHAR(255) NULL,
        [match_kvalitet_kontaktansvarlig]           VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_match_kvalitet_kontaktansvarlig] DEFAULT ('ikke_koert'),
        [match_dage_kontaktansvarlig]               INT           NULL,
        [encounter_id_reo_producent]                NVARCHAR(255) NULL,
        [match_kvalitet_reo_producent]              VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_match_kvalitet_reo_producent] DEFAULT ('ikke_koert'),
        [match_dage_reo_producent]                  INT           NULL,
        [encounter_id_reo_kontaktansvarlig]         NVARCHAR(255) NULL,
        [match_kvalitet_reo_kontaktansvarlig]       VARCHAR(20)   NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_match_kvalitet_reo_kontaktansvarlig] DEFAULT ('ikke_koert'),
        [match_dage_reo_kontaktansvarlig]           INT           NULL,
        [leverance_dato]                            DATE          NOT NULL,
        [dw_is_deleted]                             BIT           NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]                       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]                       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_dm_HAIBA_Cases_SSI] PRIMARY KEY CLUSTERED (
            [HAI_type], [cprnr], [operation_date]
        )
    );
END
GO
