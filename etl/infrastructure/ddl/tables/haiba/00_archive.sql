-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: HAIBA (hospitalserhvervede infektioner, ugentlig CSV fra SDS)
--
-- 1:1 spejling af leverancefilerne. Alle værdikolonner er NVARCHAR — arkivet
-- typer ikke, det bevarer kildens tekst. Typning hører til i stg-laget.
--
-- Hver leverance er et fuldt snapshot, og arkivet skal afspejle den nyeste.
-- Mergen kører derfor med detect_deletes=True: rækker der ikke længere findes
-- hos SDS får dw_is_deleted = 1. Se DSI/csv_files/haiba_archive.py.
--
-- Merge-nøgler er verificeret unikke mod alle 14 filer i leverance 2026-08-15
-- / 2026-08-08 (haiba_key_probe.py) — nul dubletter, op til 2.138.224 rækker
-- i CDI_COHA.
--
-- SORIdentifier er tom præcis når rækken er et landsaggregat
-- (RegionKode 8888/9999) — fra 0,4 % af BACT-rækkerne til 85,4 % af
-- Acute_HIP. Nøglekolonner er derfor NOT NULL og bærer '' frem for NULL:
-- SQL Server tillader ikke NULL i en primærnøgle, og NULL matcher aldrig sig
-- selv i MERGE'ens ON-klausul, så rækken ville blive genindsat hver kørsel.
--
-- Nøglekolonnerne er snævert dimensioneret, så den clustered PK holder sig
-- langt under 900-byte-grænsen (agg: 340 bytes).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.haiba_agg_infektion  (BACT, UTI, CDI_COHA, CDI_HOHA)
-- ~6,7 mio. rækker pr. leverance. BEMÆRK: HAI er 'COHA'/'HOHA' her, mens
-- case-filerne bruger 'CDI_COHA'/'CDI_HOHA' — enhver join skal mappe.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[haiba_agg_infektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[haiba_agg_infektion] (
        [HAI]                 NVARCHAR(50)  NOT NULL,
        [HAIBA_dato]          NVARCHAR(30)  NOT NULL,
        [Rolle_ansvar]        NVARCHAR(50)  NOT NULL,
        [RegionKode]          NVARCHAR(10)  NOT NULL,
        [SORIdentifier]       NVARCHAR(30)  NOT NULL,
        [ejerforhold]         NVARCHAR(50)  NULL,
        [region]              NVARCHAR(255) NULL,
        [Sundhedsinstitution] NVARCHAR(255) NULL,
        [Matrikel]            NVARCHAR(255) NULL,
        [OverliggendeEnhed]   NVARCHAR(255) NULL,
        [AfdelingAfsnit]      NVARCHAR(255) NULL,
        [antal]               NVARCHAR(50)  NULL,
        [risikodoegn]         NVARCHAR(50)  NULL,
        [leverance_dato]      DATE          NOT NULL,
        [dw_is_deleted]       BIT           NOT NULL CONSTRAINT [DF_haiba_agg_infektion_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert] DATETIME2(0)  NOT NULL,
        [dw_timestamp_update] DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_haiba_agg_infektion] PRIMARY KEY CLUSTERED (
            [HAI], [HAIBA_dato], [Rolle_ansvar], [RegionKode], [SORIdentifier]
        )
    );

    -- Konsumenter filtrerer typisk på (HAI-type, aggregeringsniveau).
    CREATE NONCLUSTERED INDEX [IX_arc_haiba_agg_infektion_Region]
        ON [arc].[haiba_agg_infektion] ([RegionKode], [Rolle_ansvar])
        INCLUDE ([HAI], [HAIBA_dato], [antal], [risikodoegn]);

    CREATE NONCLUSTERED INDEX [IX_arc_haiba_agg_infektion_SOR]
        ON [arc].[haiba_agg_infektion] ([SORIdentifier]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.haiba_agg_ortopaedi  (Acute/Elective HIP/KNEE)
-- Samme dimensioner, andre mål. ~38.000 rækker pr. leverance.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[haiba_agg_ortopaedi]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[haiba_agg_ortopaedi] (
        [HAI]                 NVARCHAR(50)  NOT NULL,
        [HAIBA_dato]          NVARCHAR(30)  NOT NULL,
        [Rolle_ansvar]        NVARCHAR(50)  NOT NULL,
        [RegionKode]          NVARCHAR(10)  NOT NULL,
        [SORIdentifier]       NVARCHAR(30)  NOT NULL,
        [ejerforhold]         NVARCHAR(50)  NULL,
        [region]              NVARCHAR(255) NULL,
        [Sundhedsinstitution] NVARCHAR(255) NULL,
        [Matrikel]            NVARCHAR(255) NULL,
        [OverliggendeEnhed]   NVARCHAR(255) NULL,
        [AfdelingAfsnit]      NVARCHAR(255) NULL,
        [antal90]             NVARCHAR(50)  NULL,
        [antal365]            NVARCHAR(50)  NULL,
        [operationer]         NVARCHAR(50)  NULL,
        [leverance_dato]      DATE          NOT NULL,
        [dw_is_deleted]       BIT           NOT NULL CONSTRAINT [DF_haiba_agg_ortopaedi_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert] DATETIME2(0)  NOT NULL,
        [dw_timestamp_update] DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_haiba_agg_ortopaedi] PRIMARY KEY CLUSTERED (
            [HAI], [HAIBA_dato], [Rolle_ansvar], [RegionKode], [SORIdentifier]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_haiba_agg_ortopaedi_Region]
        ON [arc].[haiba_agg_ortopaedi] ([RegionKode], [Rolle_ansvar])
        INCLUDE ([HAI], [HAIBA_dato], [antal90], [antal365], [operationer]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.haiba_cases_infektion  (BACT, UTI, CDI_COHA, CDI_HOHA — case-niveau)
--
-- INDEHOLDER RÅ CPR. Adgang skal begrænses til ETL-servicebrugeren og
-- GDPR-databehandlere; slutbrugere må kun se views med DW_EK_Borger.
--
-- Nøglen er (HAI_type, cprnr, HAI_date) — verificeret unik i alle fire
-- case-filer. extid og larlabnr er BEVIDST udeladt af nøglen: de indeholder
-- mellemrumsseparerede lister af laboratorie-id'er uden øvre længde, og hører
-- ikke hjemme i en clustered nøgle.
--
-- findings1..7 er dimensioneret efter maksimum på tværs af typer: BACT
-- leverer 7, UTI leverer p.t. 4 (leverede 6 i februar 2025), CDI ingen.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[haiba_cases_infektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[haiba_cases_infektion] (
        [HAI_type]                 NVARCHAR(50)  NOT NULL,
        [cprnr]                    NVARCHAR(10)  NOT NULL,
        [HAI_date]                 NVARCHAR(30)  NOT NULL,
        [HAI_Opholdsadresse]       NVARCHAR(30)  NULL,
        [HAI_Kontaktansvar]        NVARCHAR(30)  NULL,
        [extid]                    NVARCHAR(500) NULL,
        [larlabnr]                 NVARCHAR(500) NULL,
        [HAI_RegionKode]           NVARCHAR(10)  NULL,
        [HAI_Region]               NVARCHAR(255) NULL,
        [HAI_ejerforhold]          NVARCHAR(50)  NULL,
        [HAI_Matrikel]             NVARCHAR(255) NULL,
        [HAI_Sundhedsinstitution]  NVARCHAR(255) NULL,
        [HAI_OverliggendeEnhed]    NVARCHAR(255) NULL,
        [HAI_AfdelingAfsnit]       NVARCHAR(255) NULL,
        [findings1]                NVARCHAR(500) NULL,
        [findings2]                NVARCHAR(500) NULL,
        [findings3]                NVARCHAR(500) NULL,
        [findings4]                NVARCHAR(500) NULL,
        [findings5]                NVARCHAR(500) NULL,
        [findings6]                NVARCHAR(500) NULL,
        [findings7]                NVARCHAR(500) NULL,
        [leverance_dato]           DATE          NOT NULL,
        [dw_is_deleted]            BIT           NOT NULL CONSTRAINT [DF_haiba_cases_infektion_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]      DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_haiba_cases_infektion] PRIMARY KEY CLUSTERED (
            [HAI_type], [cprnr], [HAI_date]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_haiba_cases_infektion_Kontaktansvar]
        ON [arc].[haiba_cases_infektion] ([HAI_Kontaktansvar]);

    CREATE NONCLUSTERED INDEX [IX_arc_haiba_cases_infektion_Cpr]
        ON [arc].[haiba_cases_infektion] ([cprnr]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.haiba_cases_ssi  (SSI_HIP, SSI_KNEE)
--
-- INDEHOLDER RÅ CPR — samme adgangsbegrænsning som ovenfor.
--
-- operation_date og reo_date leveres i SAS-format (10FEB2023:13:00:00.000) og
-- normaliseres til ISO ved indlæsning; alle øvrige datoer er allerede ISO.
-- SOR-kolonnerne hedder HAI_producent / HAI_kontaktansvarlig her, ikke
-- HAI_Opholdsadresse / HAI_Kontaktansvar som i infektions-casene.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[haiba_cases_ssi]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[haiba_cases_ssi] (
        [HAI_type]                     NVARCHAR(50)  NOT NULL,
        [cprnr]                        NVARCHAR(10)  NOT NULL,
        [operation_date]               NVARCHAR(30)  NOT NULL,
        [banr]                         NVARCHAR(500) NULL,
        [larlabnr]                     NVARCHAR(500) NULL,
        [extid]                        NVARCHAR(500) NULL,
        [Index_diagnosis]              NVARCHAR(50)  NULL,
        [Index_operation]              NVARCHAR(100) NULL,
        [Index_acute]                  NVARCHAR(10)  NULL,
        [Index_side]                   NVARCHAR(20)  NULL,
        [HAI_producent]                NVARCHAR(30)  NULL,
        [HAI_kontaktansvarlig]         NVARCHAR(30)  NULL,
        [HAI_date]                     NVARCHAR(30)  NULL,
        [reo_diagnosis]                NVARCHAR(50)  NULL,
        [reoperation]                  NVARCHAR(255) NULL,
        [reo_date]                     NVARCHAR(30)  NULL,
        [reo_acute]                    NVARCHAR(10)  NULL,
        [reo_side]                     NVARCHAR(20)  NULL,
        [reo_producent]                NVARCHAR(30)  NULL,
        [reo_kontaktansvarlig]         NVARCHAR(30)  NULL,
        [HAI_kamme_positive_biopsies]  NVARCHAR(10)  NULL,
        [HAI_kamme_total_biopsies]     NVARCHAR(10)  NULL,
        [SSI_acute_365]                NVARCHAR(10)  NULL,
        [SSI_acute_90]                 NVARCHAR(10)  NULL,
        [HAI_RegionKode]               NVARCHAR(10)  NULL,
        [HAI_Region]                   NVARCHAR(255) NULL,
        [HAI_ejerforhold]              NVARCHAR(50)  NULL,
        [HAI_Matrikel]                 NVARCHAR(255) NULL,
        [HAI_Sundhedsinstitution]      NVARCHAR(255) NULL,
        [HAI_OverliggendeEnhed]        NVARCHAR(255) NULL,
        [HAI_AfdelingAfsnit]           NVARCHAR(255) NULL,
        [findings1]                    NVARCHAR(500) NULL,
        [findings2]                    NVARCHAR(500) NULL,
        [findings3]                    NVARCHAR(500) NULL,
        [findings4]                    NVARCHAR(500) NULL,
        [findings5]                    NVARCHAR(500) NULL,
        [findings6]                    NVARCHAR(500) NULL,
        [findings7]                    NVARCHAR(500) NULL,
        [findings8]                    NVARCHAR(500) NULL,
        [findings9]                    NVARCHAR(500) NULL,
        [leverance_dato]               DATE          NOT NULL,
        [dw_is_deleted]                BIT           NOT NULL CONSTRAINT [DF_haiba_cases_ssi_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]          DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]          DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_haiba_cases_ssi] PRIMARY KEY CLUSTERED (
            [HAI_type], [cprnr], [operation_date]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_haiba_cases_ssi_Cpr]
        ON [arc].[haiba_cases_ssi] ([cprnr]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.haiba_agg_revision  (revisionsspor)
--
-- Arkivet holder kun nyeste snapshot. SDS reviderer historiske tal — ~16.500
-- BACT-rækker fik nye værdier på samme nøgle over én uge — og uden denne log
-- ville de rettelser være usynlige. Det er hele begrundelsen for at gå gennem
-- arkivlaget i stedet for at læse filerne direkte ind i en mart.
--
-- Kun agg-tabellerne logges. Case-data er reelt append-only (0 ændrede rækker
-- over en uge mod 3.268 uændrede), så en log dér ville stå tom.
--
-- Insert-only. Én række pr. ændret målekolonne pr. kørsel.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[haiba_agg_revision]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[haiba_agg_revision] (
        [revision_id]         BIGINT IDENTITY(1,1) NOT NULL,
        [kildetabel]          NVARCHAR(50)  NOT NULL,
        [HAI]                 NVARCHAR(50)  NOT NULL,
        [HAIBA_dato]          NVARCHAR(30)  NOT NULL,
        [Rolle_ansvar]        NVARCHAR(50)  NOT NULL,
        [RegionKode]          NVARCHAR(10)  NOT NULL,
        [SORIdentifier]       NVARCHAR(30)  NOT NULL,
        [maalekolonne]        NVARCHAR(50)  NOT NULL,
        [vaerdi_foer]         NVARCHAR(50)  NULL,
        [vaerdi_efter]        NVARCHAR(50)  NULL,
        [leverance_dato_foer] DATE          NULL,
        [leverance_dato]      DATE          NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)  NOT NULL CONSTRAINT [DF_haiba_agg_revision_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_arc_haiba_agg_revision] PRIMARY KEY CLUSTERED ([revision_id])
    );

    CREATE NONCLUSTERED INDEX [IX_arc_haiba_agg_revision_Noegle]
        ON [arc].[haiba_agg_revision] (
            [HAI], [HAIBA_dato], [Rolle_ansvar], [RegionKode], [SORIdentifier]
        )
        INCLUDE ([maalekolonne], [vaerdi_foer], [vaerdi_efter], [leverance_dato]);

    CREATE NONCLUSTERED INDEX [IX_arc_haiba_agg_revision_Leverance]
        ON [arc].[haiba_agg_revision] ([leverance_dato]);
END
GO
