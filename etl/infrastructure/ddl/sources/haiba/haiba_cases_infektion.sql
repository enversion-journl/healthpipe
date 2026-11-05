-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
