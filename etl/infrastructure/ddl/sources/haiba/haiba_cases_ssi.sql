-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
