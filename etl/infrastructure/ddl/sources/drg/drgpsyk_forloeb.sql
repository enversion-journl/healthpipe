-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_forloeb  (DRGPSYK: Psyk_forloeb)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_forloeb]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_forloeb] (
        [takstsystem]                   NVARCHAR(3)     NOT NULL,
        [foerste_kontakt_uuid]          NVARCHAR(36)    NOT NULL,
        [sidste_kontakt_uuid]           NVARCHAR(36)    NULL,
        [DW_EK_Helbredsforloeb]         NVARCHAR(500)   NULL,
        [CPRnummer]                     NVARCHAR(500)   NULL,
        [INDTIDSPUNKT_Forloeb_SAMLET]   NVARCHAR(500)   NULL,
        [INDTIDSPUNKT_Forloeb]          NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_ForLoeb_SAMLET]    NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_Forloeb]           NVARCHAR(500)   NULL,
        [VARIGHED_TIME_Forloeb]         NVARCHAR(500)   NULL,
        [LIGGEDAGE_Forloeb]             NVARCHAR(500)   NULL,
        [KONTAKTTYPE_Forloeb]           NVARCHAR(500)   NULL,
        [FORLOEB_TYPE]                  NVARCHAR(500)   NULL,
        [PRIORITET_Forloeb]             NVARCHAR(500)   NULL,
        [PRIS_PsykiatriForloeb]         NVARCHAR(500)   NULL,
        [DtdJusteretTakst]              NVARCHAR(500)   NULL,
        [LONSUMAFGIFT_PRIS_Forloeb]     NVARCHAR(500)   NULL,
        [ADIAG_Forloeb]                 NVARCHAR(500)   NULL,
        [SYGEHUS]                       NVARCHAR(500)   NULL,
        [SYGEHUSNAVN]                   NVARCHAR(510)   NULL,
        [SYGEHUSTYPE]                   NVARCHAR(500)   NULL,
        [SYGEHUS_REGION]                NVARCHAR(500)   NULL,
        [BO_KOM_Forloeb]                NVARCHAR(500)   NULL,
        [BO_REGION_Forloeb]             NVARCHAR(500)   NULL,
        [BO_KOM_SOURCE]                 NVARCHAR(500)   NULL,
        [ALDER_AAR_IND_Forloeb]         NVARCHAR(500)   NULL,
        [ALDER_AAR_UD_Forloeb]          NVARCHAR(500)   NULL,
        [KOEN]                          NVARCHAR(500)   NULL,
        [ANTAL_I_FORLOEB]               NVARCHAR(500)   NULL,
        [FORLOEB_AFSLUTTET]             NVARCHAR(500)   NULL,
        [BETALINGSAFTALE_Forloeb]       NVARCHAR(500)   NULL,
        [Betaler_Forloeb]               NVARCHAR(500)   NULL,
        [SPECIALISERINGSNIVEAU_Forloeb] NVARCHAR(500)   NULL,
        [Koerselsdato]                  NVARCHAR(500)   NULL,
        [leverance_dato]                DATE            NOT NULL,
        [dw_is_deleted]                 BIT             NOT NULL CONSTRAINT [DF_drgpsyk_forloeb_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]           DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]           DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_forloeb] PRIMARY KEY CLUSTERED (
            [takstsystem], [foerste_kontakt_uuid]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_forloeb_DW_Timestamp_Update]
        ON [arc].[drgpsyk_forloeb] ([dw_timestamp_update] DESC);
END
GO
