-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.drg_sghforlob  (DRG: DRGSGHFORLOB)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_sghforlob]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_sghforlob] (
        [takstsystem]                   NVARCHAR(3)     NOT NULL,
        [ind_kontakt_uuid]              NVARCHAR(36)    NOT NULL,
        [ud_kontakt_uuid]               NVARCHAR(36)    NULL,
        [drg_kontakt_uuid]              NVARCHAR(36)    NULL,
        [DRGSGHFORLOB_TYPE]             NVARCHAR(500)   NULL,
        [ANTAL_I_FORLOB]                NVARCHAR(500)   NULL,
        [AKTIVITETSAAR]                 NVARCHAR(500)   NULL,
        [MAANED_DRGSGHF]                NVARCHAR(500)   NULL,
        [SYGEHUS_REGION]                NVARCHAR(500)   NULL,
        [SYGEHUS]                       NVARCHAR(500)   NULL,
        [SYGEHUSNAVN]                   NVARCHAR(500)   NULL,
        [SYGEHUSTYPE]                   NVARCHAR(500)   NULL,
        [CPR]                           NVARCHAR(500)   NULL,
        [ALDER_AAR_DRGSGHF]             NVARCHAR(500)   NULL,
        [ALDER_DAGE_DRGSGHF]            NVARCHAR(500)   NULL,
        [KOEN]                          NVARCHAR(500)   NULL,
        [INDTIDSPUNKT_DRGSGHF]          NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_DRGSGHF]           NVARCHAR(500)   NULL,
        [LIGGEDAGE_DRGSGHF]             NVARCHAR(500)   NULL,
        [KONTAKTDAGE_DRGSGHF]           NVARCHAR(500)   NULL,
        [PRIORITET_DRGSGHF]             NVARCHAR(500)   NULL,
        [KONTAKTAARSAG_DRGSGHF]         NVARCHAR(500)   NULL,
        [DRGKONTAKTTYPE_DRGSGHF]        NVARCHAR(500)   NULL,
        [AFSLUTNINGSMAADE_DRGSGHF]      NVARCHAR(500)   NULL,
        [HENVISNINGSMAADE_DRGSGHF]      NVARCHAR(500)   NULL,
        [HENVISNINGSAARSAG_DRGSGHF]     NVARCHAR(500)   NULL,
        [HENVISNINGSTIDSPUNKT_DRGSGHF]  NVARCHAR(500)   NULL,
        [HENVISENDEINSTANS_DRGSGHF]     NVARCHAR(500)   NULL,
        [DRG_DRGSGHF]                   NVARCHAR(500)   NULL,
        [PRIS_DRG_DRGSGHF]              NVARCHAR(500)   NULL,
        [TRIMPUNKT_DRGSGHF]             NVARCHAR(500)   NULL,
        [LANGLIGGERDAGE_DRGSGHF]        NVARCHAR(500)   NULL,
        [LANGLIGGERPRIS_DRGSGHF]        NVARCHAR(500)   NULL,
        [TOTALPRIS_DRGSGHF]             NVARCHAR(500)   NULL,
        [LONSUMAFGIFT_PRIS_DRGSGHF]     NVARCHAR(500)   NULL,
        [LONSUMAFGIFT_LANGLIGGERPRIS]   NVARCHAR(500)   NULL,
        [D90UA05_DRGSGHF]               NVARCHAR(500)   NULL,
        [ADIAG_DRGSGHF]                 NVARCHAR(500)   NULL,
        [ADIAG_DRGSGH_IND]              NVARCHAR(500)   NULL,
        [ADIAG_DRGSGH_UD]               NVARCHAR(500)   NULL,
        [DRG_GROUPER_DRGSGHF]           NVARCHAR(500)   NULL,
        [REGEL_DRGSGHF]                 NVARCHAR(500)   NULL,
        [RTC_DRGSGHF]                   NVARCHAR(500)   NULL,
        [MDC_DRGSGHF]                   NVARCHAR(500)   NULL,
        [BO_KOM_DRGSGHF]                NVARCHAR(500)   NULL,
        [BO_KOM_SOURCE_DRGSGHF]         NVARCHAR(500)   NULL,
        [BO_REGION_DRGSGHF]             NVARCHAR(500)   NULL,
        [BETALER_KOM_DRGSGHF]           NVARCHAR(500)   NULL,
        [BETALER_REGION_DRGSGHF]        NVARCHAR(500)   NULL,
        [BETALINGSAFTALE_DRGSGHF]       NVARCHAR(500)   NULL,
        [BETALER_DRGSGHF]               NVARCHAR(500)   NULL,
        [SPECIALISERINGSNIVEAU_DRGSGHF] NVARCHAR(500)   NULL,
        [MRK_NAERHED_1]                 NVARCHAR(500)   NULL,
        [MRK_NAERHED_2]                 NVARCHAR(500)   NULL,
        [MRK_NAERHED_4]                 NVARCHAR(500)   NULL,
        [VARIGHED_TIME_DRGSGHF]         NVARCHAR(500)   NULL,
        [VARIGHED_TIME_TYPE_DRGSGHF]    NVARCHAR(500)   NULL,
        [MRK_Kirurgi]                   NVARCHAR(500)   NULL,
        [leverance_dato]                DATE            NOT NULL,
        [dw_is_deleted]                 BIT             NOT NULL CONSTRAINT [DF_drg_sghforlob_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]           DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]           DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_sghforlob] PRIMARY KEY CLUSTERED (
            [takstsystem], [ind_kontakt_uuid]
        )
    );

    -- Aarsskive-scoped delete-detection filtrerer paa (takstsystem, aktivitetsaar).
    CREATE NONCLUSTERED INDEX [IX_arc_drg_sghforlob_Aarsskive]
        ON [arc].[drg_sghforlob] ([takstsystem], [AKTIVITETSAAR], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_drg_sghforlob_DW_Timestamp_Update]
        ON [arc].[drg_sghforlob] ([dw_timestamp_update] DESC);
END
GO
