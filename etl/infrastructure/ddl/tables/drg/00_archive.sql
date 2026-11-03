-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: DRG-grupperet LPR fra SDS
--
-- 1:1 spejling af leverancefilerne MINUS de leverance-interne heltals-ID'er og
-- SDS' DW_EK_*/DW_SK_*-noegler: de omnummereres ved hver leverance og ville
-- vaere ude af sync paa tvaers af tabeller efter en merge. Relationerne er i
-- stedet oploest til LPR3-UUID-referencer (kolonnerne *_uuid/*_noegle),
-- beregnet i ext-views af DSI/csv_files/drg_archive.py.
-- Undtagelse: psyk-tabellernes DW_EK_Helbredsforloeb beholdes som ANALYTISK
-- attribut (psyk-skemaet har ingen forloebs-UUID) — aldrig join-noegle.
--
-- Alle vaerdikolonner er NVARCHAR — arkivet typer ikke. Vaerdikolonner har
-- SAMME bredde som ext (min. 500) saa en for lang vaerdi aldrig kan afbryde
-- mergen med truncation (HAIBA-laere, [internal ticket]); kun noeglekolonner er
-- snaevre, og extract validerer deres laengde med tydelig fejl.
--
-- Merge-noegler (verificeret i drg_noegler_beslutningsnotat.md): takstsystem +
-- KONTAKT_VAERDI / PROCEDURE_VAERDI / ind-kontaktens UUID / komposit for
-- diagnoser. Noeglekolonner er NOT NULL og baerer '' frem for NULL: SQL
-- Server tillader ikke NULL i en primaernoegle, og NULL matcher aldrig sig
-- selv i MERGE'ens ON-klausul.
--
-- detect_deletes koerer AARSSKIVE-SCOPED: M-leverancer daekker tre
-- aktivitetsaar, U-leverancer kun to, saa delete-detection begraenses til de
-- (takstsystem, aktivitetsaar)-skiver leverancen faktisk indeholder.
-- Udgaaede takstsystemer (somatik-T24) beholder deres raekker.
--
-- Kolonnesaettet er UNIONEN af SDS' Metadata_*-skemaer for T24/T25/T26
-- (T24/T25 mangler FORLOB_REFERENCE_* — kolonnerne staar tomme der).
-- Genereret fra DSI/csv_files/drg_datasets.py; konsistens haandhaeves af
-- tests/unit/test_drg_datasets_ddl.py.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.drg_kontakt  (DRG: DRGKONTAKT)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_kontakt]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_kontakt] (
        [takstsystem]              NVARCHAR(3)     NOT NULL,
        [sygehusforloeb_noegle]    NVARCHAR(36)    NULL,
        [FORLOEBSPROCEDURE]        NVARCHAR(500)   NULL,
        [DRGSGHFORLOB_TYPE]        NVARCHAR(500)   NULL,
        [DRGSGHFORLOB_NR]          NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_DRGSGHF]      NVARCHAR(500)   NULL,
        [AKTIVITETSAAR]            NVARCHAR(500)   NULL,
        [SYGEHUS_REGION]           NVARCHAR(500)   NULL,
        [SYGEHUS]                  NVARCHAR(500)   NULL,
        [SYGEHUSNAVN]              NVARCHAR(500)   NULL,
        [SYGEHUSTYPE]              NVARCHAR(500)   NULL,
        [DRG_KONTAKT_ENHED]        NVARCHAR(500)   NULL,
        [DRG_KONTAKT_ENHEDSNAVN]   NVARCHAR(500)   NULL,
        [SPECIALE]                 NVARCHAR(500)   NULL,
        [DRG_FORLOBSELEMENT_ENHED] NVARCHAR(500)   NULL,
        [CPR]                      NVARCHAR(500)   NULL,
        [ALDER_AAR]                NVARCHAR(500)   NULL,
        [ALDER_DAGE]               NVARCHAR(500)   NULL,
        [KOEN]                     NVARCHAR(500)   NULL,
        [INDTIDSPUNKT_DRGKONTAKT]  NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_DRGKONTAKT]   NVARCHAR(500)   NULL,
        [LIGGEDAGE]                NVARCHAR(500)   NULL,
        [KONTAKTDAGE]              NVARCHAR(500)   NULL,
        [PRIORITET]                NVARCHAR(500)   NULL,
        [KONTAKTAARSAG]            NVARCHAR(500)   NULL,
        [DRGKONTAKTTYPE]           NVARCHAR(500)   NULL,
        [KONTAKTTYPE]              NVARCHAR(500)   NULL,
        [AFSLUTNINGSMAADE]         NVARCHAR(500)   NULL,
        [HENVISNINGSMAADE]         NVARCHAR(500)   NULL,
        [HENVISNINGSAARSAG]        NVARCHAR(500)   NULL,
        [HENVISNINGSTIDSPUNKT]     NVARCHAR(500)   NULL,
        [HENVISENDEINSTANS]        NVARCHAR(500)   NULL,
        [DRG]                      NVARCHAR(500)   NULL,
        [DRG_DRGSGHF]              NVARCHAR(500)   NULL,
        [PRIS_DRG]                 NVARCHAR(500)   NULL,
        [LANGLIGGERDAGE]           NVARCHAR(500)   NULL,
        [TRIMPUNKT]                NVARCHAR(500)   NULL,
        [LANGLIGGERPRIS]           NVARCHAR(500)   NULL,
        [TOTALPRIS_DRG]            NVARCHAR(500)   NULL,
        [D90UA05]                  NVARCHAR(500)   NULL,
        [DRG_GROUPER]              NVARCHAR(500)   NULL,
        [REGEL]                    NVARCHAR(500)   NULL,
        [RTC]                      NVARCHAR(500)   NULL,
        [MDC]                      NVARCHAR(500)   NULL,
        [UDFALDSGIVENDE_KONTAKT]   NVARCHAR(500)   NULL,
        [TUNGESTE_DRGKONTAKT]      NVARCHAR(500)   NULL,
        [BETALINGSAFTALE]          NVARCHAR(500)   NULL,
        [BETALER]                  NVARCHAR(500)   NULL,
        [SPECIALISERINGSNIVEAU]    NVARCHAR(500)   NULL,
        [BO_KOM]                   NVARCHAR(500)   NULL,
        [BO_KOM_SOURCE]            NVARCHAR(500)   NULL,
        [BO_REGION]                NVARCHAR(500)   NULL,
        [BETALER_KOM]              NVARCHAR(500)   NULL,
        [BETALER_REGION]           NVARCHAR(500)   NULL,
        [KONTAKT_VAERDI]           NVARCHAR(36)    NOT NULL,
        [FORLOB_VAERDI]            NVARCHAR(500)   NULL,
        [VARIGHED_TIME]            NVARCHAR(500)   NULL,
        [VARIGHED_TIME_TYPE]       NVARCHAR(500)   NULL,
        [DIA01]                    NVARCHAR(500)   NULL,
        [DIA02]                    NVARCHAR(500)   NULL,
        [DIA03]                    NVARCHAR(500)   NULL,
        [DIA04]                    NVARCHAR(500)   NULL,
        [DIA05]                    NVARCHAR(500)   NULL,
        [DIA06]                    NVARCHAR(500)   NULL,
        [DIA07]                    NVARCHAR(500)   NULL,
        [DIA08]                    NVARCHAR(500)   NULL,
        [DIA09]                    NVARCHAR(500)   NULL,
        [DIA10]                    NVARCHAR(500)   NULL,
        [DIA11]                    NVARCHAR(500)   NULL,
        [DIA12]                    NVARCHAR(500)   NULL,
        [DIA13]                    NVARCHAR(500)   NULL,
        [DIA14]                    NVARCHAR(500)   NULL,
        [DIA15]                    NVARCHAR(500)   NULL,
        [DIA16]                    NVARCHAR(500)   NULL,
        [DIA17]                    NVARCHAR(500)   NULL,
        [DIA18]                    NVARCHAR(500)   NULL,
        [DIA19]                    NVARCHAR(500)   NULL,
        [DIA20]                    NVARCHAR(500)   NULL,
        [TDIA01]                   NVARCHAR(500)   NULL,
        [TDIA02]                   NVARCHAR(500)   NULL,
        [TDIA03]                   NVARCHAR(500)   NULL,
        [TDIA04]                   NVARCHAR(500)   NULL,
        [TDIA05]                   NVARCHAR(500)   NULL,
        [TDIA06]                   NVARCHAR(500)   NULL,
        [TDIA07]                   NVARCHAR(500)   NULL,
        [TDIA08]                   NVARCHAR(500)   NULL,
        [TDIA09]                   NVARCHAR(500)   NULL,
        [TDIA10]                   NVARCHAR(500)   NULL,
        [TDIA11]                   NVARCHAR(500)   NULL,
        [TDIA12]                   NVARCHAR(500)   NULL,
        [TDIA13]                   NVARCHAR(500)   NULL,
        [TDIA14]                   NVARCHAR(500)   NULL,
        [PROC01]                   NVARCHAR(500)   NULL,
        [PROC02]                   NVARCHAR(500)   NULL,
        [PROC03]                   NVARCHAR(500)   NULL,
        [PROC04]                   NVARCHAR(500)   NULL,
        [PROC05]                   NVARCHAR(500)   NULL,
        [PROC06]                   NVARCHAR(500)   NULL,
        [PROC07]                   NVARCHAR(500)   NULL,
        [PROC08]                   NVARCHAR(500)   NULL,
        [PROC09]                   NVARCHAR(500)   NULL,
        [PROC10]                   NVARCHAR(500)   NULL,
        [PROC11]                   NVARCHAR(500)   NULL,
        [PROC12]                   NVARCHAR(500)   NULL,
        [PROC13]                   NVARCHAR(500)   NULL,
        [PROC14]                   NVARCHAR(500)   NULL,
        [PROC15]                   NVARCHAR(500)   NULL,
        [PROC16]                   NVARCHAR(500)   NULL,
        [PROC17]                   NVARCHAR(500)   NULL,
        [PROC18]                   NVARCHAR(500)   NULL,
        [PROC19]                   NVARCHAR(500)   NULL,
        [PROC20]                   NVARCHAR(500)   NULL,
        [PROC21]                   NVARCHAR(500)   NULL,
        [PROC22]                   NVARCHAR(500)   NULL,
        [PROC23]                   NVARCHAR(500)   NULL,
        [PROC24]                   NVARCHAR(500)   NULL,
        [PROC25]                   NVARCHAR(500)   NULL,
        [PROC26]                   NVARCHAR(500)   NULL,
        [PROC27]                   NVARCHAR(500)   NULL,
        [PROC28]                   NVARCHAR(500)   NULL,
        [PROC29]                   NVARCHAR(500)   NULL,
        [PROC30]                   NVARCHAR(500)   NULL,
        [PROC31]                   NVARCHAR(500)   NULL,
        [PROC32]                   NVARCHAR(500)   NULL,
        [PROC33]                   NVARCHAR(500)   NULL,
        [PROC34]                   NVARCHAR(500)   NULL,
        [PROC35]                   NVARCHAR(500)   NULL,
        [PROC36]                   NVARCHAR(500)   NULL,
        [PROC37]                   NVARCHAR(500)   NULL,
        [PROC38]                   NVARCHAR(500)   NULL,
        [PROC39]                   NVARCHAR(500)   NULL,
        [PROC40]                   NVARCHAR(500)   NULL,
        [TPROC01]                  NVARCHAR(500)   NULL,
        [TPROC02]                  NVARCHAR(500)   NULL,
        [TPROC03]                  NVARCHAR(500)   NULL,
        [TPROC04]                  NVARCHAR(500)   NULL,
        [TPROC05]                  NVARCHAR(500)   NULL,
        [TPROC06]                  NVARCHAR(500)   NULL,
        [TPROC07]                  NVARCHAR(500)   NULL,
        [TPROC08]                  NVARCHAR(500)   NULL,
        [TPROC09]                  NVARCHAR(500)   NULL,
        [TPROC10]                  NVARCHAR(500)   NULL,
        [INK1]                     NVARCHAR(500)   NULL,
        [INK2]                     NVARCHAR(500)   NULL,
        [INK3]                     NVARCHAR(500)   NULL,
        [INK4]                     NVARCHAR(500)   NULL,
        [INK5]                     NVARCHAR(500)   NULL,
        [INK6]                     NVARCHAR(500)   NULL,
        [FORLOB_REFERENCE_VAERDI]  NVARCHAR(500)   NULL,
        [FORLOB_REFERENCE_TYPE]    NVARCHAR(500)   NULL,
        [leverance_dato]           DATE            NOT NULL,
        [dw_is_deleted]            BIT             NOT NULL CONSTRAINT [DF_drg_kontakt_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]      DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_kontakt] PRIMARY KEY CLUSTERED (
            [takstsystem], [KONTAKT_VAERDI]
        )
    );

    -- Aarsskive-scoped delete-detection filtrerer paa (takstsystem, aktivitetsaar).
    CREATE NONCLUSTERED INDEX [IX_arc_drg_kontakt_Aarsskive]
        ON [arc].[drg_kontakt] ([takstsystem], [AKTIVITETSAAR], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_drg_kontakt_DW_Timestamp_Update]
        ON [arc].[drg_kontakt] ([dw_timestamp_update] DESC);
END
GO

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

-- ----------------------------------------------------------------------------
-- arc.drg_diagnose  (DRG: DIAGNOSER)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_diagnose]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_diagnose] (
        [takstsystem]         NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]        NVARCHAR(36)    NOT NULL,
        [DIAGNOSE]            NVARCHAR(10)    NOT NULL,
        [ART]                 NVARCHAR(10)    NOT NULL,
        [SIDEANGIVELSE]       NVARCHAR(10)    NOT NULL,
        [DIAGNOSE_VAERDI]     NVARCHAR(500)   NULL,
        [Aktivitetsaar]       NVARCHAR(500)   NULL,
        [leverance_dato]      DATE            NOT NULL,
        [dw_is_deleted]       BIT             NOT NULL CONSTRAINT [DF_drg_diagnose_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert] DATETIME2(0)    NOT NULL,
        [dw_timestamp_update] DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_diagnose] PRIMARY KEY CLUSTERED (
            [takstsystem], [kontakt_uuid], [DIAGNOSE], [ART], [SIDEANGIVELSE]
        )
    );

    -- Aarsskive-scoped delete-detection filtrerer paa (takstsystem, aktivitetsaar).
    CREATE NONCLUSTERED INDEX [IX_arc_drg_diagnose_Aarsskive]
        ON [arc].[drg_diagnose] ([takstsystem], [Aktivitetsaar], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_drg_diagnose_DW_Timestamp_Update]
        ON [arc].[drg_diagnose] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drg_diagnose_tillaeg  (DRG: DIAGNOSER_TILLAEG)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_diagnose_tillaeg]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_diagnose_tillaeg] (
        [takstsystem]           NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]          NVARCHAR(36)    NOT NULL,
        [DIAGNOSE]              NVARCHAR(10)    NOT NULL,
        [ART]                   NVARCHAR(10)    NOT NULL,
        [SIDEANGIVELSE]         NVARCHAR(10)    NOT NULL,
        [TILLAEGSKODE_DIAGNOSE] NVARCHAR(10)    NOT NULL,
        [Aktivitetsaar]         NVARCHAR(500)   NULL,
        [leverance_dato]        DATE            NOT NULL,
        [dw_is_deleted]         BIT             NOT NULL CONSTRAINT [DF_drg_diagnose_tillaeg_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_diagnose_tillaeg] PRIMARY KEY CLUSTERED (
            [takstsystem], [kontakt_uuid], [DIAGNOSE], [ART], [SIDEANGIVELSE], [TILLAEGSKODE_DIAGNOSE]
        )
    );

    -- Aarsskive-scoped delete-detection filtrerer paa (takstsystem, aktivitetsaar).
    CREATE NONCLUSTERED INDEX [IX_arc_drg_diagnose_tillaeg_Aarsskive]
        ON [arc].[drg_diagnose_tillaeg] ([takstsystem], [Aktivitetsaar], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_drg_diagnose_tillaeg_DW_Timestamp_Update]
        ON [arc].[drg_diagnose_tillaeg] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drg_procedure  (DRG: PROCEDURE)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_procedure]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_procedure] (
        [takstsystem]               NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]              NVARCHAR(36)    NOT NULL,
        [PROCEDURE]                 NVARCHAR(500)   NULL,
        [SIDEANGIVELSE]             NVARCHAR(500)   NULL,
        [HANDLINGSSPEC]             NVARCHAR(500)   NULL,
        [KONTRAST]                  NVARCHAR(500)   NULL,
        [PROCEDURE_STARTTIDSPUNKT]  NVARCHAR(500)   NULL,
        [PROCEDURE_SLUTTIDSPUNKT]   NVARCHAR(500)   NULL,
        [PRODUCERENDE_ENHED]        NVARCHAR(500)   NULL,
        [PRODUCERENDE_SYGEHUS]      NVARCHAR(500)   NULL,
        [PROCEDURE_BETALINGSAFTALE] NVARCHAR(500)   NULL,
        [PROCEDURE_VAERDI]          NVARCHAR(36)    NOT NULL,
        [Aktivitetsaar]             NVARCHAR(500)   NULL,
        [leverance_dato]            DATE            NOT NULL,
        [dw_is_deleted]             BIT             NOT NULL CONSTRAINT [DF_drg_procedure_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]       DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]       DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_procedure] PRIMARY KEY CLUSTERED (
            [takstsystem], [PROCEDURE_VAERDI]
        )
    );

    -- Aarsskive-scoped delete-detection filtrerer paa (takstsystem, aktivitetsaar).
    CREATE NONCLUSTERED INDEX [IX_arc_drg_procedure_Aarsskive]
        ON [arc].[drg_procedure] ([takstsystem], [Aktivitetsaar], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_drg_procedure_DW_Timestamp_Update]
        ON [arc].[drg_procedure] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drg_procedure_tillaeg  (DRG: PROCEDURE_TILLAEG)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_procedure_tillaeg]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_procedure_tillaeg] (
        [takstsystem]            NVARCHAR(3)     NOT NULL,
        [procedure_uuid]         NVARCHAR(36)    NOT NULL,
        [kontakt_uuid]           NVARCHAR(36)    NOT NULL,
        [TILLAEGSKODE_PROCEDURE] NVARCHAR(10)    NOT NULL,
        [Aktivitetsaar]          NVARCHAR(500)   NULL,
        [leverance_dato]         DATE            NOT NULL,
        [dw_is_deleted]          BIT             NOT NULL CONSTRAINT [DF_drg_procedure_tillaeg_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]    DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]    DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_procedure_tillaeg] PRIMARY KEY CLUSTERED (
            [takstsystem], [procedure_uuid], [TILLAEGSKODE_PROCEDURE]
        )
    );

    -- Aarsskive-scoped delete-detection filtrerer paa (takstsystem, aktivitetsaar).
    CREATE NONCLUSTERED INDEX [IX_arc_drg_procedure_tillaeg_Aarsskive]
        ON [arc].[drg_procedure_tillaeg] ([takstsystem], [Aktivitetsaar], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_drg_procedure_tillaeg_DW_Timestamp_Update]
        ON [arc].[drg_procedure_tillaeg] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drg_interne_koder  (DRG: INTERNE_KODER)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_interne_koder]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_interne_koder] (
        [takstsystem]         NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]        NVARCHAR(36)    NOT NULL,
        [INTERN_KODE]         NVARCHAR(10)    NOT NULL,
        [INTERN_TYPE]         NVARCHAR(4)     NOT NULL,
        [Aktivitetsaar]       NVARCHAR(500)   NULL,
        [leverance_dato]      DATE            NOT NULL,
        [dw_is_deleted]       BIT             NOT NULL CONSTRAINT [DF_drg_interne_koder_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert] DATETIME2(0)    NOT NULL,
        [dw_timestamp_update] DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_interne_koder] PRIMARY KEY CLUSTERED (
            [takstsystem], [kontakt_uuid], [INTERN_TYPE], [INTERN_KODE]
        )
    );

    -- Aarsskive-scoped delete-detection filtrerer paa (takstsystem, aktivitetsaar).
    CREATE NONCLUSTERED INDEX [IX_arc_drg_interne_koder_Aarsskive]
        ON [arc].[drg_interne_koder] ([takstsystem], [Aktivitetsaar], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_drg_interne_koder_DW_Timestamp_Update]
        ON [arc].[drg_interne_koder] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drg_takstbeskrivelse  (DRG: DRGTAKSTBESKRIVELSE)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_takstbeskrivelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_takstbeskrivelse] (
        [takstsystem]         NVARCHAR(3)     NOT NULL,
        [DRG]                 NVARCHAR(6)     NOT NULL,
        [BESKRIVELSE]         NVARCHAR(500)   NULL,
        [leverance_dato]      DATE            NOT NULL,
        [dw_is_deleted]       BIT             NOT NULL CONSTRAINT [DF_drg_takstbeskrivelse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert] DATETIME2(0)    NOT NULL,
        [dw_timestamp_update] DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_takstbeskrivelse] PRIMARY KEY CLUSTERED (
            [takstsystem], [DRG]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drg_takstbeskrivelse_DW_Timestamp_Update]
        ON [arc].[drg_takstbeskrivelse] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drg_leveranceinfo  (DRG: LEVERANCEINFO)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_leveranceinfo]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_leveranceinfo] (
        [takstsystem]         NVARCHAR(3)     NOT NULL,
        [DATAOMRAADE]         NVARCHAR(3)     NOT NULL,
        [LPROPDATERINGSDATO]  NVARCHAR(10)    NOT NULL,
        [leverance_dato]      DATE            NOT NULL,
        [dw_is_deleted]       BIT             NOT NULL CONSTRAINT [DF_drg_leveranceinfo_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert] DATETIME2(0)    NOT NULL,
        [dw_timestamp_update] DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drg_leveranceinfo] PRIMARY KEY CLUSTERED (
            [takstsystem], [DATAOMRAADE], [LPROPDATERINGSDATO]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drg_leveranceinfo_DW_Timestamp_Update]
        ON [arc].[drg_leveranceinfo] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_kontakt  (DRGPSYK: Psyk_kontakt)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_kontakt]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_kontakt] (
        [takstsystem]                NVARCHAR(3)     NOT NULL,
        [forloeb_noegle]             NVARCHAR(36)    NULL,
        [DW_EK_Helbredsforloeb]      NVARCHAR(500)   NULL,
        [CPRnummer]                  NVARCHAR(500)   NULL,
        [INDTIDSPUNKT_KONTAKT]       NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_KONTAKT]        NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_KONTAKT_KONSTR] NVARCHAR(500)   NULL,
        [KONTAKTTYPE]                NVARCHAR(500)   NULL,
        [PRIORITET]                  NVARCHAR(500)   NULL,
        [KONTAKT_ENHED]              NVARCHAR(500)   NULL,
        [KONTAKT_ENHEDSNAVN]         NVARCHAR(510)   NULL,
        [SPECIALE]                   NVARCHAR(500)   NULL,
        [SYGEHUS]                    NVARCHAR(500)   NULL,
        [SYGEHUSNAVN]                NVARCHAR(510)   NULL,
        [SYGEHUSTYPE]                NVARCHAR(500)   NULL,
        [SYGEHUS_REGION]             NVARCHAR(500)   NULL,
        [BO_KOM]                     NVARCHAR(500)   NULL,
        [BO_REGION]                  NVARCHAR(500)   NULL,
        [BO_KOM_SOURCE]              NVARCHAR(500)   NULL,
        [ALDER_AAR_IND]              NVARCHAR(500)   NULL,
        [ALDER_AAR_UD]               NVARCHAR(500)   NULL,
        [KOEN]                       NVARCHAR(500)   NULL,
        [KONTAKTTYPE_Forloeb]        NVARCHAR(500)   NULL,
        [KONTAKT_AFSLUTTET]          NVARCHAR(500)   NULL,
        [KONTAKT_VAERDI]             NVARCHAR(36)    NOT NULL,
        [Koerselsdato]               NVARCHAR(500)   NULL,
        [DIA01]                      NVARCHAR(500)   NULL,
        [DIA02]                      NVARCHAR(500)   NULL,
        [DIA03]                      NVARCHAR(500)   NULL,
        [DIA04]                      NVARCHAR(500)   NULL,
        [DIA05]                      NVARCHAR(500)   NULL,
        [PROC01]                     NVARCHAR(500)   NULL,
        [PROC02]                     NVARCHAR(500)   NULL,
        [PROC03]                     NVARCHAR(500)   NULL,
        [PROC04]                     NVARCHAR(500)   NULL,
        [PROC05]                     NVARCHAR(500)   NULL,
        [leverance_dato]             DATE            NOT NULL,
        [dw_is_deleted]              BIT             NOT NULL CONSTRAINT [DF_drgpsyk_kontakt_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]        DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]        DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_kontakt] PRIMARY KEY CLUSTERED (
            [takstsystem], [KONTAKT_VAERDI]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_kontakt_DW_Timestamp_Update]
        ON [arc].[drgpsyk_kontakt] ([dw_timestamp_update] DESC);
END
GO

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

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_diagnose  (DRGPSYK: Psyk_Diagnose)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_diagnose]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_diagnose] (
        [takstsystem]         NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]        NVARCHAR(36)    NOT NULL,
        [Diagnose]            NVARCHAR(10)    NOT NULL,
        [ART]                 NVARCHAR(10)    NOT NULL,
        [SIDEANGIVELSE]       NVARCHAR(44)    NOT NULL,
        [leverance_dato]      DATE            NOT NULL,
        [dw_is_deleted]       BIT             NOT NULL CONSTRAINT [DF_drgpsyk_diagnose_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert] DATETIME2(0)    NOT NULL,
        [dw_timestamp_update] DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_diagnose] PRIMARY KEY CLUSTERED (
            [takstsystem], [kontakt_uuid], [Diagnose], [ART], [SIDEANGIVELSE]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_diagnose_DW_Timestamp_Update]
        ON [arc].[drgpsyk_diagnose] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_diagnose_tillaeg  (DRGPSYK: Psyk_Diagnose_tillaeg)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_diagnose_tillaeg]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_diagnose_tillaeg] (
        [takstsystem]           NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]          NVARCHAR(36)    NOT NULL,
        [Diagnose]              NVARCHAR(10)    NOT NULL,
        [ART]                   NVARCHAR(10)    NOT NULL,
        [SIDEANGIVELSE]         NVARCHAR(44)    NOT NULL,
        [TILLAEGSKODE_DIAGNOSE] NVARCHAR(10)    NOT NULL,
        [leverance_dato]        DATE            NOT NULL,
        [dw_is_deleted]         BIT             NOT NULL CONSTRAINT [DF_drgpsyk_diagnose_tillaeg_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_diagnose_tillaeg] PRIMARY KEY CLUSTERED (
            [takstsystem], [kontakt_uuid], [Diagnose], [ART], [SIDEANGIVELSE], [TILLAEGSKODE_DIAGNOSE]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_diagnose_tillaeg_DW_Timestamp_Update]
        ON [arc].[drgpsyk_diagnose_tillaeg] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_procedure  (DRGPSYK: Psyk_procedure)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_procedure]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_procedure] (
        [takstsystem]              NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]             NVARCHAR(36)    NOT NULL,
        [PROCEDUREKODE]            NVARCHAR(500)   NULL,
        [SIDEANGIVELSE]            NVARCHAR(500)   NULL,
        [HANDLINGSSPEC]            NVARCHAR(500)   NULL,
        [KONTRAST]                 NVARCHAR(500)   NULL,
        [PROCEDURE_STARTTIDSPUNKT] NVARCHAR(500)   NULL,
        [PROCEDURE_SLUTTIDSPUNKT]  NVARCHAR(500)   NULL,
        [PRODUCERENDE_ENHED]       NVARCHAR(500)   NULL,
        [PRODUCERENDE_SYGEHUS]     NVARCHAR(500)   NULL,
        [PROCEDURE_VAERDI]         NVARCHAR(36)    NOT NULL,
        [leverance_dato]           DATE            NOT NULL,
        [dw_is_deleted]            BIT             NOT NULL CONSTRAINT [DF_drgpsyk_procedure_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]      DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_procedure] PRIMARY KEY CLUSTERED (
            [takstsystem], [PROCEDURE_VAERDI]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_procedure_DW_Timestamp_Update]
        ON [arc].[drgpsyk_procedure] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_procedure_tillaeg  (DRGPSYK: Psyk_procedure_tillaeg)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_procedure_tillaeg]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_procedure_tillaeg] (
        [takstsystem]            NVARCHAR(3)     NOT NULL,
        [procedure_uuid]         NVARCHAR(36)    NOT NULL,
        [kontakt_uuid]           NVARCHAR(36)    NOT NULL,
        [TILLAEGSKODE_PROCEDURE] NVARCHAR(10)    NOT NULL,
        [leverance_dato]         DATE            NOT NULL,
        [dw_is_deleted]          BIT             NOT NULL CONSTRAINT [DF_drgpsyk_procedure_tillaeg_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]    DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]    DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_procedure_tillaeg] PRIMARY KEY CLUSTERED (
            [takstsystem], [procedure_uuid], [TILLAEGSKODE_PROCEDURE]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_procedure_tillaeg_DW_Timestamp_Update]
        ON [arc].[drgpsyk_procedure_tillaeg] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_betalingsoplysninger  (DRGPSYK: Psyk_Betalingsoplysninger)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_betalingsoplysninger]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_betalingsoplysninger] (
        [takstsystem]            NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]           NVARCHAR(36)    NOT NULL,
        [Bet_opl_starttidspunkt] NVARCHAR(30)    NOT NULL,
        [Bet_opl_sluttidspunkt]  NVARCHAR(500)   NULL,
        [BETALINGSAFTALE]        NVARCHAR(500)   NULL,
        [Betaler]                NVARCHAR(500)   NULL,
        [SPECIALISERINGSNIVEAU]  NVARCHAR(500)   NULL,
        [INDB_TIDSPUNKT]         NVARCHAR(500)   NULL,
        [leverance_dato]         DATE            NOT NULL,
        [dw_is_deleted]          BIT             NOT NULL CONSTRAINT [DF_drgpsyk_betalingsoplysninger_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]    DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]    DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_betalingsoplysninger] PRIMARY KEY CLUSTERED (
            [takstsystem], [kontakt_uuid], [Bet_opl_starttidspunkt]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_betalingsoplysninger_DW_Timestamp_Update]
        ON [arc].[drgpsyk_betalingsoplysninger] ([dw_timestamp_update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.drg_pris_revision — insert-only log over aendrede pris-/takstkolonner.
-- En raekke pr. aendret kolonne pr. leverance (HAIBA-moenstret). Skrives af
-- drg_archive.py FOER mergen.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drg_pris_revision]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drg_pris_revision] (
        [revision_id]         BIGINT IDENTITY(1,1) NOT NULL,
        [kildetabel]          NVARCHAR(60)  NOT NULL,
        [takstsystem]         NVARCHAR(3)   NOT NULL,
        [noegle]              NVARCHAR(36)  NOT NULL,  -- kontakt-/forloebs-UUID
        [kolonne]             NVARCHAR(60)  NOT NULL,
        [vaerdi_foer]         NVARCHAR(500) NULL,
        [vaerdi_efter]        NVARCHAR(500) NULL,
        [leverance_dato]      DATE          NOT NULL,
        [dw_timestamp_insert] DATETIME2(0)  NOT NULL CONSTRAINT [DF_drg_pris_revision_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_arc_drg_pris_revision] PRIMARY KEY CLUSTERED ([revision_id])
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drg_pris_revision_Noegle]
        ON [arc].[drg_pris_revision] ([kildetabel], [takstsystem], [noegle]);
END
GO
