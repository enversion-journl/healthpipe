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
-- mergen med truncation (HAIBA-laere); kun noeglekolonner er
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
