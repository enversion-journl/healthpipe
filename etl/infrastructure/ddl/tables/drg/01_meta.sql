-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- meta.DRGLeverance — which delivery the DRG archive currently reflects.
--
-- One row per (kildetabel, takstsystem, aktivitetsaar, leverance_ts): the
-- ACCEPTED deliveries, at aarsskive-grain. A leverance dækker flere skiver (M
-- bærer tre aktivitetsaar, U to), og tier 2 i leverancedetektionen afgør pr.
-- skive, om leverancen er nyere end det, arkivet allerede afspejler — kun de
-- accepterede skiver får en række her. "Hvilken cap afspejler skive X?" er
-- derfor MAX(lpropdateringsdato) for den skive.
--
-- Aktivitetsaar er '' for de datasæt der ikke har et aktivitetsaar (psyk,
-- takstbeskrivelse, leveranceinfo) — samme ''-frem-for-NULL-konvention som
-- arkivets nøglekolonner, så nøglen aldrig indeholder NULL.
--
-- kildetabel BLIVER i nøglen: en delvist lykket kørsel (nogle af
-- leverancetypens tabeller merget) må ikke registrere en cap, de øvrige
-- tabeller ikke afspejler.
--
-- cap_kilde er 'reel' (LPROPDATERINGSDATO fra leverancens LEVERANCEINFO) eller
-- 'afledt' (filnavnets timestamp, brugt når LEVERANCEINFO mangler). Se
-- docs/source_systems/drg/drg_leverancedetektion_beslutningsnotat.md.
--
-- leverance_ts er filnavnets PAKKE-timestamp og er med i noeglen, ikke
-- leverance_dato: to leverancer kan pakkes samme dato (det er praecis derfor
-- tier 1 bruger det fulde timestamp), og med datoen i noeglen ville den anden
-- overskrive den foerstes registrering.
--
-- leverance_dato er PAKKEdatoen (provenance), IKKE datacap'en — de to er
-- forskellige for M-leverancer, og det er netop derfor cap'en har sin egen
-- kolonne.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'DRGLeverance'
)
BEGIN
    CREATE TABLE [meta].[DRGLeverance] (
        [kildetabel]           NVARCHAR(60)  NOT NULL,
        [takstsystem]          NVARCHAR(3)   NOT NULL,
        [aktivitetsaar]        NVARCHAR(4)   NOT NULL
            CONSTRAINT [DF_DRGLeverance_aktivitetsaar] DEFAULT (''),
        [leverance_ts]         DATETIME2(0)  NOT NULL,
        [leverance_dato]       DATE          NOT NULL,
        [kadence]              NVARCHAR(1)   NULL,
        [lpropdateringsdato]   NVARCHAR(10)  NULL,
        [cap_kilde]            NVARCHAR(6)   NULL,
        [indlaesningstype]     NVARCHAR(20)  NOT NULL,
        [raekker_indsat]       INT           NULL,
        [raekker_opdateret]    INT           NULL,
        [raekker_soft_slettet] INT           NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_DRGLeverance_dw_timestamp_insert] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_DRGLeverance_dw_timestamp_update] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        CONSTRAINT [PK_drg_meta_leverance] PRIMARY KEY CLUSTERED (
            [kildetabel], [takstsystem], [aktivitetsaar], [leverance_ts]
        )
    );
END
GO
