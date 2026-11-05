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
