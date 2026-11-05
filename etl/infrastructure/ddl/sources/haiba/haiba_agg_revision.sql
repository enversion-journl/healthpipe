-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
