-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
