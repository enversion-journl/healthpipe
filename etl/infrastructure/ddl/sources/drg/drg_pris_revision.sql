-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
