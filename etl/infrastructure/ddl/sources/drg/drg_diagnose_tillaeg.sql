-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
