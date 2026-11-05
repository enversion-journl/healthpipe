-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
