-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
