-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

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
