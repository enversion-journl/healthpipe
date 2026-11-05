-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_procedure_tillaeg  (DRGPSYK: Psyk_procedure_tillaeg)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_procedure_tillaeg]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_procedure_tillaeg] (
        [takstsystem]            NVARCHAR(3)     NOT NULL,
        [procedure_uuid]         NVARCHAR(36)    NOT NULL,
        [kontakt_uuid]           NVARCHAR(36)    NOT NULL,
        [TILLAEGSKODE_PROCEDURE] NVARCHAR(10)    NOT NULL,
        [leverance_dato]         DATE            NOT NULL,
        [dw_is_deleted]          BIT             NOT NULL CONSTRAINT [DF_drgpsyk_procedure_tillaeg_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]    DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]    DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_procedure_tillaeg] PRIMARY KEY CLUSTERED (
            [takstsystem], [procedure_uuid], [TILLAEGSKODE_PROCEDURE]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_procedure_tillaeg_DW_Timestamp_Update]
        ON [arc].[drgpsyk_procedure_tillaeg] ([dw_timestamp_update] DESC);
END
GO
