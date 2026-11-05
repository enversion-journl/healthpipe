-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_procedure  (DRGPSYK: Psyk_procedure)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_procedure]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_procedure] (
        [takstsystem]              NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]             NVARCHAR(36)    NOT NULL,
        [PROCEDUREKODE]            NVARCHAR(500)   NULL,
        [SIDEANGIVELSE]            NVARCHAR(500)   NULL,
        [HANDLINGSSPEC]            NVARCHAR(500)   NULL,
        [KONTRAST]                 NVARCHAR(500)   NULL,
        [PROCEDURE_STARTTIDSPUNKT] NVARCHAR(500)   NULL,
        [PROCEDURE_SLUTTIDSPUNKT]  NVARCHAR(500)   NULL,
        [PRODUCERENDE_ENHED]       NVARCHAR(500)   NULL,
        [PRODUCERENDE_SYGEHUS]     NVARCHAR(500)   NULL,
        [PROCEDURE_VAERDI]         NVARCHAR(36)    NOT NULL,
        [leverance_dato]           DATE            NOT NULL,
        [dw_is_deleted]            BIT             NOT NULL CONSTRAINT [DF_drgpsyk_procedure_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]      DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_procedure] PRIMARY KEY CLUSTERED (
            [takstsystem], [PROCEDURE_VAERDI]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_procedure_DW_Timestamp_Update]
        ON [arc].[drgpsyk_procedure] ([dw_timestamp_update] DESC);
END
GO
