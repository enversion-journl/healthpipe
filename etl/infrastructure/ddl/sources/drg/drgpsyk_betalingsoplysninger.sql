-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_betalingsoplysninger  (DRGPSYK: Psyk_Betalingsoplysninger)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_betalingsoplysninger]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_betalingsoplysninger] (
        [takstsystem]            NVARCHAR(3)     NOT NULL,
        [kontakt_uuid]           NVARCHAR(36)    NOT NULL,
        [Bet_opl_starttidspunkt] NVARCHAR(30)    NOT NULL,
        [Bet_opl_sluttidspunkt]  NVARCHAR(500)   NULL,
        [BETALINGSAFTALE]        NVARCHAR(500)   NULL,
        [Betaler]                NVARCHAR(500)   NULL,
        [SPECIALISERINGSNIVEAU]  NVARCHAR(500)   NULL,
        [INDB_TIDSPUNKT]         NVARCHAR(500)   NULL,
        [leverance_dato]         DATE            NOT NULL,
        [dw_is_deleted]          BIT             NOT NULL CONSTRAINT [DF_drgpsyk_betalingsoplysninger_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]    DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]    DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_betalingsoplysninger] PRIMARY KEY CLUSTERED (
            [takstsystem], [kontakt_uuid], [Bet_opl_starttidspunkt]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_betalingsoplysninger_DW_Timestamp_Update]
        ON [arc].[drgpsyk_betalingsoplysninger] ([dw_timestamp_update] DESC);
END
GO
