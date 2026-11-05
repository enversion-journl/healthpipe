-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_022_historisk_personnummer  (record type 022, 45 characters)
-- Historical personal numbers. The bridge between an old and a current number.
-- pnrgaeld (position 4-13) is the current number and pnr the historical one;
-- the person behind the row is pnrgaeld, and delete detection is limited by it.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_022_historisk_personnummer]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_022_historisk_personnummer] (
        [pnrgaeld]              NVARCHAR(10)  NOT NULL,
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [start_dt_person]       NVARCHAR(10)  NULL,
        [start_dt_umrk_person]  NVARCHAR(1)   NULL,
        [slut_dt_person]        NVARCHAR(10)  NULL,
        [slut_dt_umrk_person]   NVARCHAR(1)   NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_022_historisk_personnummer_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_022_historisk_personnummer] PRIMARY KEY CLUSTERED (
            [pnr], [pnrgaeld]
        )
    );
END
GO
