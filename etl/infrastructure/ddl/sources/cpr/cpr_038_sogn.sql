-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_038_sogn  (record type 038, 77 characters)
-- Current parish of residence.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_038_sogn]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_038_sogn] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [sognekode]            NVARCHAR(4)   NULL,
        [sognenvn]             NVARCHAR(60)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_038_sogn_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_038_sogn] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
