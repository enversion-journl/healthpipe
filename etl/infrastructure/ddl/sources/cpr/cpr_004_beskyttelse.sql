-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_004_beskyttelse  (record type 004, 37 characters)
-- Current protection. Up to three types per person, hence the type in the key.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_004_beskyttelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_004_beskyttelse] (
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [beskyttype]            NVARCHAR(4)   NOT NULL,
        [start_dt_beskyttelse]  NVARCHAR(10)  NULL,
        [slet_dt_beskyttelse]   NVARCHAR(10)  NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_004_beskyttelse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_004_beskyttelse] PRIMARY KEY CLUSTERED (
            [pnr], [beskyttype]
        )
    );
END
GO
