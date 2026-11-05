-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_099_haendelse  (record type 099, 45 characters)
-- Events affecting the person during the day. A log: new events are
-- appended, nothing is updated or deleted.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_099_haendelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_099_haendelse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [timestampu]           NVARCHAR(12)  NOT NULL,
        [haendelse]            NVARCHAR(3)   NOT NULL,
        [afledtmrk]            NVARCHAR(2)   NOT NULL,
        [nglkonst]             NVARCHAR(15)  NOT NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_099_haendelse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_099_haendelse] PRIMARY KEY CLUSTERED (
            [pnr], [timestampu], [haendelse], [afledtmrk], [nglkonst]
        )
    );
END
GO
