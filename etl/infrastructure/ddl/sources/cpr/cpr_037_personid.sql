-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_037_personid  (record type 037, 49 characters)
-- PersonID: stable identity across changes of personal number.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_037_personid]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_037_personid] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [person_id]            NVARCHAR(36)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_037_personid_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_037_personid] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
