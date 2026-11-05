-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_005_udrejse  (record type 005, 200 characters)
-- Emigration details for persons with status 80.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_005_udrejse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_005_udrejse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [udr_landekod]         NVARCHAR(4)   NULL,
        [udrdto]               NVARCHAR(12)  NULL,
        [udrdto_umrk]          NVARCHAR(1)   NULL,
        [udlandadr1]           NVARCHAR(34)  NULL,
        [udlandadr2]           NVARCHAR(34)  NULL,
        [udlandadr3]           NVARCHAR(34)  NULL,
        [udlandadr4]           NVARCHAR(34)  NULL,
        [udlandadr5]           NVARCHAR(34)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_005_udrejse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_005_udrejse] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
