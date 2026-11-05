-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_012_civilstand  (record type 012, 95 characters)
-- Current marital status, including the personal number of the spouse.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_012_civilstand]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_012_civilstand] (
        [pnr]                        NVARCHAR(10)  NOT NULL,
        [civst]                      NVARCHAR(1)   NULL,
        [aegtepnr]                   NVARCHAR(10)  NULL,
        [aegtefoed_dt]               NVARCHAR(10)  NULL,
        [aegtefoeddt_umrk]           NVARCHAR(1)   NULL,
        [aegtenvn]                   NVARCHAR(34)  NULL,
        [aegtenvn_mrk]               NVARCHAR(1)   NULL,
        [haenstart_civilstand]       NVARCHAR(12)  NULL,
        [haenstart_umrk_civilstand]  NVARCHAR(1)   NULL,
        [sep_henvis_civilstand]      NVARCHAR(12)  NULL,
        [leverance_dato]             DATE          NOT NULL,
        [dw_is_deleted]              BIT           NOT NULL CONSTRAINT [DF_cpr_012_civilstand_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]        DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]        DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_012_civilstand] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
