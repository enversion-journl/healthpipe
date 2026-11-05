-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_008_navn  (record type 008, 193 characters)
-- Current names: first, middle and last name, and addressing name.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_008_navn]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_008_navn] (
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [fornvn]                NVARCHAR(50)  NULL,
        [fornvn_mrk]            NVARCHAR(1)   NULL,
        [melnvn]                NVARCHAR(40)  NULL,
        [melnvn_mrk]            NVARCHAR(1)   NULL,
        [efternvn]              NVARCHAR(40)  NULL,
        [efternvn_mrk]          NVARCHAR(1)   NULL,
        [nvnhaenstart]          NVARCHAR(12)  NULL,
        [haenstart_umrk_navne]  NVARCHAR(1)   NULL,
        [adrnvn]                NVARCHAR(34)  NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_008_navn_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_008_navn] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
