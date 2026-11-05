-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_010_statsborgerskab  (record type 010, 30 characters)
-- Current citizenship as country code.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_010_statsborgerskab]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_010_statsborgerskab] (
        [pnr]                             NVARCHAR(10)  NOT NULL,
        [landekode]                       NVARCHAR(4)   NULL,
        [haenstart_statsborgerskab]       NVARCHAR(12)  NULL,
        [haenstart_umrk_statsborgerskab]  NVARCHAR(1)   NULL,
        [leverance_dato]                  DATE          NOT NULL,
        [dw_is_deleted]                   BIT           NOT NULL CONSTRAINT [DF_cpr_010_statsborgerskab_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]             DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_010_statsborgerskab] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
