-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_015_foraeldre  (record type 015, 147 characters)
-- Mother and father: personal number, name and date of birth.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_015_foraeldre]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_015_foraeldre] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [mor_dt]               NVARCHAR(10)  NULL,
        [mor_dt_umrk]          NVARCHAR(1)   NULL,
        [pnrmor]               NVARCHAR(10)  NULL,
        [mor_foed_dt]          NVARCHAR(10)  NULL,
        [mor_foed_dt_umrk]     NVARCHAR(1)   NULL,
        [mornvn]               NVARCHAR(34)  NULL,
        [mornvn_mrk]           NVARCHAR(1)   NULL,
        [far_dt]               NVARCHAR(10)  NULL,
        [far_dt_umrk]          NVARCHAR(1)   NULL,
        [pnrfar]               NVARCHAR(10)  NULL,
        [far_foed_dt]          NVARCHAR(10)  NULL,
        [far_foed_dt_umrk]     NVARCHAR(1)   NULL,
        [farnvn]               NVARCHAR(34)  NULL,
        [farnvn_mrk]           NVARCHAR(1)   NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_015_foraeldre_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_015_foraeldre] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
