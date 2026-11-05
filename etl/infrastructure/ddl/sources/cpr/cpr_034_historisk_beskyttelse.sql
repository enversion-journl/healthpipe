-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_034_historisk_beskyttelse  (record type 034, 37 characters)
-- Historical protections with periods.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_034_historisk_beskyttelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_034_historisk_beskyttelse] (
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [beskyttype]            NVARCHAR(4)   NOT NULL,
        [start_dt_beskyttelse]  NVARCHAR(10)  NOT NULL,
        [slut_dt_beskyttelse]   NVARCHAR(10)  NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_034_historisk_beskyttelse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        -- No primary key: the source does not identify its rows
        -- uniquely here. Measured on the deliveries, even the widest
        -- combination of source columns was not unique. The rows are
        -- therefore replaced entirely for the persons a delivery
        -- contains, and the index exists to make that replacement fast.
        INDEX [IX_arc_cpr_034_historisk_beskyttelse_person] CLUSTERED ([pnr], [beskyttype], [start_dt_beskyttelse])
    );
END
GO
