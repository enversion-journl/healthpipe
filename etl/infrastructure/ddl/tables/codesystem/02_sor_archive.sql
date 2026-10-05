-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- SOR archive: arc.sor_register
--
-- Persistent DFM archive of the raw SOR register deliveries (ext.sor_register).
-- Grain: one row per SOR entity VERSION — (code, from_date). The daily extract
-- ("current" mode, SorEntity_daily.csv) upserts the current/future versions;
-- an "all_historical" run (Sor_complete.zip) backfills every version ever
-- published. Rows are never deleted (unlimited retention) — entity closure is
-- carried by to_date on the current version (cs_sor projects it as the
-- closingDate property), not by row removal.
--
-- Downstream (cs_sor) reads the CURRENT version per code:
--   ROW_NUMBER() OVER (PARTITION BY code ORDER BY from_date DESC) = 1
-- which the clustered PK (code, from_date) serves directly.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[sor_register]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[sor_register] (
        [code]                NVARCHAR(100) NOT NULL,
        [from_date]           NVARCHAR(50)  NOT NULL,
        [to_date]             NVARCHAR(50)  NULL,
        [display]             NVARCHAR(500) NULL,
        [parent_code]         NVARCHAR(500) NULL,
        [entityType]          NVARCHAR(500) NULL,
        [entityTypeId]        NVARCHAR(500) NULL,
        [municipality]        NVARCHAR(500) NULL,
        [region]              NVARCHAR(500) NULL,
        [owner]               NVARCHAR(500) NULL,
        [ownerId]             NVARCHAR(500) NULL,
        [cvrNumber]           NVARCHAR(500) NULL,
        [website]             NVARCHAR(500) NULL,
        [email]               NVARCHAR(500) NULL,
        [phone]               NVARCHAR(500) NULL,
        [address]             NVARCHAR(500) NULL,
        [postalCode]          NVARCHAR(500) NULL,
        [city]                NVARCHAR(500) NULL,
        [openingDate]         NVARCHAR(500) NULL,
        [dw_timestamp_insert] DATETIME2(0)  NOT NULL,
        [dw_timestamp_update] DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_sor_register] PRIMARY KEY CLUSTERED ([code], [from_date])
    );
END
GO
