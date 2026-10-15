-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- EPJI location archive: arc.epji_location
--
-- Persistent archive of the EPJI location + location_identifier full snapshot
-- (ext.epji_location). Grain: one row per EPJI LOCATION.EXTERNAL_ID. Unlike
-- arc.sor_register (a cumulative version file that never deletes), every
-- extract run here is a genuine full snapshot of current EPJI state, so the
-- MERGE runs with detect_deletes = true: a location absent from the latest
-- snapshot is soft-deleted (dw_is_deleted = 1) rather than silently dropped.
-- A location that reappears in a later snapshot is un-deleted automatically.
--
-- Downstream (cs_shakalike) reads only the live rows:
--   WHERE dw_is_deleted = 0
--
-- Scope: STAGING only. This file lives under tables/epji_archive/, its own
-- staging-only folder (only staging_infra_epji_archive executes it — same
-- pattern as tables/ifdb_archive/), so no DB_NAME guard is needed: DSO simply
-- never runs this file.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[epji_location]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[epji_location] (
        [code]                 NVARCHAR(100) NOT NULL,
        [location_pk]          NVARCHAR(100) NULL,
        [display]              NVARCHAR(500) NULL,
        [sor_code]             NVARCHAR(100) NULL,
        [identifier_system]    NVARCHAR(500) NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_arc_epji_location_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_epji_location] PRIMARY KEY CLUSTERED ([code])
    );
END
GO
