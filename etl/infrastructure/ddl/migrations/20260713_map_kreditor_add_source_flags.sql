-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Add sticky dw_source_<system> flags to map.Kreditor.
--
-- WHY: map.Kreditor becomes the CVR registry for the delta-driven Kreditor
--   pipeline: stg.dim_Kreditor stages the ARCHIVE DELTA (changed CVRs +
--   per-source flags, filtered on the dim.Kreditor checkpoint) and MERGEs
--   into the map with STICKY flags (never 1 -> 0); every staged CVR gets
--   dw_timestamp_update stamped — making the map the complete change signal
--   for dim_Kreditor's delta selection.
--
--   Existing rows start with flags = 0; the first MERGE after deploy flips
--   them from the full archive picture (stamping dw_timestamp_update), which
--   makes the first dim_Kreditor run a de-facto full refresh. Self-initialising.
--
--   Staging-side only in effect: map.* tables are not DSO-synced, but the
--   file runs harmlessly on both servers (guards no-op where the table is
--   absent).
--
-- Idempotent: safe to re-run.
-- ============================================================================

IF OBJECT_ID(N'[map].[Kreditor]') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'dw_source_sydax')
        ALTER TABLE [map].[Kreditor] ADD [dw_source_sydax] BIT NOT NULL DEFAULT 0;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'dw_source_ils')
        ALTER TABLE [map].[Kreditor] ADD [dw_source_ils] BIT NOT NULL DEFAULT 0;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = 'dw_source_mysupply')
        ALTER TABLE [map].[Kreditor] ADD [dw_source_mysupply] BIT NOT NULL DEFAULT 0;
END
GO
