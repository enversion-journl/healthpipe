-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260707 (US 28010 / PR 11799): Drop the stale sec.RolleDomaene shape so the
-- base DDL can recreate it as the canonical composite table.
--
-- Existing QA/prod environments deployed sec.RolleDomaene from the earlier
-- 01_tables.sql shape: 7 columns incl. a RolleDomaeneId INT IDENTITY surrogate
-- PK. PR 11799 redefined the table in tables/security/04_dpa_tables.sql as a
-- 6-column composite (PK = Bruger, Sikkerhedsdomaene, no IDENTITY), but the
-- base DDL create is guarded by IF NOT EXISTS, so redeploying never reshaped
-- the already-existing table. The DSO sync (dso_sync_sec_rolledomaene) then
-- fails with
--   "Cannot insert explicit value for identity column in table '#_sync_staging'"
-- because SELECT ... INTO #_sync_staging copies the IDENTITY property and the
-- OPENQUERY INSERT supplies explicit values for every column.
--
-- This migration only DROPs the stale table (creates belong in DDL, not
-- migrations). The canonical table is recreated by the base DDL in
-- tables/security/04_dpa_tables.sql, which runs after this migration:
-- staging_infra_security and dso_infra_security both depend on
-- {staging,dso}_migrations, so the drop is ordered before the recreate.
--
-- Safe to drop the table: sec.RolleDomaene is fully refreshed every night from
-- AD via truncate_insert (populate_security -> dso_sync_sec_rolledomaene), and
-- the dependent objects (sec.HentBrugerRolleDomaene, the vw_dim masking views)
-- are not SCHEMABOUND, so they do not block DROP TABLE.
--
-- On fresh installs and on environments already on the composite shape this is
-- a no-op: COL_LENGTH returns NULL when the table or the RolleDomaeneId column
-- does not exist.
-- ============================================================================

IF COL_LENGTH(N'sec.RolleDomaene', N'RolleDomaeneId') IS NOT NULL
BEGIN
    DROP TABLE [sec].[RolleDomaene];
    PRINT 'Dropped stale sec.RolleDomaene (RolleDomaeneId IDENTITY surrogate shape); base DDL will recreate it as the composite table';
END
GO
