-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260814 (AB#28846): Drop the stale sec.RBOrganisation shape so the base DDL
-- can recreate it as the swap-load trio clustered on the DPA access path.
--
-- Existing environments deployed sec.RBOrganisation from the earlier
-- 01_tables.sql shape: RBOrganisationId INT IDENTITY as clustered PK, a
-- dw_is_deleted column, plus a separate UNIQUE (bruger, DW_EK_RBOrganisation)
-- and IX'er på bruger og DW_EK_RBOrganisation. 01_tables.sql now defines the
-- canonical shape: composite clustered PK (bruger, DW_EK_RBOrganisation), no
-- IDENTITY, no dw_is_deleted (indholdet overskrives fuldt hver kørsel via
-- swap-load) — plus de strukturelt identiske swap-tabeller
-- RBOrganisation_New/_Old og loggen RBOrganisationLoadLog.
--
-- This migration only DROPs stale-shaped tables (creates belong in DDL, not
-- migrations). The base DDL in tables/security/01_tables.sql recreates them:
-- staging_infra_security and dso_infra_security both depend on
-- {staging,dso}_migrations, so the drop is ordered before the recreate.
-- DROP TABLE removes all constraints/indexes regardless of the environment's
-- index names (20260708_rename_90_indexes lowercased them in deployed envs).
-- _New/_Old droppes sammen med den levende tabel så de tre aldrig kan have
-- divergerende form (ALTER TABLE ... SWITCH kræver strukturel identitet).
--
-- Safe to drop: sec.RBOrganisation is fully refreshed from AD via swap-load
-- (populate_security -> dso_sync_sec_rborganisation), and the dependents
-- (sec.HentRBOrganisation, reference views) are not SCHEMABOUND. The table is
-- EMPTY until the next security_daily_job run (03:00) — run the job manually
-- after deploy if the gap matters (DPA fails closed: no rows = masked data
-- for K-users).
--
-- On fresh installs and already-reshaped environments this is a no-op:
-- COL_LENGTH returns NULL when the table or the column does not exist.
-- ============================================================================

IF COL_LENGTH(N'sec.RBOrganisation', N'RBOrganisationId') IS NOT NULL
   OR COL_LENGTH(N'sec.RBOrganisation', N'dw_is_deleted') IS NOT NULL
BEGIN
    DROP TABLE [sec].[RBOrganisation];
    IF OBJECT_ID(N'[sec].[RBOrganisation_New]', 'U') IS NOT NULL
        DROP TABLE [sec].[RBOrganisation_New];
    IF OBJECT_ID(N'[sec].[RBOrganisation_Old]', 'U') IS NOT NULL
        DROP TABLE [sec].[RBOrganisation_Old];
    PRINT 'Dropped stale sec.RBOrganisation shape (IDENTITY/dw_is_deleted); base DDL recreates the swap-load trio clustered on (bruger, DW_EK_RBOrganisation)';
END
GO
