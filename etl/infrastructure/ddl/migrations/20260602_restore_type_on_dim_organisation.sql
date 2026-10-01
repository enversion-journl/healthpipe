-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Restore [Type] column on dim.Organisation (SOR EntityTypeName).
--
-- The dim.Okosted extraction (20260518_cleanup_okosted_from_organisation.sql)
-- dropped [Type] together with the OKOSTED-specific columns. [Type] is a
-- legitimate SOR attribute (sor.SOR_registry.EntityTypeName), not OKOSTED-
-- specific, and is still produced by stg_organisation_sor — so it is restored
-- here. dim_organisation backfills the values on its next materialisation.
--
-- Upgrade path for EXISTING databases only. On a fresh DB the base table DDL
-- (tables/organisation/02_dims.sql) already declares [Type], and migrations run
-- BEFORE the organisation tables are created, so the step is guarded with
-- OBJECT_ID(...) IS NOT NULL to no-op when the table does not yet exist.
-- Runs against both staging and DSO via the central migrations loader.
-- Safe to re-run: the step is guarded.
-- ============================================================================

IF OBJECT_ID(N'[dim].[Organisation]') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'Type'
   )
BEGIN
    ALTER TABLE [dim].[Organisation] ADD [Type] [varchar](100) NULL;
    PRINT 'Restored Type on dim.Organisation';
END
GO
