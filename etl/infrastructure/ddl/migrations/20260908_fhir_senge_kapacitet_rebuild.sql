-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Senge/kapacitet rebuild: drop the five flat tables and let base DDL rebuild
--
--   fhir.WardUnit, fhir.WardUnit_Capacity, fhir.Bed,
--   fhir.Encounter_BedSpace, fhir.Encounter_Absence
--
-- These tables have only ever existed on dev, so there is nothing to preserve
-- and no consumer to migrate. They replace four incremental migrations
-- (create, rename to English, dw_is_deleted NOT NULL, ward_unit_code) that
-- built the same shape in steps and left two problems behind:
--
--   * sp_rename does NOT rename constraints or indexes. A database that ran
--     the rename kept PK_FHIR_Sengeenhed / DF_Sengeenhed_dw_is_deleted /
--     IX_FHIR_Sengeenhed_*, while a freshly provisioned database gets the
--     WardUnit names from base DDL. Exactly the drift
--     20260825b_name_default_constraints.sql cleaned up for 504 constraints,
--     and a later guarded migration that looks up a constraint by name would
--     quietly no-op on one of the two.
--   * The step order became a deployment precondition: the migrations had to
--     run before the SoF job, or the MERGE failed on column names.
--
-- The migration only DROPs. tables/fhir/01_tables.sql is the single source of
-- truth for the shape and recreates all five with their canonical constraint
-- and index names on the next infrastructure deployment — the table DDL assets
-- depend on the migration assets, so the order holds by construction.
--
-- The rows come back on the next SoF run, which re-extracts them from Aidbox.
-- Capacity Observations must be reposted first: their business key is now
-- formatted as explicit ISO 8601 ({type}:{sengeenhed_id}:{fra_iso} with a T,
-- not a space), so the resources carry new identifiers.
--
-- Per table, guarded so a re-run is a no-op and the staging/DSO split is safe
-- (a table absent on this database is skipped).
-- ============================================================================
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[fhir].[Encounter_Absence]', N'U') IS NOT NULL
    DROP TABLE [fhir].[Encounter_Absence];
GO

IF OBJECT_ID(N'[fhir].[Encounter_BedSpace]', N'U') IS NOT NULL
    DROP TABLE [fhir].[Encounter_BedSpace];
GO

IF OBJECT_ID(N'[fhir].[Bed]', N'U') IS NOT NULL
    DROP TABLE [fhir].[Bed];
GO

IF OBJECT_ID(N'[fhir].[WardUnit_Capacity]', N'U') IS NOT NULL
    DROP TABLE [fhir].[WardUnit_Capacity];
GO

IF OBJECT_ID(N'[fhir].[WardUnit]', N'U') IS NOT NULL
    DROP TABLE [fhir].[WardUnit];
GO
