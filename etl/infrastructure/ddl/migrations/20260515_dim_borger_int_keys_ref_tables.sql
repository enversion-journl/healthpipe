-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Borger pipeline rebuild: INT surrogate keys, ref-table architecture,
-- fhir.Patient_Address sidecar
--
-- Drop-and-rebuild migration that aligns the Borger pipeline with the
-- lineage in `data_lineage/dim/dim_borger.yml` (commit 80aa970d):
--
--   * dim.Borger keys move from BIGINT to INT; CleanCode_Borger removed.
--     Mapping between fhir.Patient.patient_id and INT surrogates moves
--     into the new ref schema (ref.Patient_Borger + ref.Patient_Address_Borger).
--   * fhir.Patient schema replaced — address columns move to new
--     fhir.Patient_Address sidecar; identifier and demographic columns
--     align with dk-core profile.
--   * Legacy IFDB-direct path dropped: stg.Borger and map.Borger removed.
--
-- The migration only DROPs old objects. Base DDL files
-- (tables/shared_dimensions/02_dims.sql, tables/fhir/01_tables.sql,
-- tables/ref/01_tables.sql, common/functions/GetBorgerSK_TVF.sql) recreate
-- everything with the new schema on the next infrastructure deployment.
--
-- The sentinel rows (-1 = UNKNOWN, -2 = N/A) are seeded inside the new
-- dim.Borger CREATE TABLE block in 02_dims.sql — not here.
--
-- Idempotent: IF OBJECT_ID / IF EXISTS guards on every operation.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Drop dependent views first (CREATE OR ALTER will reinstate them with
--    the new column set on next deployment)
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[vw_dim].[Borger]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_dim].[Borger];
    PRINT 'Dropped view vw_dim.Borger (will be recreated by views/security/01_vw_dim_security.sql)';
END
GO

IF OBJECT_ID(N'[vw_fhir].[Patient]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_fhir].[Patient];
    PRINT 'Dropped view vw_fhir.Patient (will be recreated by views/fhir/* on next deployment)';
END
GO

-- ----------------------------------------------------------------------------
-- 2. Drop GetBorgerSK_TVF — signature changes from BIGINT to INT.
--    Recreated by common/functions/GetBorgerSK_TVF.sql via dso_infra_functions /
--    staging_infra_functions.
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[dbo].[GetBorgerSK_TVF]', 'IF') IS NOT NULL
BEGIN
    DROP FUNCTION [dbo].[GetBorgerSK_TVF];
    PRINT 'Dropped function dbo.GetBorgerSK_TVF (will be recreated with INT parameter)';
END
GO

-- ----------------------------------------------------------------------------
-- 3. Drop legacy IFDB-direct staging and map tables.
--    The new pipeline reads from fhir.Patient + ref.Patient_Borger instead.
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[stg].[Borger]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [stg].[Borger];
    PRINT 'Dropped table stg.Borger (legacy IFDB-direct path removed)';
END
GO

IF OBJECT_ID(N'[map].[Borger]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [map].[Borger];
    PRINT 'Dropped table map.Borger (BIGINT surrogate assignment replaced by ref.Patient_Borger INT IDENTITY)';
END
GO

-- ----------------------------------------------------------------------------
-- 4. Drop dim.Borger — recreated by tables/shared_dimensions/02_dims.sql
--    with INT keys and the full lineage-defined column set.
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [dim].[Borger];
    PRINT 'Dropped table dim.Borger (will be recreated with INT keys and new column set)';
END
GO

-- ----------------------------------------------------------------------------
-- 5. Drop fhir.Patient — column set changes (address columns move to
--    fhir.Patient_Address; new identifier/demographic columns added).
--    Drop is safe because SOF pipeline (sof_patient_*) refills on next run.
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [fhir].[Patient];
    PRINT 'Dropped table fhir.Patient (will be recreated by tables/fhir/01_tables.sql with new column set)';
END
GO

-- ----------------------------------------------------------------------------
-- Note: fhir.Patient_Address, ref.Patient_Borger, ref.Patient_Address_Borger
-- are NEW tables — they have no legacy state to drop. Their CREATE TABLE
-- blocks live in:
--   * tables/fhir/01_tables.sql              (fhir.Patient_Address)
--   * tables/ref/01_tables.sql               (ref.Patient_Borger + ref.Patient_Address_Borger)
-- They will be created on the next infrastructure deployment.
-- ----------------------------------------------------------------------------
