-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: enforce NOT NULL on the required FHIR patient references.
--
-- These columns are FK-Required in the lineage, and the posting path already
-- hard-fails on an unresolved required reference (reference_resolver raises for
-- is_required=True). The one remaining leak was a NULL *source* PATIENT_ID
-- sailing through as a NULL flat value; this constraint closes it at the schema
-- level, so the DQA not_null checks on these columns are retired (redundant).
--
-- Defensive: each table is pre-checked for existing NULLs and RAISERRORs with a
-- clear message instead of applying -- fix the data first (those rows are
-- invalid: a clinical resource without a patient).
--
-- SQL Server does not allow changing nullability while an index depends on the
-- column, so each block drops the dependent index(es), ALTERs, and recreates
-- them exactly as defined in tables/fhir/01_tables.sql (kept in sync).
--
-- NOTE: the ALTER scans the table under a schema lock and the index rebuilds run
-- offline on non-Enterprise editions. On large tables (fhir.Observation,
-- ~18M-row fhir.Encounter) run in a maintenance window.
-- Applied to both staging and DSO by staging_migrations / dso_migrations.
-- =============================================================================

-- fhir.Encounter ---------------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[Encounter]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[Encounter] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.Encounter has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_Encounter_PatientId] ON [fhir].[Encounter];
        ALTER TABLE [fhir].[Encounter] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_PatientId]
            ON [fhir].[Encounter] ([patient_id] ASC) ON [PRIMARY];
    END
END
GO

-- fhir.EpisodeOfCare -----------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[EpisodeOfCare]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[EpisodeOfCare] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.EpisodeOfCare has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_EpisodeOfCare_PatientId] ON [fhir].[EpisodeOfCare];
        ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_PatientId]
            ON [fhir].[EpisodeOfCare] ([patient_id] ASC) ON [PRIMARY];
    END
END
GO

-- fhir.Observation (also carries the composite SFI index on patient_id) --------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[Observation]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[Observation] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.Observation has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_Observation_PatientId] ON [fhir].[Observation];
        DROP INDEX IF EXISTS [IX_FHIR_Observation_Sfi] ON [fhir].[Observation];
        ALTER TABLE [fhir].[Observation] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_PatientId]
            ON [fhir].[Observation] ([patient_id] ASC) ON [PRIMARY];
        CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_Sfi]
            ON [fhir].[Observation] ([element_system] ASC, [element_code] ASC, [patient_id] ASC, [effective_date] ASC) ON [PRIMARY];
    END
END
GO

-- fhir.Condition ----------------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[Condition]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[Condition] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.Condition has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_Condition_PatientId] ON [fhir].[Condition];
        ALTER TABLE [fhir].[Condition] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_PatientId]
            ON [fhir].[Condition] ([patient_id] ASC) ON [PRIMARY];
    END
END
GO

-- fhir.Procedure -----------------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[Procedure]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[Procedure] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.Procedure has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_Procedure_PatientId] ON [fhir].[Procedure];
        ALTER TABLE [fhir].[Procedure] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_PatientId]
            ON [fhir].[Procedure] ([patient_id] ASC) ON [PRIMARY];
    END
END
GO

-- fhir.ServiceRequest --------------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[ServiceRequest]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[ServiceRequest] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.ServiceRequest has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_ServiceRequest_PatientId] ON [fhir].[ServiceRequest];
        ALTER TABLE [fhir].[ServiceRequest] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_PatientId]
            ON [fhir].[ServiceRequest] ([patient_id] ASC) ON [PRIMARY];
    END
END
GO

-- fhir.CarePlan ---------------------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[CarePlan]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[CarePlan] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.CarePlan has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_CarePlan_PatientId] ON [fhir].[CarePlan];
        ALTER TABLE [fhir].[CarePlan] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_CarePlan_PatientId]
            ON [fhir].[CarePlan] ([patient_id] ASC) ON [PRIMARY];
    END
END
GO

-- fhir.CarePlan_Activity ---------------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[fhir].[CarePlan_Activity]'), N'patient_id', 'AllowsNull') = 1
BEGIN
    IF EXISTS (SELECT 1 FROM [fhir].[CarePlan_Activity] WHERE [patient_id] IS NULL)
        RAISERROR(N'fhir.CarePlan_Activity has rows with NULL patient_id (required reference) -- fix them before applying NOT NULL.', 16, 1);
    ELSE
    BEGIN
        DROP INDEX IF EXISTS [IX_FHIR_CarePlan_Activity_PatientId] ON [fhir].[CarePlan_Activity];
        ALTER TABLE [fhir].[CarePlan_Activity] ALTER COLUMN [patient_id] NVARCHAR(255) NOT NULL;
        CREATE NONCLUSTERED INDEX [IX_FHIR_CarePlan_Activity_PatientId]
            ON [fhir].[CarePlan_Activity] ([patient_id] ASC) ON [PRIMARY];
    END
END
GO
