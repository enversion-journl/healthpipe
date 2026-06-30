-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- FHIR Flat Tables (SQL on FHIR ViewDefinition materialization)
--
-- These tables store flattened FHIR resource data extracted from the Aidbox
-- PostgreSQL "sof" schema views. Each table corresponds to a SQL on FHIR
-- ViewDefinition that projects FHIR resources into a tabular structure.
--
-- Columns:
--   - Resource-specific columns from the ViewDefinition
--   - last_updated: Resource timestamp from Aidbox (r.ts) for partition logic
--   - created_at: Resource creation timestamp from Aidbox (r.cts)
--   - DW_Timestamp_Insert / DW_Timestamp_Update: DW audit timestamps
-- ============================================================================

-- ============================================================================
-- Patient (from patient_view)
-- dk-core compliant projection: one row per patient_id. Address history
-- lives in fhir.Patient_Address (Type 2 sidecar).
-- Identifier-system URIs are NOT embedded in lineage YAMLs; they live in
-- utils/fhir/identifier_systems.py and are resolved by the transform layer.
-- See data_lineage/fhir/fhir_patient.yml for the source-of-truth column spec.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Patient]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Patient] (
    [patient_id] NVARCHAR(255) NOT NULL,
    -- Identifiers (dk-core slices; at least one of patient_ssn/x_ecpr/d_ecpr
    -- must be set per patient — enforced by ref.Patient_Borger, not here)
    [patient_ssn] NVARCHAR(10) NULL,
    [previous_ssn] NVARCHAR(10) NULL,
    [x_ecpr] NVARCHAR(50) NULL,
    [d_ecpr] NVARCHAR(50) NULL,
    [ifdb_patient_id] NVARCHAR(255) NULL,
    -- Status (dk-core: active, deceased)
    [active] BIT NULL,
    [deceased] BIT NULL,
    -- Name (dk-core slice 'official' — flattened)
    [name_use] NVARCHAR(20) NULL,
    [name_given] NVARCHAR(500) NULL,
    [name_family] NVARCHAR(255) NULL,
    [name_text] NVARCHAR(500) NULL,
    -- Demographics
    [gender] NVARCHAR(20) NULL,
    [birth_date] DATE NULL,
    -- Marital status (Type 1; periode-historik håndteres ikke her)
    [marital_status_code] NVARCHAR(8) NULL,
    [marital_status_display] NVARCHAR(100) NULL,
    -- General practitioner
    [gp_display] NVARCHAR(500) NULL,
    [gp_identifier] NVARCHAR(50) NULL,
    -- Citizenship (HL7 patient-citizenship extension)
    [citizenship_code] NVARCHAR(8) NULL,
    [citizenship_display] NVARCHAR(255) NULL,
    -- Family relations (region-internal extensions)
    [mother_ssn] NVARCHAR(10) NULL,
    [father_ssn] NVARCHAR(10) NULL,
    -- Privacy / context flags
    [address_protection] BIT NULL,
    [discretion_flag] BIT NULL,
    [medcom_test_person] BIT NULL,
    -- District (region-local extensions)
    [district_code] NVARCHAR(8) NULL,
    [district_text] NVARCHAR(100) NULL,
    [social_district_code] NVARCHAR(8) NULL,
    [social_district_text] NVARCHAR(100) NULL,
    -- Audit (meta + region-internal)
    [modtidspunkt] DATETIME2 NULL,
    [is_deleted] BIT NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_Patient] PRIMARY KEY CLUSTERED ([patient_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Patient_LastUpdated] ON [fhir].[Patient] ([last_updated] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Patient_SSN] ON [fhir].[Patient] ([patient_ssn] ASC) ON [PRIMARY];

END
GO

-- ============================================================================
-- Patient_Address (from patient_address_view)
-- Type 2 sidecar for FHIR Patient.address — one row per address period
-- per patient. Powers ref.Patient_Address_Borger which derives the
-- DW_SK_Borger surrogate via non-overlapping DW_ValidFrom/DW_ValidTo
-- intervals (LEAD over address_period_start).
--
-- PK: composite on (patient_id, address_period_start). Null sentinel
-- '1900-01-01' applied at MERGE time for IFDB-only patients without a
-- recorded start date.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Patient_Address] (
    [patient_id] NVARCHAR(255) NOT NULL,
    [address_period_start] NVARCHAR(50) NOT NULL,
    [address_period_end] NVARCHAR(50) NULL,
    [address_use] NVARCHAR(20) NULL,
    [address_official] BIT NULL,
    [address_line] NVARCHAR(500) NULL,
    [address_postal_code] NVARCHAR(10) NULL,
    [address_city] NVARCHAR(255) NULL,
    [address_country] NVARCHAR(8) NULL,
    [municipality_code] NVARCHAR(8) NULL,
    [region_code] NVARCHAR(8) NULL,
    [kvhx] NVARCHAR(64) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_Patient_Address] PRIMARY KEY CLUSTERED ([patient_id], [address_period_start])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Patient_Address_LastUpdated] ON [fhir].[Patient_Address] ([last_updated] ASC) ON [PRIMARY];

END

-- ============================================================================
-- Encounter (from encounter_view)
-- Note: one row per Encounter resource.
-- PK is encounter_id.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[Encounter](
        [encounter_id] NVARCHAR(255) NOT NULL,
        [patient_id] NVARCHAR(255) NULL,
        [status] NVARCHAR(50) NULL,
        [class_code] NVARCHAR(100) NULL,
        [class_display] NVARCHAR(255) NULL,
        [period_start] NVARCHAR(50) NULL,
        [period_end] NVARCHAR(50) NULL,
        [episode_of_care_id] NVARCHAR(255) NULL,
        [service_provider_id] NVARCHAR(255) NULL,
        [identifier_value] NVARCHAR(255) NULL,
        [is_deleted] INT NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_Encounter] PRIMARY KEY CLUSTERED ([encounter_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_PatientId]
        ON [fhir].[Encounter] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_LastUpdated]
        ON [fhir].[Encounter] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_EpisodeOfCareId]
        ON [fhir].[Encounter] ([episode_of_care_id] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Encounter Location (from encounter_location_view)
-- Note: forEachOrNull: location produces one row per location per encounter.
-- Encounters without locations get one row with empty location_id.
-- A patient can visit the same location multiple times within one encounter
-- (e.g. ward A → operating room → ward A), so location_period_start is
-- part of the PK to distinguish separate stays.
-- PK is composite: encounter_id + location_id + location_period_start
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(
            N'[fhir].[Encounter_Location]'
        )
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Encounter_Location] (
    [encounter_id] NVARCHAR(255) NOT NULL,
    [location_id] NVARCHAR(255) NOT NULL DEFAULT N'',
    [location_status] NVARCHAR(50) NULL,
    [location_period_start] NVARCHAR(50) NOT NULL DEFAULT N'',
    [location_period_end] NVARCHAR(50) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    PRIMARY KEY CLUSTERED (
        [encounter_id],
        [location_id],
        [location_period_start]
    )
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Encounter_Location_EncounterId] ON [fhir].[Encounter_Location] ([encounter_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Encounter_Location_LastUpdated] ON [fhir].[Encounter_Location] ([last_updated] ASC) ON [PRIMARY];

END

-- ============================================================================
-- Condition (from condition_view)
-- Note: forEach: code.coding produces one row per coding per condition.
-- PK is composite: condition_id + code + code_system
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[Condition]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[Condition](
        [condition_id] NVARCHAR(255) NOT NULL,
        [patient_id] NVARCHAR(255) NULL,
        [encounter_id] NVARCHAR(255) NULL,
        [clinical_status] NVARCHAR(50) NULL,
        [verification_status] NVARCHAR(50) NULL,
        [recorded_date] NVARCHAR(50) NULL,
        [code] NVARCHAR(255) NOT NULL DEFAULT N'',
        [code_system] NVARCHAR(500) NOT NULL DEFAULT N'',
        [code_display] NVARCHAR(500) NULL,
        [source_system] NVARCHAR(100) NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_Condition] PRIMARY KEY CLUSTERED ([condition_id], [code], [code_system])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_PatientId]
        ON [fhir].[Condition] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_EncounterId]
        ON [fhir].[Condition] ([encounter_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_LastUpdated]
        ON [fhir].[Condition] ([last_updated] ASC) ON [PRIMARY];

    -- Kept in sync with migrations/05_condition_add_source_identifier_value.sql
    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_SourceIdentifierValue]
        ON [fhir].[Condition] ([source_system] ASC, [source_identifier_value] ASC)
        ON [PRIMARY];
END
GO

-- ============================================================================
-- Observation (from observation_view)
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Observation]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Observation] (
    [observation_id] NVARCHAR(255) NOT NULL,
    [patient_id] NVARCHAR(255) NULL,
    [encounter_id] NVARCHAR(255) NULL,
    [organization_id] NVARCHAR(255) NULL,
    [practitioner_id] NVARCHAR(255) NULL,
    [practitioner_display] NVARCHAR(500) NULL,
    [status] NVARCHAR(50) NULL,
    [category_code] NVARCHAR(255) NULL,
    [code] NVARCHAR(255) NULL,
    [code_system] NVARCHAR(500) NULL,
    [code_display] NVARCHAR(500) NULL,
    [element_system] NVARCHAR(500) NULL,
    [element_code] NVARCHAR(255) NULL,
    [element_display] NVARCHAR(500) NULL,
    [effective_date] NVARCHAR(50) NULL,
    [value_type] NVARCHAR(50) NULL,
    [value_int] NVARCHAR(50) NULL,
    [value_dec] NVARCHAR(50) NULL,
    [value_string] NVARCHAR(MAX) NULL,
    [value_datetime] NVARCHAR(50) NULL,
    [value_bool] NVARCHAR(10) NULL,
    [value_quantity] NVARCHAR(100) NULL,
    [value_unit] NVARCHAR(100) NULL,
    [value_coding_system] NVARCHAR(500) NULL,
    [value_coding_code] NVARCHAR(255) NULL,
    [value_coding_display] NVARCHAR(500) NULL,
    [ifdb_aktivitet_id] NVARCHAR(255) NULL,
    [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL,
    [element_nummer] NVARCHAR(50) NULL,
    [klynge_id] NVARCHAR(50) NULL,
    [klynge_id_hierarki] NVARCHAR(500) NULL,
    [klynge_valg_ledetekst] NVARCHAR(MAX) NULL,
    [klynge_parent_id] NVARCHAR(50) NULL,
    [repetition_id] NVARCHAR(50) NULL,
    [element_kode] NVARCHAR(255) NULL,
    [element_original_kode] NVARCHAR(255) NULL,
    [element_administrativ_enhed_id] NVARCHAR(255) NULL,
    [repetition_id_hierarki] NVARCHAR(500) NULL,
    [opdateret_tid] NVARCHAR(50) NULL,
    [xml_status] INT NULL,
    [xml_status_text] NVARCHAR(255) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    -- Sargable change-detection column for SFI delta discovery
    -- (kept in sync with migrations/08_delta_discovery_effective_timestamp.sql)
    [DW_Timestamp_Effective] AS ISNULL([DW_Timestamp_Update], [DW_Timestamp_Insert]) PERSISTED,
    CONSTRAINT [PK_FHIR_Observation] PRIMARY KEY CLUSTERED ([observation_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_PatientId] ON [fhir].[Observation] ([patient_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_LastUpdated] ON [fhir].[Observation] ([last_updated] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_Sfi]
    ON [fhir].[Observation] ([element_system] ASC, [element_code] ASC, [patient_id] ASC, [effective_date] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_IfdbAktivitet] ON [fhir].[Observation] ([ifdb_aktivitet_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_SystemDWEffective]
    ON [fhir].[Observation] ([element_system] ASC, [DW_Timestamp_Effective] ASC)
    INCLUDE ([observation_id]) ON [PRIMARY];

END
GO

-- ----------------------------------------------------------------------------
-- Idempotent migration for SFI/typed-value columns on existing fhir.Observation.
-- Pre-SFI deployments only had [code], [value_string], [value_quantity], [value_unit].
-- Each ALTER only fires if the column is missing.
-- ----------------------------------------------------------------------------
IF EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND type = 'U'
)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_system')
        ALTER TABLE [fhir].[Observation] ADD [element_system] NVARCHAR(500) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_code')
        ALTER TABLE [fhir].[Observation] ADD [element_code] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_display')
        ALTER TABLE [fhir].[Observation] ADD [element_display] NVARCHAR(500) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_type')
        ALTER TABLE [fhir].[Observation] ADD [value_type] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_int')
        ALTER TABLE [fhir].[Observation] ADD [value_int] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_dec')
        ALTER TABLE [fhir].[Observation] ADD [value_dec] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_datetime')
        ALTER TABLE [fhir].[Observation] ADD [value_datetime] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_bool')
        ALTER TABLE [fhir].[Observation] ADD [value_bool] NVARCHAR(10) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_coding_system')
        ALTER TABLE [fhir].[Observation] ADD [value_coding_system] NVARCHAR(500) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_coding_code')
        ALTER TABLE [fhir].[Observation] ADD [value_coding_code] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'value_coding_display')
        ALTER TABLE [fhir].[Observation] ADD [value_coding_display] NVARCHAR(500) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'ifdb_aktivitet_id')
        ALTER TABLE [fhir].[Observation] ADD [ifdb_aktivitet_id] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'ifdb_aktivitet_sfi_id')
        ALTER TABLE [fhir].[Observation] ADD [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_nummer')
        ALTER TABLE [fhir].[Observation] ADD [element_nummer] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'klynge_id')
        ALTER TABLE [fhir].[Observation] ADD [klynge_id] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'klynge_id_hierarki')
        ALTER TABLE [fhir].[Observation] ADD [klynge_id_hierarki] NVARCHAR(500) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'klynge_valg_ledetekst')
        ALTER TABLE [fhir].[Observation] ADD [klynge_valg_ledetekst] NVARCHAR(MAX) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'klynge_parent_id')
        ALTER TABLE [fhir].[Observation] ADD [klynge_parent_id] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'repetition_id')
        ALTER TABLE [fhir].[Observation] ADD [repetition_id] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_kode')
        ALTER TABLE [fhir].[Observation] ADD [element_kode] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_original_kode')
        ALTER TABLE [fhir].[Observation] ADD [element_original_kode] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_administrativ_enhed_id')
        ALTER TABLE [fhir].[Observation] ADD [element_administrativ_enhed_id] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'repetition_id_hierarki')
        ALTER TABLE [fhir].[Observation] ADD [repetition_id_hierarki] NVARCHAR(500) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'opdateret_tid')
        ALTER TABLE [fhir].[Observation] ADD [opdateret_tid] NVARCHAR(50) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'xml_status')
        ALTER TABLE [fhir].[Observation] ADD [xml_status] INT NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'xml_status_text')
        ALTER TABLE [fhir].[Observation] ADD [xml_status_text] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'organization_id')
        ALTER TABLE [fhir].[Observation] ADD [organization_id] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'practitioner_id')
        ALTER TABLE [fhir].[Observation] ADD [practitioner_id] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'practitioner_display')
        ALTER TABLE [fhir].[Observation] ADD [practitioner_display] NVARCHAR(500) NULL;

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_FHIR_Observation_Sfi'
    )
    AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'element_system')
        CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_Sfi]
            ON [fhir].[Observation] ([element_system] ASC, [element_code] ASC, [patient_id] ASC, [effective_date] ASC) ON [PRIMARY];

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_FHIR_Observation_IfdbAktivitet'
    )
    AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'ifdb_aktivitet_id')
        CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_IfdbAktivitet]
            ON [fhir].[Observation] ([ifdb_aktivitet_id] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- EpisodeOfCare (from episode_of_care_view)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[EpisodeOfCare](
        [episode_of_care_id] NVARCHAR(255) NOT NULL,
        [patient_id] NVARCHAR(255) NULL,
        [status] NVARCHAR(50) NULL,
        [type_code] NVARCHAR(255) NULL,
        [type_display] NVARCHAR(500) NULL,
        [period_start] NVARCHAR(50) NULL,
        [period_end] NVARCHAR(50) NULL,
        [managing_organization_id] NVARCHAR(255) NULL,
        [identifier_value] NVARCHAR(255) NULL,
        [previous_episode_of_care_id] NVARCHAR(255) NULL,
        [previous_eoc_ref_type_code] NVARCHAR(255) NULL,
        [healthissue_id] NVARCHAR(255) NULL,
        [discharge_disposition_code] NVARCHAR(255) NULL,
        [is_deleted] INT NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_EpisodeOfCare] PRIMARY KEY CLUSTERED ([episode_of_care_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_PatientId]
        ON [fhir].[EpisodeOfCare] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_LastUpdated]
        ON [fhir].[EpisodeOfCare] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_HealthissueId]
        ON [fhir].[EpisodeOfCare] ([healthissue_id] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Location (from location_view)
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Location]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Location] (
    [location_id] NVARCHAR(255) NOT NULL,
    [name] NVARCHAR(500) NULL,
    [status] NVARCHAR(50) NULL,
    [description] NVARCHAR(MAX) NULL,
    [type_code] NVARCHAR(255) NULL,
    [type_display] NVARCHAR(500) NULL,
    [managing_organization_id] NVARCHAR(255) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_Location] PRIMARY KEY CLUSTERED ([location_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Location_LastUpdated] ON [fhir].[Location] ([last_updated] ASC) ON [PRIMARY];

END

-- ============================================================================
-- Organization (from organization_view)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[Organization]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[Organization](
        [organization_id] NVARCHAR(255) NOT NULL,
        [name] NVARCHAR(500) NULL,
        [active] NVARCHAR(10) NULL,
        [type_code] NVARCHAR(255) NULL,
        [type_display] NVARCHAR(500) NULL,
        [part_of_id] NVARCHAR(255) NULL,
        [identifier_value_sor] NVARCHAR(255) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_Organization] PRIMARY KEY CLUSTERED ([organization_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Organization_LastUpdated]
        ON [fhir].[Organization] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- CarePlan (from careplan_view)
-- One row per CarePlan. Filter: category = alle_markoerer.
-- PK is careplan_id.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[CarePlan]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[CarePlan] (
    [careplan_id] NVARCHAR(255) NOT NULL,
    [patient_id] NVARCHAR(255) NULL,
    [status] NVARCHAR(50) NULL,
    [intent] NVARCHAR(50) NULL,
    [category_code] NVARCHAR(100) NULL,
    [period_start] NVARCHAR(50) NULL,
    [period_end] NVARCHAR(50) NULL,
    [organization_id] NVARCHAR(255) NULL,
    [identifier_value] NVARCHAR(500) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_CarePlan] PRIMARY KEY CLUSTERED ([careplan_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_CarePlan_PatientId] ON [fhir].[CarePlan] ([patient_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_CarePlan_LastUpdated] ON [fhir].[CarePlan] ([last_updated] ASC) ON [PRIMARY];

END

-- ============================================================================
-- CarePlan_Activity (from careplan_activity_view)
-- forEach: activity produces one row per activity per careplan.
-- PK is ifdb_eventmarker_id (unique source identifier per activity).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[CarePlan_Activity](
        [careplan_id] NVARCHAR(255) NOT NULL,
        [patient_id] NVARCHAR(255) NULL,
        [category_code] NVARCHAR(100) NULL,
        [ifdb_eventmarker_id] NVARCHAR(255) NOT NULL,
        [episode_of_care_id] NVARCHAR(255) NULL,
        [markoerkode] NVARCHAR(255) NULL,
        [markoer_display] NVARCHAR(500) NULL,
        [organization_id] NVARCHAR(255) NULL,
        [practitioner_id] NVARCHAR(255) NULL,
        [activity_timestamp] NVARCHAR(50) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_CarePlan_Activity] PRIMARY KEY CLUSTERED ([ifdb_eventmarker_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_CarePlan_Activity_CarePlanId]
        ON [fhir].[CarePlan_Activity] ([careplan_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_CarePlan_Activity_PatientId]
        ON [fhir].[CarePlan_Activity] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_CarePlan_Activity_LastUpdated]
        ON [fhir].[CarePlan_Activity] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- ServiceRequest (from IFDB HENVISNING and RIS_YDELSE)
-- One row per ServiceRequest. PK is service_request_id (FHIR resource ID).
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[ServiceRequest] (
    [service_request_id] NVARCHAR(255) NOT NULL,
    [identifier_system] NVARCHAR(255) NULL,
    [identifier_value] NVARCHAR(255) NULL,
    [patient_id] NVARCHAR(255) NULL,
    [episode_of_care_id] NVARCHAR(255) NULL,
    [encounter_id] NVARCHAR(255) NULL,
    [status] NVARCHAR(50) NULL,
    [intent] NVARCHAR(50) NULL,
    [priority] NVARCHAR(50) NULL,
    [category_code] NVARCHAR(255) NULL,
    [category_display] NVARCHAR(500) NULL,
    [code] NVARCHAR(255) NULL,
    [code_system] NVARCHAR(500) NULL,
    [code_display] NVARCHAR(500) NULL,
    [authored_on] NVARCHAR(50) NULL,
    [occurrence_start] NVARCHAR(50) NULL,
    [occurrence_end] NVARCHAR(50) NULL,
    [requester_id] NVARCHAR(255) NULL,
    [performer_id] NVARCHAR(255) NULL,
    [source_table] NVARCHAR(100) NULL,
    [is_deleted] INT NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_ServiceRequest] PRIMARY KEY CLUSTERED ([service_request_id])
) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_PatientId]
        ON [fhir].[ServiceRequest] ([patient_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_EpisodeOfCareId]
        ON [fhir].[ServiceRequest] ([episode_of_care_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_EncounterId]
        ON [fhir].[ServiceRequest] ([encounter_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_Identifier]
        ON [fhir].[ServiceRequest] ([identifier_system] ASC, [identifier_value] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_LastUpdated]
        ON [fhir].[ServiceRequest] ([last_updated] ASC) ON [PRIMARY];

END
GO

-- ============================================================================
-- Procedure (from IFDB ADMINISTRATIVE_PROCEDURE and RIS_YDELSE)
-- One row per procedure. PK is procedure_id (FHIR resource ID).
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Procedure]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Procedure] (
    [procedure_id] NVARCHAR(255) NOT NULL,
    [patient_id] NVARCHAR(255) NULL,
    [encounter_id] NVARCHAR(255) NULL,
    [episode_of_care_id] NVARCHAR(255) NULL,
    [status] NVARCHAR(50) NULL,
    [category_code] NVARCHAR(255) NULL,
    [category_display] NVARCHAR(500) NULL,
    [code] NVARCHAR(255) NULL,
    [code_system] NVARCHAR(500) NULL,
    [code_display] NVARCHAR(500) NULL,
    [performed_datetime] NVARCHAR(50) NULL,
    [performed_period_start] NVARCHAR(50) NULL,
    [performed_period_end] NVARCHAR(50) NULL,
    [body_site_code] NVARCHAR(255) NULL,
    [body_site_display] NVARCHAR(500) NULL,
    [source_system] NVARCHAR(100) NULL,
    [ifdb_aktivitet_id] NVARCHAR(255) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_Procedure] PRIMARY KEY CLUSTERED ([procedure_id])
) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_PatientId]
        ON [fhir].[Procedure] ([patient_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_EncounterId]
        ON [fhir].[Procedure] ([encounter_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_Code]
        ON [fhir].[Procedure] ([code] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_LastUpdated]
        ON [fhir].[Procedure] ([last_updated] ASC) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_IfdbAktivitet]
        ON [fhir].[Procedure] ([ifdb_aktivitet_id] ASC) ON [PRIMARY];

END

-- Migrate fhir.Procedure: add lossless columns if table already exists without them.
-- Idempotent: each ALTER only runs if the column is missing.
IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Procedure]')
        AND
    type = 'U'
) BEGIN IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Procedure]')
        AND name = N'ifdb_aktivitet_id'
)
ALTER TABLE [fhir].[Procedure]
ADD [ifdb_aktivitet_id] NVARCHAR(255) NULL;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Procedure]')
        AND name = N'IX_FHIR_Procedure_IfdbAktivitet'
)
CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_IfdbAktivitet]
    ON [fhir].[Procedure] ([ifdb_aktivitet_id] ASC) ON [PRIMARY];

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Procedure]')
        AND name = N'performed_period_start'
)
ALTER TABLE [fhir].[Procedure]
ADD [performed_period_start] NVARCHAR(50) NULL;

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Procedure]')
        AND name = N'performed_period_end'
)
ALTER TABLE [fhir].[Procedure]
ADD [performed_period_end] NVARCHAR(50) NULL;

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Procedure]')
        AND name = N'body_site_code'
)
ALTER TABLE [fhir].[Procedure]
ADD [body_site_code] NVARCHAR(255) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]') AND name = N'body_site_display')
        ALTER TABLE [fhir].[Procedure] ADD [body_site_display] NVARCHAR(500) NULL;
END
GO

-- ============================================================================
-- Questionnaire (from questionnaire_view)
-- One row per Questionnaire resource. PK is questionnaire_id (FHIR resource ID).
-- Versioning lives in (url, version); routing/QR-resolution uses canonical|version.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Questionnaire] (
    [questionnaire_id] NVARCHAR(255) NOT NULL,
    [url] NVARCHAR(500) NULL,
    [version] NVARCHAR(100) NULL,
    [status] NVARCHAR(50) NULL,
    [name] NVARCHAR(500) NULL,
    [title] NVARCHAR(500) NULL,
    [date] NVARCHAR(50) NULL,
    [publisher] NVARCHAR(500) NULL,
    [source_model_name] NVARCHAR(255) NULL,
    [source_model_version] NVARCHAR(100) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_Questionnaire] PRIMARY KEY CLUSTERED ([questionnaire_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Questionnaire_UrlVersion] ON [fhir].[Questionnaire] ([url] ASC, [version] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Questionnaire_LastUpdated] ON [fhir].[Questionnaire] ([last_updated] ASC) ON [PRIMARY];

END
GO

-- ============================================================================
-- Questionnaire_item (from questionnaire_item_view)
-- forEach over item: one row per flat generated item.
-- SFI lives on item.code (system='urn:dk:ifdb:sfi'); projected as element_*.
-- PK is composite: questionnaire_id + link_id + item_key. link_id is the SFI
-- concept; item_key is native Questionnaire.item.id and preserves repeated
-- occurrences without deduplication.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[Questionnaire_item] (
    [questionnaire_id] NVARCHAR(255) NOT NULL,
    [questionnaire_url] NVARCHAR(500) NULL,
    [questionnaire_version] NVARCHAR(100) NULL,
    [link_id] NVARCHAR(255) NOT NULL DEFAULT N'',
    [item_key] NVARCHAR(100) NOT NULL DEFAULT N'',
    [text] NVARCHAR(MAX) NULL,
    [item_type] NVARCHAR(50) NULL,
    [required] NVARCHAR(10) NULL,
    [repeats] NVARCHAR(10) NULL,
    [element_system] NVARCHAR(500) NULL,
    [element_code] NVARCHAR(255) NULL,
    [element_display] NVARCHAR(500) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_FHIR_Questionnaire_item]
        PRIMARY KEY CLUSTERED ([questionnaire_id], [link_id], [item_key])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QuestionnaireItem_Sfi]
    ON [fhir].[Questionnaire_item] ([element_system] ASC, [element_code] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QuestionnaireItem_UrlVersionLinkId]
    ON [fhir].[Questionnaire_item] ([questionnaire_url] ASC, [questionnaire_version] ASC, [link_id] ASC, [item_key] ASC) ON [PRIMARY];

END
GO

-- ============================================================================
-- QuestionnaireResponse (from questionnaire_response_view)
-- One row per QR resource. PK is qr_id (FHIR resource ID).
-- questionnaire is the canonical|version reference back to fhir.Questionnaire.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[QuestionnaireResponse] (
    [qr_id] NVARCHAR(255) NOT NULL,
    [questionnaire] NVARCHAR(600) NULL,
    [questionnaire_canonical] NVARCHAR(500) NULL,
    [questionnaire_version] NVARCHAR(100) NULL,
    [status] NVARCHAR(50) NULL,
    [patient_id] NVARCHAR(255) NULL,
    [encounter_id] NVARCHAR(255) NULL,
    [organization_id] NVARCHAR(255) NULL,
    [practitioner_id] NVARCHAR(255) NULL,
    [practitioner_display] NVARCHAR(500) NULL,
    [authored] NVARCHAR(50) NULL,
    [source_aktivitet_id] NVARCHAR(255) NULL,
    [source_model_name] NVARCHAR(255) NULL,
    [source_model_version] NVARCHAR(100) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    -- Sargable change-detection column for SFI delta discovery
    -- (kept in sync with migrations/08_delta_discovery_effective_timestamp.sql)
    [DW_Timestamp_Effective] AS ISNULL([DW_Timestamp_Update], [DW_Timestamp_Insert]) PERSISTED,
    CONSTRAINT [PK_FHIR_QuestionnaireResponse] PRIMARY KEY CLUSTERED ([qr_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_PatientAuthored]
    ON [fhir].[QuestionnaireResponse] ([patient_id] ASC, [authored] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_SourceAktivitet]
    ON [fhir].[QuestionnaireResponse] ([source_aktivitet_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_LastUpdated]
    ON [fhir].[QuestionnaireResponse] ([last_updated] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_CanonicalVersion]
    ON [fhir].[QuestionnaireResponse] (
        [questionnaire_canonical] ASC,
        [questionnaire_version] ASC
    )
    INCLUDE ([patient_id], [authored], [source_model_name]) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_DWEffective]
    ON [fhir].[QuestionnaireResponse] ([DW_Timestamp_Effective] ASC)
    INCLUDE ([qr_id]) ON [PRIMARY];

END
GO

-- ----------------------------------------------------------------------------
-- Migrate fhir.QuestionnaireResponse for clinician/organization keys.
-- ----------------------------------------------------------------------------
IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
        AND
    type = 'U'
) BEGIN
    IF COL_LENGTH('fhir.QuestionnaireResponse', 'questionnaire_canonical') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse]
            ADD [questionnaire_canonical] NVARCHAR(500) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse', 'questionnaire_version') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse]
            ADD [questionnaire_version] NVARCHAR(100) NULL;

    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'organization_id'
    )
    ALTER TABLE [fhir].[QuestionnaireResponse]
        ADD [organization_id] NVARCHAR(255) NULL;

    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'practitioner_id'
    )
    ALTER TABLE [fhir].[QuestionnaireResponse]
        ADD [practitioner_id] NVARCHAR(255) NULL;

    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'practitioner_display'
    )
    ALTER TABLE [fhir].[QuestionnaireResponse]
        ADD [practitioner_display] NVARCHAR(500) NULL;

    UPDATE qr
    SET
        questionnaire_canonical =
            CASE
                WHEN qr.questionnaire IS NULL THEN NULL
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN LEFT(qr.questionnaire, CHARINDEX(N'|', qr.questionnaire) - 1)
                ELSE qr.questionnaire
            END,
        questionnaire_version =
            CASE
                WHEN qr.questionnaire IS NULL THEN NULL
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN SUBSTRING(
                        qr.questionnaire,
                        CHARINDEX(N'|', qr.questionnaire) + 1,
                        LEN(qr.questionnaire)
                    )
                ELSE NULL
            END
    FROM [fhir].[QuestionnaireResponse] AS qr
    WHERE
        qr.questionnaire IS NOT NULL
        AND (
            qr.questionnaire_canonical IS NULL
            OR qr.questionnaire_canonical <> CASE
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN LEFT(qr.questionnaire, CHARINDEX(N'|', qr.questionnaire) - 1)
                ELSE qr.questionnaire
            END
            OR ISNULL(qr.questionnaire_version, N'') <> ISNULL(CASE
                WHEN CHARINDEX(N'|', qr.questionnaire) > 0
                    THEN SUBSTRING(
                        qr.questionnaire,
                        CHARINDEX(N'|', qr.questionnaire) + 1,
                        LEN(qr.questionnaire)
                    )
                ELSE NULL
            END, N'')
        );

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
          AND name = N'IX_FHIR_QR_CanonicalVersion'
    )
    AND EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
          AND name = N'questionnaire_canonical'
    )
        CREATE NONCLUSTERED INDEX [IX_FHIR_QR_CanonicalVersion]
            ON [fhir].[QuestionnaireResponse] (
                [questionnaire_canonical] ASC,
                [questionnaire_version] ASC
            )
            INCLUDE ([patient_id], [authored], [source_model_name]) ON [PRIMARY];
END
GO

-- ============================================================================
-- QuestionnaireResponse_item (from questionnaire_response_item_view)
-- forEach over item with nested forEachOrNull on answer.
-- One row per (QR, item, answer); QRs without answers get a single all-NULL row.
-- element_system/element_code are NOT projected here -- they are derived from
-- fhir.Questionnaire_item via the (questionnaire_canonical, link_id, item_key)
-- join.
-- PK is composite: qr_id + link_id + item_key + answer_index.
-- item_key comes from native QuestionnaireResponse.item.id and keeps repeated
-- QR items with identical linkId distinct without custom extensions.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
        AND
    type = 'U'
) BEGIN
CREATE TABLE [fhir].[QuestionnaireResponse_item] (
    [qr_id] NVARCHAR(255) NOT NULL,
    [questionnaire] NVARCHAR(600) NULL,
    [link_id] NVARCHAR(255) NOT NULL DEFAULT N'',
    [item_key] NVARCHAR(100) NOT NULL DEFAULT N'',
    [text] NVARCHAR(MAX) NULL,
    [answer_index] NVARCHAR(10) NOT NULL DEFAULT N'',
    [value_type] NVARCHAR(50) NULL,
    [value_int] NVARCHAR(50) NULL,
    [value_dec] NVARCHAR(50) NULL,
    [value_string] NVARCHAR(MAX) NULL,
    [value_datetime] NVARCHAR(50) NULL,
    [value_bool] NVARCHAR(10) NULL,
    [value_quantity] NVARCHAR(100) NULL,
    [value_unit] NVARCHAR(100) NULL,
    [value_coding_system] NVARCHAR(500) NULL,
    [value_coding_code] NVARCHAR(255) NULL,
    [value_coding_display] NVARCHAR(500) NULL,
    [element_nummer] NVARCHAR(50) NULL,
    [klynge_id] NVARCHAR(50) NULL,
    [klynge_id_hierarki] NVARCHAR(500) NULL,
    [klynge_valg_ledetekst] NVARCHAR(MAX) NULL,
    [klynge_parent_id] NVARCHAR(50) NULL,
    [repetition_id] NVARCHAR(50) NULL,
    [element_kode] NVARCHAR(255) NULL,
    [element_original_kode] NVARCHAR(255) NULL,
    [element_administrativ_enhed_id] NVARCHAR(255) NULL,
    [repetition_id_hierarki] NVARCHAR(500) NULL,
    [opdateret_tid] NVARCHAR(50) NULL,
    [xml_status] INT NULL,
    [xml_status_text] NVARCHAR(255) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    -- Sargable change-detection column for SFI delta discovery
    -- (kept in sync with migrations/08_delta_discovery_effective_timestamp.sql)
    [DW_Timestamp_Effective] AS ISNULL([DW_Timestamp_Update], [DW_Timestamp_Insert]) PERSISTED,
    CONSTRAINT [PK_FHIR_QuestionnaireResponse_item]
        PRIMARY KEY CLUSTERED ([qr_id], [link_id], [item_key], [answer_index])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QRItem_QrId]
    ON [fhir].[QuestionnaireResponse_item] ([qr_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QRItem_LinkId]
    ON [fhir].[QuestionnaireResponse_item] ([link_id] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QRItem_LastUpdated]
    ON [fhir].[QuestionnaireResponse_item] ([last_updated] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QRI_DWEffective]
    ON [fhir].[QuestionnaireResponse_item] ([DW_Timestamp_Effective] ASC)
    INCLUDE ([qr_id]) ON [PRIMARY];

END
GO

IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
        AND type = 'U'
)
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'item_key') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD [item_key] NVARCHAR(100) NOT NULL CONSTRAINT [DF_FHIR_QRItem_ItemKey] DEFAULT N'';
END
GO

IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
        AND type = 'U'
)
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'answer_index') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD [answer_index] NVARCHAR(10) NOT NULL CONSTRAINT [DF_FHIR_QRItem_AnswerIndex] DEFAULT N'';
END
GO

IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
        AND type = 'U'
)
BEGIN
    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'element_nummer') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [element_nummer] NVARCHAR(50) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'klynge_id') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [klynge_id] NVARCHAR(50) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'klynge_id_hierarki') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [klynge_id_hierarki] NVARCHAR(500) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'klynge_valg_ledetekst') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [klynge_valg_ledetekst] NVARCHAR(MAX) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'klynge_parent_id') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [klynge_parent_id] NVARCHAR(50) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'repetition_id') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [repetition_id] NVARCHAR(50) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'element_kode') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [element_kode] NVARCHAR(255) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'element_original_kode') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [element_original_kode] NVARCHAR(255) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'element_administrativ_enhed_id') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [element_administrativ_enhed_id] NVARCHAR(255) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'repetition_id_hierarki') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [repetition_id_hierarki] NVARCHAR(500) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'opdateret_tid') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [opdateret_tid] NVARCHAR(50) NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'xml_status') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [xml_status] INT NULL;

    IF COL_LENGTH('fhir.QuestionnaireResponse_item', 'xml_status_text') IS NULL
        ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [xml_status_text] NVARCHAR(255) NULL;
END
GO

IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
        AND type = 'U'
)
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'item_key') IS NOT NULL
AND COL_LENGTH('fhir.QuestionnaireResponse_item', 'answer_index') IS NOT NULL
AND EXISTS (
    SELECT 1
    FROM sys.key_constraints kc
    WHERE
        kc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
        AND kc.[name] = N'PK_FHIR_QuestionnaireResponse_item'
)
AND (
    NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints kc
        INNER JOIN sys.index_columns ic
            ON ic.object_id = kc.parent_object_id
            AND ic.index_id = kc.unique_index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
            AND c.column_id = ic.column_id
        WHERE
            kc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
            AND kc.[name] = N'PK_FHIR_QuestionnaireResponse_item'
            AND c.[name] = N'item_key'
    )
    OR NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints kc
        INNER JOIN sys.index_columns ic
            ON ic.object_id = kc.parent_object_id
            AND ic.index_id = kc.unique_index_id
        INNER JOIN sys.columns c
            ON c.object_id = ic.object_id
            AND c.column_id = ic.column_id
        WHERE
            kc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
            AND kc.[name] = N'PK_FHIR_QuestionnaireResponse_item'
            AND c.[name] = N'answer_index'
    )
)
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        DROP CONSTRAINT [PK_FHIR_QuestionnaireResponse_item];

    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD CONSTRAINT [PK_FHIR_QuestionnaireResponse_item]
        PRIMARY KEY CLUSTERED ([qr_id], [link_id], [item_key], [answer_index]);
END
GO

-- ============================================================================
-- Appointment (from appointment_view)
-- Note: one row per Appointment resource.
-- Source: BOM_IFDB.AFTALE + AFTALEBOOKINGHISTORIK + AFTALERESSOURCEDELTAGER + joined lookups
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[Appointment](
        [appointment_id] NVARCHAR(255) NOT NULL,
        [identifier_value] NVARCHAR(255) NULL,
        [status] NVARCHAR(50) NULL,
        [appointment_type_code] NVARCHAR(100) NULL,
        [appointment_type_display] NVARCHAR(255) NULL,
        [appointment_type_text] NVARCHAR(255) NULL,
        [service_type_code] NVARCHAR(100) NULL,
        [cancelation_reason_code] NVARCHAR(100) NULL,
        [cancelation_reason_display] NVARCHAR(500) NULL,
        [cancelation_reason_text] NVARCHAR(500) NULL,
        [description] NVARCHAR(MAX) NULL,
        [patient_instruction] NVARCHAR(MAX) NULL,
        [comment] NVARCHAR(MAX) NULL,
        [start] NVARCHAR(50) NULL,
        [end] NVARCHAR(50) NULL,
        [minutes_duration] INT NULL,
        [created] NVARCHAR(50) NULL,
        [patient_id] NVARCHAR(255) NULL,
        [performing_location_id] NVARCHAR(255) NULL,
        [is_deleted] INT NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [DW_Timestamp_Insert] DATETIME2 NULL,
        [DW_Timestamp_Update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_Appointment] PRIMARY KEY CLUSTERED ([appointment_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Appointment_PatientId]
        ON [fhir].[Appointment] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Appointment_PerformingLocationId]
        ON [fhir].[Appointment] ([performing_location_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Appointment_LastUpdated]
        ON [fhir].[Appointment] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Appointment_Start]
        ON [fhir].[Appointment] ([start] ASC) ON [PRIMARY];
END
GO

-- Migration: widen free-text columns for existing installations
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]') AND type = 'U')
BEGIN
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]') AND name = N'description' AND max_length != -1)
        ALTER TABLE [fhir].[Appointment] ALTER COLUMN [description] NVARCHAR(MAX) NULL;
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]') AND name = N'patient_instruction' AND max_length != -1)
        ALTER TABLE [fhir].[Appointment] ALTER COLUMN [patient_instruction] NVARCHAR(MAX) NULL;
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]') AND name = N'comment' AND max_length != -1)
        ALTER TABLE [fhir].[Appointment] ALTER COLUMN [comment] NVARCHAR(MAX) NULL;
END
GO
