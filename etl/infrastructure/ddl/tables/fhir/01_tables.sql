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
--   - dw_timestamp_insert / dw_timestamp_update: DW audit timestamps
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
    -- must be set per patient — enforced by ref.PatientToBorger, not here)
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
    -- Marital status (Type 1; period history is not modelled here)
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
    [address_protection] BIT NOT NULL CONSTRAINT [DF_Patient_address_protection] DEFAULT 0,
    -- Municipality of the current address; kept when name and address are masked.
    [municipality_code] NVARCHAR(8) NULL,
    [discretion_flag] BIT NULL,
    [medcom_test_person] BIT NULL,
    -- District (region-local extensions)
    [district_code] NVARCHAR(20) NULL,
    [district_text] NVARCHAR(100) NULL,
    [social_district_code] NVARCHAR(32) NULL,
    [social_district_text] NVARCHAR(100) NULL,
    -- Audit (meta + region-internal)
    [modtidspunkt] DATETIME2 NULL,
    -- Projected from the resource's urn:fhir:isDeleted extension; a missing
    -- extension coalesces to 0 at load (boolean_false_default_columns in
    -- sof_utils.py). DEFAULT 0 covers rows created outside the SoF MERGE --
    -- those landed NULL and vanished behind a plain `WHERE dw_is_deleted = 0`
    -- (the 898 HAIBA stub patients).
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Patient_dw_is_deleted] DEFAULT 0,
    -- Provenance: meta_source is the builder's URI; each source_* flag mirrors
    -- one meta.tag, i.e. whether that source contributed to the citizen.
    [meta_source] NVARCHAR(255) NULL,
    [source_cpr] BIT NULL,
    [source_ifdb] BIT NULL,
    [source_haiba] BIT NULL,
    [source_sdsdrg] BIT NULL,
    [source_sdsudred] BIT NULL,
    [cpr_updated] DATE NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Patient_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Patient_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_FHIR_Patient] PRIMARY KEY CLUSTERED ([patient_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Patient_LastUpdated] ON [fhir].[Patient] ([last_updated] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Patient_SSN] ON [fhir].[Patient] ([patient_ssn] ASC) ON [PRIMARY];

-- DQA comparison index (kept in sync with ddl/migrations/20260625_fhir_dqa_comparison_indexes.sql)
CREATE NONCLUSTERED
INDEX [IX_FHIR_Patient_IfdbPatientId] ON [fhir].[Patient] ([ifdb_patient_id] ASC) ON [PRIMARY];

END
GO

-- ============================================================================
-- Patient_Address (from patient_address_view)
-- Type 2 sidecar for FHIR Patient.address — one row per address period
-- per patient. Powers ref.Patient_Address_To_Borger which derives the
-- dw_sk_Borger surrogate via non-overlapping dw_valid_from/dw_valid_to
-- intervals (LEAD over address_period_start).
--
-- PK: (patient_id, address_period_start). Period columns are DATETIME2(0),
-- normalised at load by normalize_fhir_datetime(). Absent start = 1900-01-01.
-- dw_is_deleted is set by child-grain reconcile; MERGE clears it on a match
-- because a soft-deleted row still occupies the PK.
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
    [address_period_start] DATETIME2(0) NOT NULL CONSTRAINT [DF_Patient_Address_address_period_start] DEFAULT '1900-01-01',
    [address_period_end] DATETIME2(0) NULL,
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
    -- Warehouse-managed: patient_address_view does not project the field --
    -- child-grain reconcile and dqa_fhir_server_cleanup set it. DEFAULT is
    -- required: the SoF MERGE does not list this column in its INSERT, so
    -- without it every insert would fail against NOT NULL. MERGE clears the
    -- flag on match (resurrect_soft_deleted) so a restored address is not
    -- left marked deleted.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Patient_Address_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Patient_Address_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Patient_Address_dw_timestamp_update] DEFAULT SYSDATETIME(),
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
        [patient_id] NVARCHAR(255) NOT NULL,
        [status] NVARCHAR(50) NULL,
        [class_code] NVARCHAR(100) NULL,
        [class_display] NVARCHAR(255) NULL,
        [period_start] NVARCHAR(50) NULL,
        [period_end] NVARCHAR(50) NULL,
        [episode_of_care_id] NVARCHAR(255) NULL,
        [service_provider_id] NVARCHAR(255) NULL,
        [identifier_value] NVARCHAR(255) NULL,
        -- Projected from the resource's urn:fhir:isDeleted extension.
        -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
        -- (boolean_false_default_columns in sof_utils.py).
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Encounter_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Encounter] PRIMARY KEY CLUSTERED ([encounter_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_PatientId]
        ON [fhir].[Encounter] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_LastUpdated]
        ON [fhir].[Encounter] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_EpisodeOfCareId]
        ON [fhir].[Encounter] ([episode_of_care_id] ASC) ON [PRIMARY];

    -- DQA comparison indexes (kept in sync with ddl/migrations/20260625_fhir_dqa_comparison_indexes.sql)
    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_IdentifierValue]
        ON [fhir].[Encounter] ([identifier_value] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_ServiceProviderId]
        ON [fhir].[Encounter] ([service_provider_id] ASC) ON [PRIMARY];
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
    [location_id] NVARCHAR(255) NOT NULL CONSTRAINT [DF_Encounter_Location_location_id] DEFAULT N'',
    [location_status] NVARCHAR(50) NULL,
    [location_period_start] NVARCHAR(50) NOT NULL CONSTRAINT [DF_Encounter_Location_location_period_start] DEFAULT N'',
    [location_period_end] NVARCHAR(50) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    -- Added by ddl/migrations/20260629_fhir_encounter_location_add_is_deleted.sql.
    -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the parent
    -- resource carries urn:fhir:isDeleted, but this view grain does not
    -- project the field -- it is set only by dqa_fhir_server_cleanup when the
    -- parent is hard-deleted in Aidbox, and cleared on MERGE match
    -- (resurrect_soft_deleted).
    -- NOTE the limit: `= 0` means "not marked by cleanup" -- NOT "still
    -- exists under the parent". A row that disappears from a still-live
    -- parent, or whose parent is IFDB-tombstoned (isDeleted=1) but still in
    -- Aidbox, stays at 0. Child-grain reconcile (see Patient_Address) is not
    -- enabled here.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Encounter_Location_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_Location_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_Location_dw_timestamp_update] DEFAULT SYSDATETIME(),
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

-- DQA comparison index (kept in sync with ddl/migrations/20260625_fhir_dqa_comparison_indexes.sql)
CREATE NONCLUSTERED
INDEX [IX_FHIR_Encounter_Location_LocationId] ON [fhir].[Encounter_Location] ([location_id] ASC) ON [PRIMARY];

END

-- ============================================================================
-- DocumentReference (from documentreference_view)
-- Note: one row per DocumentReference resource (epikrise + udskrivningsrapport,
-- distinguished by source_table). PK is documentreference_id.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[DocumentReference](
        [documentreference_id] NVARCHAR(255) NOT NULL,
        [source_table] NVARCHAR(100) NULL,
        [status] NVARCHAR(50) NULL,
        [doc_status] NVARCHAR(50) NULL,
        [type_code] NVARCHAR(512) NULL,
        [type_system] NVARCHAR(512) NULL,
        [type_display] NVARCHAR(255) NULL,
        [patient_id] NVARCHAR(255) NULL,
        [author_org_ref] NVARCHAR(512) NULL,
        [doc_date] NVARCHAR(50) NULL,
        [epikrise_id] NVARCHAR(255) NULL,
        [care_discharge_report_id] NVARCHAR(255) NULL,
        [notification_identifier] NVARCHAR(255) NULL,
        [priority] NVARCHAR(255) NULL,
        -- Projected from the resource's urn:fhir:isDeleted extension.
        -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
        -- (boolean_false_default_columns in sof_utils.py).
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_DocumentReference_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DocumentReference_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DocumentReference_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_DocumentReference] PRIMARY KEY CLUSTERED ([documentreference_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_DocumentReference_PatientId]
        ON [fhir].[DocumentReference] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_DocumentReference_LastUpdated]
        ON [fhir].[DocumentReference] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_DocumentReference_SourceTable]
        ON [fhir].[DocumentReference] ([source_table] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- DocumentReference Recipient (from documentreference_recipient_view)
-- Note: forEach over the recipients extension produces one row per recipient.
-- recipient_key is a stable per-recipient identity carried in the FHIR
-- extension (epikrise = EPIKRISE_MODTAGER.OBJEKT_ID; care_discharge =
-- RECEIVER_AFDELING_KODE|CCRECEIVER), mirroring CarePlan_Activity.
-- dw_is_deleted is warehouse-managed (not projected from the resource); see
-- Encounter_Location.
-- PK is composite: documentreference_id + recipient_key.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[DocumentReference_Recipient](
        [documentreference_id] NVARCHAR(255) NOT NULL,
        [recipient_key] NVARCHAR(255) NOT NULL CONSTRAINT [DF_DocumentReference_Recipient_recipient_key] DEFAULT N'',
        [recipient_type] NVARCHAR(100) NULL,
        [organization_ref] NVARCHAR(512) NULL,
        [recipient_name] NVARCHAR(512) NULL,
        [recipient_address] NVARCHAR(512) NULL,
        [cc_flag] NVARCHAR(10) NULL,
        [status_code] NVARCHAR(512) NULL,
        [status_text] NVARCHAR(255) NULL,
        [transmission_time] NVARCHAR(50) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the parent
        -- resource carries urn:fhir:isDeleted, but this view grain does not
        -- project the field -- it is set only by dqa_fhir_server_cleanup when the
        -- parent is hard-deleted in Aidbox, and cleared on MERGE match
        -- (resurrect_soft_deleted).
        -- NOTE the limit: `= 0` means "not marked by cleanup" -- NOT "still
        -- exists under the parent". A row that disappears from a still-live
        -- parent, or whose parent is IFDB-tombstoned (isDeleted=1) but still in
        -- Aidbox, stays at 0. Child-grain reconcile (see Patient_Address) is not
        -- enabled here.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_DocumentReference_Recipient_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_DocumentReference_Recipient_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_DocumentReference_Recipient_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_DocumentReference_Recipient]
            PRIMARY KEY CLUSTERED ([documentreference_id], [recipient_key])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_DocumentReference_Recipient_DocRefId]
        ON [fhir].[DocumentReference_Recipient] ([documentreference_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_DocumentReference_Recipient_LastUpdated]
        ON [fhir].[DocumentReference_Recipient] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Condition (from condition_view)
-- Grain: one row per Condition resource (PK: condition_id).
-- code/code_system/code_display carry the PRIMARY diagnosis coding, selected in
-- condition_view via code.coding.where(userSelected = true). SKS supplementary
-- codes (tillaegskoder — many are ATC medication codes) are NOT projected into
-- this diagnosis table; they remain on the Aidbox resource for a future
-- AllergyIntolerance model. See migrations/10_condition_diagnosis_code_only.sql.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[Condition]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[Condition](
        [condition_id] NVARCHAR(255) NOT NULL,
        [patient_id] NVARCHAR(255) NOT NULL,
        [encounter_id] NVARCHAR(255) NULL,
        [clinical_status] NVARCHAR(50) NULL,
        [verification_status] NVARCHAR(50) NULL,
        [recorded_date] NVARCHAR(50) NULL,
        [code] NVARCHAR(255) NULL,
        [code_system] NVARCHAR(500) NULL,
        [code_display] NVARCHAR(500) NULL,
        [diagnosetype] NVARCHAR(50) NULL,
        [category_code] NVARCHAR(100) NULL,
        [source_system] NVARCHAR(100) NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        -- Forloebsreference fra episodeOfCare-udvidelsen. Samme sti som paa
        -- fhir.Procedure, saa diagnose- og procedurespor kobles til forloeb
        -- paa samme maade paa tvaers af aftagerfladen.
        [episode_of_care_id] NVARCHAR(255) NULL,
        -- Registreringen skal ikke indberettes til Landspatientregisteret.
        -- Samme meta-tag som paa fhir.Procedure: sat naar kildens flag er
        -- sandt, saa loadet skriver 0 eller 1 og aldrig NULL. Raekker fra
        -- foer kolonnen fandtes staar NULL, indtil genkoerslen har skrevet
        -- dem om -- derefter strammes til NOT NULL.
        [lpr3_ignored] BIT NULL,
        [modtidspunkt] DATETIME2 NULL,
        [opdateret_tid] DATETIME2 NULL,
        -- Projected from the resource's urn:fhir:isDeleted extension.
        -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
        -- (boolean_false_default_columns in sof_utils.py).
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Condition_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Condition] PRIMARY KEY CLUSTERED ([condition_id])
    ) ON [PRIMARY] WITH (DATA_COMPRESSION = PAGE);

    -- PAGE-komprimering på tabel og indeks: indlæsningerne er disk-bundne, og de brede
    -- Unicode-kolonner komprimerer godt. Hvis CPU bliver flaskehalsen i stedet: ROW.
    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_PatientId]
        ON [fhir].[Condition] ([patient_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_EncounterId]
        ON [fhir].[Condition] ([encounter_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_LastUpdated]
        ON [fhir].[Condition] ([last_updated] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

    -- Kept in sync with migrations/05_condition_add_source_identifier_value.sql
    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_SourceIdentifierValue]
        ON [fhir].[Condition] ([source_system] ASC, [source_identifier_value] ASC)
        WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];
END
GO

-- ============================================================================
-- Condition_SupplementaryCode (from condition_supplementary_code_view)
-- forEach over non-userSelected codings on Condition.code.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Condition_SupplementaryCode]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[Condition_SupplementaryCode] (
        [condition_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [supplementary_code] NVARCHAR(255) NOT NULL,
        [supplementary_code_system] NVARCHAR(500) NOT NULL,
        [supplementary_code_display] NVARCHAR(500) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the parent
        -- resource carries urn:fhir:isDeleted, but this view grain does not
        -- project the field -- it is set by dqa_fhir_server_cleanup when the
        -- parent is hard-deleted in Aidbox, and cleared on MERGE match
        -- (resurrect_soft_deleted).
        -- Child-grain reconcile IS enabled for this grain (child_grain_reconcile
        -- on the view config), so a row that disappears from a still-live parent
        -- is also marked here -- unlike the grains where only cleanup writes it.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Condition_SupplementaryCode_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_SupplementaryCode_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_SupplementaryCode_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Condition_SupplementaryCode]
            PRIMARY KEY CLUSTERED ([condition_id], [supplementary_code], [supplementary_code_system])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_SupplementaryCode_ConditionId]
        ON [fhir].[Condition_SupplementaryCode] ([condition_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Condition_SupplementaryCode_LastUpdated]
        ON [fhir].[Condition_SupplementaryCode] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Condition_henvisning (from condition_henvisning_view)
-- forEach over henvisningId-sub-extensions on pasDiagnoseSourceMeta. One row per
-- (diagnose, henvisning); a PAS diagnosis can reference up to 24 referrals.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Condition_henvisning]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[Condition_henvisning] (
        [condition_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [henvisning_id] NVARCHAR(255) NOT NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed; see Condition_tillaegskode.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Condition_henvisning_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_henvisning_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_henvisning_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Condition_henvisning]
            PRIMARY KEY CLUSTERED ([condition_id], [henvisning_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ConditionHenvisning_HenvisningId]
        ON [fhir].[Condition_henvisning] ([henvisning_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ConditionHenvisning_LastUpdated]
        ON [fhir].[Condition_henvisning] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Observation_Speciale (from observation_speciale_view)
-- forEach over specialerne paa aktivitetsudfoerelsen. En udfoerelse kan have
-- flere specialer, saa de faar eget grain frem for kolonner paa Observation.
-- ============================================================================
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Observation_Speciale]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[Observation_Speciale] (
        [observation_id] NVARCHAR(255) NOT NULL,
        [speciale_kode] NVARCHAR(512) NOT NULL,
        [speciale_kodetekst] NVARCHAR(512) NULL,
        [speciale_hospital_kode] NVARCHAR(512) NULL,
        [speciale_hospital_navn] NVARCHAR(512) NULL,
        [speciale_hospital_orgkode] NVARCHAR(512) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed; see Condition_henvisning.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Observation_Speciale_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Observation_Speciale_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Observation_Speciale_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Observation_Speciale]
            PRIMARY KEY CLUSTERED ([observation_id], [speciale_kode])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ObservationSpeciale_SpecialeKode]
        ON [fhir].[Observation_Speciale] ([speciale_kode] ASC) ON [PRIMARY];
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
    [patient_id] NVARCHAR(255) NOT NULL,
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
    -- Projected from the resource's urn:fhir:isDeleted extension.
    -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
    -- (boolean_false_default_columns in sof_utils.py).
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Observation_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Observation_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Observation_dw_timestamp_update] DEFAULT SYSDATETIME(),
    -- Sargable change-detection column for SFI delta discovery
    -- (kept in sync with migrations/08_delta_discovery_effective_timestamp.sql)
    [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED,
    CONSTRAINT [PK_FHIR_Observation] PRIMARY KEY CLUSTERED ([observation_id])
) ON [PRIMARY] WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_PatientId] ON [fhir].[Observation] ([patient_id] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_LastUpdated] ON [fhir].[Observation] ([last_updated] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_Sfi]
    ON [fhir].[Observation] ([element_system] ASC, [element_code] ASC, [patient_id] ASC, [effective_date] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_IfdbAktivitet] ON [fhir].[Observation] ([ifdb_aktivitet_id] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Observation_SystemDWEffective]
    ON [fhir].[Observation] ([element_system] ASC, [dw_timestamp_effective] ASC)
    INCLUDE ([observation_id]) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

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
            ON [fhir].[Observation] ([element_system] ASC, [element_code] ASC, [patient_id] ASC, [effective_date] ASC)
            WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_FHIR_Observation_IfdbAktivitet'
    )
    AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'ifdb_aktivitet_id')
        CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_IfdbAktivitet]
            ON [fhir].[Observation] ([ifdb_aktivitet_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];
END
GO

-- ============================================================================
-- EpisodeOfCare (from episode_of_care_view)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[EpisodeOfCare](
        [episode_of_care_id] NVARCHAR(255) NOT NULL,
        [patient_id] NVARCHAR(255) NOT NULL,
        [status] NVARCHAR(50) NULL,
        [type_code] NVARCHAR(255) NULL,
        [type_display] NVARCHAR(500) NULL,
        -- Clock time as IFDB sends it (Danish local time); any offset is dropped at load,
        -- not converted (keep_datetime_wall_clock in sof_utils.py).
        [period_start] DATETIME2(0) NULL,
        [period_end] DATETIME2(0) NULL,
        [managing_organization_id] NVARCHAR(255) NULL,
        [identifier_value] NVARCHAR(255) NULL,
        [previous_episode_of_care_id] NVARCHAR(255) NULL,
        [previous_eoc_ref_type_code] NVARCHAR(255) NULL,
        [healthissue_id] NVARCHAR(255) NULL,
        [healthissue_code] NVARCHAR(255) NULL,
        [healthissue_text] NVARCHAR(128) NULL,
        [discharge_disposition_code] NVARCHAR(255) NULL,
        -- Projected from the resource's urn:fhir:isDeleted extension.
        -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
        -- (boolean_false_default_columns in sof_utils.py).
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_EpisodeOfCare_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_EpisodeOfCare_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_EpisodeOfCare_dw_timestamp_update] DEFAULT SYSDATETIME(),
        -- Set by fhir_dw_keys_episode_of_care, not by SoF. Last, as on a migrated table.
        [dw_ek_Borger] INT NOT NULL CONSTRAINT [DF_EpisodeOfCare_dw_ek_Borger] DEFAULT -1,
        [dw_sk_Borger] INT NOT NULL CONSTRAINT [DF_EpisodeOfCare_dw_sk_Borger] DEFAULT -1,
        [dw_ek_Organisation] INT NOT NULL CONSTRAINT [DF_EpisodeOfCare_dw_ek_Organisation] DEFAULT -1,
        CONSTRAINT [PK_FHIR_EpisodeOfCare] PRIMARY KEY CLUSTERED ([episode_of_care_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_PatientId]
        ON [fhir].[EpisodeOfCare] ([patient_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_LastUpdated]
        ON [fhir].[EpisodeOfCare] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_HealthissueId]
        ON [fhir].[EpisodeOfCare] ([healthissue_id] ASC) ON [PRIMARY];

    -- DQA comparison indexes (kept in sync with ddl/migrations/20260625_fhir_dqa_comparison_indexes.sql)
    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_IdentifierValue]
        ON [fhir].[EpisodeOfCare] ([identifier_value] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_ManagingOrganizationId]
        ON [fhir].[EpisodeOfCare] ([managing_organization_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_EpisodeOfCare_PreviousEpisodeOfCareId]
        ON [fhir].[EpisodeOfCare] ([previous_episode_of_care_id] ASC) ON [PRIMARY];
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
    [identifier_value_sor] NVARCHAR(255) NULL,
    -- Added by ddl/migrations/20260702_fhir_org_location_add_is_deleted.sql.
    -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the
    -- resource has no urn:fhir:isDeleted extension and the view does not
    -- project the field. Set only by dqa_fhir_server_cleanup; cleared on
    -- MERGE match (resurrect_soft_deleted). NOT NULL DEFAULT 0 so consumers
    -- can write `= 0` uniformly across fhir.* tables.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Location_dw_is_deleted] DEFAULT 0,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Location_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Location_dw_timestamp_update] DEFAULT SYSDATETIME(),
    -- Set by fhir_dw_keys_location, not by SoF. Last, as on a migrated table.
    [dw_ek_Organisation] INT NOT NULL CONSTRAINT [DF_Location_dw_ek_Organisation] DEFAULT -1,
    CONSTRAINT [PK_FHIR_Location] PRIMARY KEY CLUSTERED ([location_id])
) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Location_LastUpdated] ON [fhir].[Location] ([last_updated] ASC) ON [PRIMARY];

-- DQA comparison indexes (kept in sync with ddl/migrations/20260625_fhir_dqa_comparison_indexes.sql)
CREATE NONCLUSTERED
INDEX [IX_FHIR_Location_IdentifierValueSor] ON [fhir].[Location] ([identifier_value_sor] ASC) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_Location_ManagingOrganizationId] ON [fhir].[Location] ([managing_organization_id] ASC) ON [PRIMARY];

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
        -- Soft-delete flag (warehouse-managed). Not projected from Aidbox:
        -- organization_view has no isDeleted column (unlike Patient/Encounter).
        -- sof_organization is unpartitioned, so every run is a full snapshot:
        -- mark live rows missing from the extract as 1, MERGE-resurrect the
        -- rest as 0. DEFAULT is required: the SoF MERGE does not list this
        -- column on INSERT.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Organization_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Organization_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Organization_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Organization] PRIMARY KEY CLUSTERED ([organization_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Organization_LastUpdated]
        ON [fhir].[Organization] ([last_updated] ASC) ON [PRIMARY];

    -- DQA comparison indexes (kept in sync with ddl/migrations/20260625_fhir_dqa_comparison_indexes.sql)
    CREATE NONCLUSTERED INDEX [IX_FHIR_Organization_IdentifierValueSor]
        ON [fhir].[Organization] ([identifier_value_sor] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Organization_PartOfId]
        ON [fhir].[Organization] ([part_of_id] ASC) ON [PRIMARY];

    CREATE UNIQUE NONCLUSTERED INDEX [UQ_FHIR_Organization_IdentifierValueSor_Live]
        ON [fhir].[Organization] ([identifier_value_sor])
        WHERE [dw_is_deleted] = 0 AND [identifier_value_sor] IS NOT NULL;
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
    [patient_id] NVARCHAR(255) NOT NULL,
    [status] NVARCHAR(50) NULL,
    [intent] NVARCHAR(50) NULL,
    [category_code] NVARCHAR(100) NULL,
    [period_start] NVARCHAR(50) NULL,
    [period_end] NVARCHAR(50) NULL,
    [organization_id] NVARCHAR(255) NULL,
    [identifier_value] NVARCHAR(500) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the
    -- resource has no urn:fhir:isDeleted extension and the view does not
    -- project the field. Set only by dqa_fhir_server_cleanup; cleared on
    -- MERGE match (resurrect_soft_deleted). NOT NULL DEFAULT 0 so consumers
    -- can write `= 0` uniformly across fhir.* tables.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_CarePlan_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_CarePlan_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_CarePlan_dw_timestamp_update] DEFAULT SYSDATETIME(),
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
        [patient_id] NVARCHAR(255) NOT NULL,
        [category_code] NVARCHAR(100) NULL,
        [ifdb_eventmarker_id] NVARCHAR(255) NOT NULL,
        [episode_of_care_id] NVARCHAR(255) NULL,
        [outcome_codeable_concept_code] NVARCHAR(255) NULL,
        [outcome_codeable_concept_system] NVARCHAR(500) NULL,
        [outcome_codeable_concept_display] NVARCHAR(500) NULL,
        [organization_id] NVARCHAR(255) NULL,
        [practitioner_id] NVARCHAR(255) NULL,
        [activity_timestamp] NVARCHAR(50) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: set to 1
        -- by dqa_fhir_server_cleanup when the parent is hard-deleted in Aidbox,
        -- and by the child-grain reconcile when a marker disappears from a
        -- still-live CarePlan; cleared to 0 on MERGE match
        -- (resurrect_soft_deleted). NOT NULL DEFAULT 0, so `= 0` means "still
        -- present under the parent".
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_CarePlan_Activity_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_CarePlan_Activity_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_CarePlan_Activity_dw_timestamp_update] DEFAULT SYSDATETIME(),
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
    [patient_id] NVARCHAR(255) NOT NULL,
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
    [reason_reference_id] NVARCHAR(255) NULL,
    [authored_on] NVARCHAR(50) NULL,
    [occurrence_start] NVARCHAR(50) NULL,
    [occurrence_end] NVARCHAR(50) NULL,
    [requester_id] NVARCHAR(255) NULL,
    [performer_id] NVARCHAR(255) NULL,
    [source_table] NVARCHAR(100) NULL,
    [status_kode] NVARCHAR(255) NULL,
    [prioritet_kode] NVARCHAR(255) NULL,
    [henvisningsmaade_kode] NVARCHAR(255) NULL,
    [referral_purpose] NVARCHAR(1750) NULL,
    [modtaget] DATETIME2 NULL,
    [afsendelsesdato] DATETIME2 NULL,
    [henvist_til_yder_navn] NVARCHAR(512) NULL,
    [henvist_til_yder_nummer] NVARCHAR(512) NULL,
    [henvist_til_afdeling_kode] NVARCHAR(512) NULL,
    [henvist_til_afdeling_kodetekst] NVARCHAR(512) NULL,
    [henvist_til_muni_kode] NVARCHAR(512) NULL,
    [henvist_til_muni_location] NVARCHAR(512) NULL,
    [henvist_fra_yder_navn] NVARCHAR(512) NULL,
    [henvist_fra_yder_nummer] NVARCHAR(512) NULL,
    [henvist_fra_afdeling_kode] NVARCHAR(512) NULL,
    [henvist_fra_afdeling_kodetekst] NVARCHAR(512) NULL,
    [henvist_fra_institution_navn] NVARCHAR(512) NULL,
    [henvist_fra_institution_postnr] NVARCHAR(512) NULL,
    [henvist_fra_institution_vej] NVARCHAR(512) NULL,
    [henvist_fra_institution_tlf_nr] NVARCHAR(512) NULL,
    [afslutningsaarsag_kode] NVARCHAR(512) NULL,
    [afslutningsaarsag_kodetekst] NVARCHAR(512) NULL,
    [afsender_yder_navn] NVARCHAR(512) NULL,
    [afsender_yder_nummer] NVARCHAR(512) NULL,
    [afsender_afdeling_kode] NVARCHAR(512) NULL,
    [afsender_afdeling_kodetekst] NVARCHAR(512) NULL,
    [afsender_institution_navn] NVARCHAR(512) NULL,
    [afsender_institution_postnr] NVARCHAR(512) NULL,
    [afsender_institution_vej] NVARCHAR(512) NULL,
    [afsender_institution_tlf_nr] NVARCHAR(512) NULL,
    [behandler_yder_navn] NVARCHAR(512) NULL,
    [behandler_yder_nummer] NVARCHAR(512) NULL,
    [behandler_afdeling_kode] NVARCHAR(512) NULL,
    [behandler_afdeling_kodetekst] NVARCHAR(512) NULL,
    [sent_electronical] NVARCHAR(512) NULL,
    [henvist_til_afdeling_sorkode] NVARCHAR(512) NULL,
    [henvist_fra_afdeling_sorkode] NVARCHAR(512) NULL,
    [afsender_afdeling_sorkode] NVARCHAR(512) NULL,
    [behandler_afdeling_sorkode] NVARCHAR(512) NULL,
    [henvisningsdato] DATETIME2 NULL,
    [slutdato] DATETIME2 NULL,
    [faellesvisitation] NVARCHAR(512) NULL,
    -- Projected from the resource's urn:fhir:isDeleted extension.
    -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
    -- (boolean_false_default_columns in sof_utils.py).
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_ServiceRequest_dw_is_deleted] DEFAULT 0,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_ServiceRequest_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_ServiceRequest_dw_timestamp_update] DEFAULT SYSDATETIME(),
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
-- ServiceRequest_SupplementaryCode (from servicerequest_supplementary_code_view)
-- forEach over orderDetail.coding on ServiceRequest.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_SupplementaryCode]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[ServiceRequest_SupplementaryCode] (
        [service_request_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [supplementary_code] NVARCHAR(255) NOT NULL,
        [supplementary_code_system] NVARCHAR(500) NOT NULL,
        [supplementary_code_display] NVARCHAR(500) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the parent
        -- resource carries urn:fhir:isDeleted, but this view grain does not
        -- project the field -- it is set by dqa_fhir_server_cleanup when the
        -- parent is hard-deleted in Aidbox, and cleared on MERGE match
        -- (resurrect_soft_deleted).
        -- Child-grain reconcile IS enabled for this grain (child_grain_reconcile
        -- on the view config), so a row that disappears from a still-live parent
        -- is also marked here -- unlike the grains where only cleanup writes it.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_ServiceRequest_SupplementaryCode_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_ServiceRequest_SupplementaryCode_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_ServiceRequest_SupplementaryCode_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_ServiceRequest_SupplementaryCode]
            PRIMARY KEY CLUSTERED ([service_request_id], [supplementary_code], [supplementary_code_system])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_SupplementaryCode_SrId]
        ON [fhir].[ServiceRequest_SupplementaryCode] ([service_request_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_SupplementaryCode_LastUpdated]
        ON [fhir].[ServiceRequest_SupplementaryCode] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- ServiceRequest_Recipient (from the rehabilitation_plan_receivers
-- extension via servicerequest_recipient_view). One row per receiver of a
-- genoptræningsplan. recipient_key is arc.ifdb_rehabilitation_plan_receiver.modtager_noegle:
-- the receiver's yder number, else department code, else SOR code.
-- dw_is_deleted is warehouse-managed (not projected from the resource); see
-- Encounter_Location.
-- PK is composite: service_request_id + recipient_key.
-- NB: only ServiceRequests from source_table='REHABILITATION_PLAN' carry the
-- receivers extension; HENVISNING/RIS rows produce 0 child rows (forEach over the
-- absent extension — inner-join semantics, so no fallback rows).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[ServiceRequest_Recipient](
        [service_request_id] NVARCHAR(255) NOT NULL,
        [recipient_key] NVARCHAR(255) NOT NULL CONSTRAINT [DF_ServiceRequest_Recipient_recipient_key] DEFAULT N'',
        [receiver_type] NVARCHAR(100) NULL,
        [yder_nummer] NVARCHAR(50) NULL,
        [yder_navn] NVARCHAR(512) NULL,
        [afdeling_kode] NVARCHAR(512) NULL,
        [afdeling_kodetekst] NVARCHAR(255) NULL,
        [organization_ref] NVARCHAR(512) NULL,
        [status_code] NVARCHAR(512) NULL,
        [status_text] NVARCHAR(255) NULL,
        [transmission_time] NVARCHAR(50) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the parent
        -- resource carries urn:fhir:isDeleted, but this view grain does not
        -- project the field -- it is set only by dqa_fhir_server_cleanup when the
        -- parent is hard-deleted in Aidbox, and cleared on MERGE match
        -- (resurrect_soft_deleted).
        -- NOTE the limit: `= 0` means "not marked by cleanup" -- NOT "still
        -- exists under the parent". A row that disappears from a still-live
        -- parent, or whose parent is IFDB-tombstoned (isDeleted=1) but still in
        -- Aidbox, stays at 0. Child-grain reconcile (see Patient_Address) is not
        -- enabled here.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_ServiceRequest_Recipient_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_ServiceRequest_Recipient_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_ServiceRequest_Recipient_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_ServiceRequest_Recipient]
            PRIMARY KEY CLUSTERED ([service_request_id], [recipient_key])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_Recipient_ServiceRequestId]
        ON [fhir].[ServiceRequest_Recipient] ([service_request_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequest_Recipient_LastUpdated]
        ON [fhir].[ServiceRequest_Recipient] ([last_updated] ASC) ON [PRIMARY];
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
    [patient_id] NVARCHAR(255) NOT NULL,
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
    [source_system] NVARCHAR(100) NULL,
    -- Forretningsnoeglen fra den kilde source_system peger paa. ifdb_aktivitet_id
    -- daekker kun SFI, saa uden denne kan en procedure fra administrative
    -- procedurer eller RIS ikke spores tilbage til sin kilderaekke.
    [source_identifier_value] NVARCHAR(255) NULL,
    [ifdb_aktivitet_id] NVARCHAR(255) NULL,
    -- Kildens egen statuskode og -tekst bevares raat ved siden af FHIR-status,
    -- der kun skelner mellem gennemfoert og tilbagekaldt registrering.
    [status_code] NVARCHAR(512) NULL,
    [status_text] NVARCHAR(500) NULL,
    -- Procedurens egne oplysninger bevares også ved lav udfyldning.
    [used_contrast_code] NVARCHAR(512) NULL,
    [used_contrast_text] NVARCHAR(512) NULL,
    [staff_category_code] NVARCHAR(512) NULL,
    -- Udfoerende enhed og stamafdeling som opløste Organization-referencer
    -- (Aidbox UUID) -- join til fhir.Organization for navn, type og hierarki.
    -- Kildens raa organisationsoplysninger bevares i arkiv/FHIR-extensions,
    -- ikke som parallelle organisationsattributter paa Procedure.
    -- Kildesystemets egne afdelingskoder. De findes hverken i SOR eller SHAK
    -- (maalt: 0 traeffere for alle seks), saa de kan ikke hentes ved opslag i
    -- Organization. Derfor staar de her; det er ikke en kopi af opslagsdata.
    [location_code] NVARCHAR(255) NULL,
    [location_orgcode] NVARCHAR(512) NULL,
    [stamafdeling_kode] NVARCHAR(512) NULL,
    [stamafdeling_orgkode] NVARCHAR(512) NULL,
    [udfoert_enhed_kode] NVARCHAR(512) NULL,
    [udfoert_enhed_orgkode] NVARCHAR(512) NULL,
    [performer_organization_id] NVARCHAR(255) NULL,
    [stamafdeling_organization_id] NVARCHAR(255) NULL,
    -- Klinikeren fra performer: der findes ingen Practitioner-ressourcer, saa
    -- referencen bevares som identifier og medarbejder-id projiceres direkte,
    -- som paa Observation og QuestionnaireResponse.
    [practitioner_id] NVARCHAR(255) NULL,
    [practitioner_display] NVARCHAR(500) NULL,
    [practitioner_role_code] NVARCHAR(255) NULL,
    [practitioner_role_display] NVARCHAR(500) NULL,
    [epji_created_at] DATETIME2 NULL,
    [epji_modified_at] DATETIME2 NULL,
    [ifdb_modified_at] NVARCHAR(50) NULL,
    -- Peger fra en deloperation til hovedoperationen. NULL paa hovedoperationer.
    -- Uden den kan 1+N-strukturen ikke opgoeres fladt: man kan ikke taelle
    -- operationer uden at blande hoved og dele sammen.
    [part_of_id] NVARCHAR(255) NULL,
    -- Knivtid: det tidsrum der reelt blev opereret, til forskel fra
    -- performed_period, som er kilderaekkens administrative aktivitetsperiode.
    -- DATETIME2 og ikke NVARCHAR som performed_period: knivtid skal kunne
    -- regnes paa. Vaerdien normaliseres til dansk tid ved load.
    [knivtid_start] DATETIME2 NULL,
    [knivtid_slut] DATETIME2 NULL,
    -- Beregnet i byggelaget af de to tidspunkter, saa varigheden ikke kraever
    -- strengparsning i SQL. NULL naar et af tidspunkterne mangler.
    [knivtid_minutter] INT NULL,
    -- Registreringen skal ikke indberettes til Landspatientregisteret. Bygget
    -- paa et meta.tag, der kun saettes naar flaget er sandt, saa loadet skriver
    -- 0 eller 1 og aldrig NULL. Raekker fra foer kolonnen fandtes staar NULL,
    -- indtil genkoerslen har skrevet dem om -- derefter strammes til NOT NULL.
    -- Skal skelnes "kilden sagde nej" fra "kilden sagde ingenting", staar den
    -- boolske vaerdi i kilde-metadata paa ressourcen.
    [lpr3_ignored] BIT NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    -- Projected from the resource's urn:fhir:isDeleted extension.
    -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
    -- (boolean_false_default_columns in sof_utils.py).
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Procedure_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Procedure_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Procedure_dw_timestamp_update] DEFAULT SYSDATETIME(),
    CONSTRAINT [PK_FHIR_Procedure] PRIMARY KEY CLUSTERED ([procedure_id])
) ON [PRIMARY] WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_PatientId]
        ON [fhir].[Procedure] ([patient_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_EncounterId]
        ON [fhir].[Procedure] ([encounter_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_code]
        ON [fhir].[Procedure] ([code] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_LastUpdated]
        ON [fhir].[Procedure] ([last_updated] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_IfdbAktivitet]
        ON [fhir].[Procedure] ([ifdb_aktivitet_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_PerformerOrganizationId]
        ON [fhir].[Procedure] ([performer_organization_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

END

-- ============================================================================
-- Procedure_SupplementaryCode (from procedure_supplementary_code_view)
-- forEach over non-userSelected codings on Procedure.code.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_SupplementaryCode]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[Procedure_SupplementaryCode] (
        [procedure_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [supplementary_code] NVARCHAR(255) NOT NULL,
        [supplementary_code_system] NVARCHAR(500) NOT NULL,
        [supplementary_code_display] NVARCHAR(500) NULL,
        -- Kildens raekkefoelge af tillaegskoder (1 = foerste). Kun administrative
        -- procedurer; NULL for RIS og aktivitetsresultater.
        [idx] INT NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the parent
        -- resource carries urn:fhir:isDeleted, but this view grain does not
        -- project the field -- it is set by dqa_fhir_server_cleanup when the
        -- parent is hard-deleted in Aidbox, and cleared on MERGE match
        -- (resurrect_soft_deleted).
        -- Child-grain reconcile IS enabled for this grain (child_grain_reconcile
        -- on the view config), so a row that disappears from a still-live parent
        -- is also marked here -- unlike the grains where only cleanup writes it.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Procedure_SupplementaryCode_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Procedure_SupplementaryCode_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Procedure_SupplementaryCode_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Procedure_SupplementaryCode]
            PRIMARY KEY CLUSTERED ([procedure_id], [supplementary_code], [supplementary_code_system])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_SupplementaryCode_ProcedureId]
        ON [fhir].[Procedure_SupplementaryCode] ([procedure_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_SupplementaryCode_LastUpdated]
        ON [fhir].[Procedure_SupplementaryCode] ([last_updated] ASC) ON [PRIMARY];
END
GO

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
    ON [fhir].[Procedure] ([ifdb_aktivitet_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

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
    -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the
    -- resource has no urn:fhir:isDeleted extension and the view does not
    -- project the field. Set only by dqa_fhir_server_cleanup; cleared on
    -- MERGE match (resurrect_soft_deleted). NOT NULL DEFAULT 0 so consumers
    -- can write `= 0` uniformly across fhir.* tables.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Questionnaire_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Questionnaire_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Questionnaire_dw_timestamp_update] DEFAULT SYSDATETIME(),
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
-- PK is composite: questionnaire_id + link_id_exact + item_key. link_id is the SFI
-- concept; item_key is native Questionnaire.item.id and preserves repeated
-- occurrences without deduplication. link_id_exact is link_id compared byte for
-- byte: the source spells some SFI codes in two cases (...txt / ...TXT), Aidbox
-- keeps both, and the case-insensitive database collation would merge them.
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
    [link_id] NVARCHAR(255) NOT NULL CONSTRAINT [DF_Questionnaire_item_link_id] DEFAULT N'',
    [item_key] NVARCHAR(100) NOT NULL CONSTRAINT [DF_Questionnaire_item_item_key] DEFAULT N'',
    [text] NVARCHAR(MAX) NULL,
    [item_type] NVARCHAR(50) NULL,
    [repeats] NVARCHAR(10) NULL,
    [element_system] NVARCHAR(500) NULL,
    [element_code] NVARCHAR(255) NULL,
    [element_display] NVARCHAR(500) NULL,
    [last_updated] DATETIME2 NULL,
    [created_at] DATETIME2 NULL,
    -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: set to 1
    -- only by dqa_fhir_server_cleanup when the parent is hard-deleted in
    -- Aidbox; cleared to 0 on MERGE match (resurrect_soft_deleted).
    -- NOT NULL DEFAULT 0.
    -- NOTE the limit: this is a child-grain table, so `= 0` means "not
    -- marked by cleanup" -- NOT "still exists under the parent". A row
    -- that disappears from a still-live parent stays at 0. Child-grain
    -- reconcile (see Patient_Address) is not enabled here.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Questionnaire_item_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Questionnaire_item_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Questionnaire_item_dw_timestamp_update] DEFAULT SYSDATETIME(),
    [link_id_exact] AS ([link_id] COLLATE Latin1_General_BIN2) PERSISTED NOT NULL,
    CONSTRAINT [PK_FHIR_Questionnaire_item]
        PRIMARY KEY CLUSTERED ([questionnaire_id], [link_id_exact], [item_key])
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
    -- Soft-delete flag (0 = live, 1 = deleted). Kilde-afledt: ressourcen baerer
    -- urn:fhir:isDeleted sat af kildens SLETTET, og viewet projicerer den --
    -- samme moenster som Procedure og Condition. Foer projiceringen afspejlede
    -- kolonnen KUN dqa_fhir_server_cleanup, saa en aftager kunne ikke se at et
    -- skemasvar var slettet i kilden, selvom barnetabellen
    -- QuestionnaireResponse_item havde markeringen. Ryddes ogsaa af cleanup og
    -- nulstilles paa MERGE-match (resurrect_soft_deleted). NOT NULL DEFAULT 0
    -- saa aftagere kan skrive `= 0` ensartet paa tvaers af fhir.*-tabellerne.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_QuestionnaireResponse_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_QuestionnaireResponse_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_QuestionnaireResponse_dw_timestamp_update] DEFAULT SYSDATETIME(),
    -- Sargable change-detection column for SFI delta discovery
    -- (kept in sync with migrations/08_delta_discovery_effective_timestamp.sql)
    [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED,
    CONSTRAINT [PK_FHIR_QuestionnaireResponse] PRIMARY KEY CLUSTERED ([qr_id])
) ON [PRIMARY] WITH (DATA_COMPRESSION = PAGE);

-- Fem sekundære indeks vedligeholdes pr. indsat række; under backfill-vinduer slås de
-- fra og genopbygges (sof_utils.resolve_sof_backfill_plan). Hvis LastUpdated aldrig
-- læses (sys.dm_db_index_usage_stats), kan det droppes.
CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_PatientAuthored]
    ON [fhir].[QuestionnaireResponse] ([patient_id] ASC, [authored] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_SourceAktivitet]
    ON [fhir].[QuestionnaireResponse] ([source_aktivitet_id] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_LastUpdated]
    ON [fhir].[QuestionnaireResponse] ([last_updated] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_CanonicalVersion]
    ON [fhir].[QuestionnaireResponse] (
        [questionnaire_canonical] ASC,
        [questionnaire_version] ASC
    )
    INCLUDE ([patient_id], [authored], [source_model_name])
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QR_DWEffective]
    ON [fhir].[QuestionnaireResponse] ([dw_timestamp_effective] ASC)
    INCLUDE ([qr_id]) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

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
            INCLUDE ([patient_id], [authored], [source_model_name])
            WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];
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
    [link_id] NVARCHAR(255) NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_link_id] DEFAULT N'',
    [item_key] NVARCHAR(100) NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_item_key] DEFAULT N'',
    [text] NVARCHAR(MAX) NULL,
    [answer_index] NVARCHAR(10) NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_answer_index] DEFAULT N'',
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
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed: the parent
        -- resource carries urn:fhir:isDeleted, but this view grain does not
        -- project the field -- it is set by dqa_fhir_server_cleanup when the
        -- parent is hard-deleted in Aidbox, and cleared on MERGE match
        -- (resurrect_soft_deleted).
        -- Child-grain reconcile IS enabled for this grain (child_grain_reconcile
        -- on the view config), so a row that disappears from a still-live parent
        -- is also marked here -- unlike the grains where only cleanup writes it.
    [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_dw_is_deleted] DEFAULT 0,
    [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_dw_timestamp_insert] DEFAULT SYSDATETIME(),
    [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_dw_timestamp_update] DEFAULT SYSDATETIME(),
    -- Sargable change-detection column for SFI delta discovery
    -- (kept in sync with migrations/08_delta_discovery_effective_timestamp.sql)
    [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED,
    CONSTRAINT [PK_FHIR_QuestionnaireResponse_item]
        PRIMARY KEY CLUSTERED ([qr_id], [link_id], [item_key], [answer_index])
) ON [PRIMARY] WITH (DATA_COMPRESSION = PAGE);

-- Intet indeks på qr_id alene: det er PK'ens første kolonne, og opslag pr. forælder
-- seeker på PK-præfikset. Et sådant indeks kostede en tilfældig skrivning pr. række
-- uden at give noget.
CREATE NONCLUSTERED
INDEX [IX_FHIR_QRItem_LinkId]
    ON [fhir].[QuestionnaireResponse_item] ([link_id] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QRItem_LastUpdated]
    ON [fhir].[QuestionnaireResponse_item] ([last_updated] ASC)
    WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

CREATE NONCLUSTERED
INDEX [IX_FHIR_QRI_DWEffective]
    ON [fhir].[QuestionnaireResponse_item] ([dw_timestamp_effective] ASC)
    INCLUDE ([qr_id]) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];

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
        ADD [item_key] NVARCHAR(100) NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_item_key] DEFAULT N'';
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
        ADD [answer_index] NVARCHAR(10) NOT NULL CONSTRAINT [DF_QuestionnaireResponse_item_answer_index] DEFAULT N'';
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
        -- Projected from the resource's urn:fhir:isDeleted extension.
        -- NOT NULL DEFAULT 0; a missing extension coalesces to 0 at load
        -- (boolean_false_default_columns in sof_utils.py).
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Appointment_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Appointment_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Appointment_dw_timestamp_update] DEFAULT SYSDATETIME(),
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

-- ============================================================================
-- WardUnit (from ward_unit_view)
-- Location resources identified by urn:source:ifdb:sengeenhed_id.
-- Not a row in fhir.Location (that view whitelists SOR identifiers).
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[WardUnit]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[WardUnit] (
        [ward_unit_id] NVARCHAR(255) NOT NULL,
        [name] NVARCHAR(500) NULL,
        [status] NVARCHAR(50) NULL,
        [managing_organization_id] NVARCHAR(255) NULL,
        [identifier_value] NVARCHAR(255) NULL,
        [admin_unit_code] NVARCHAR(512) NULL,
        [admin_unit_name] NVARCHAR(512) NULL,
        [admin_unit_organization_code] NVARCHAR(512) NULL,
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_WardUnit_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_WardUnit_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_WardUnit_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_WardUnit] PRIMARY KEY CLUSTERED ([ward_unit_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_WardUnit_IdentifierValue]
        ON [fhir].[WardUnit] ([identifier_value] ASC) ON [PRIMARY];

    -- Joinnoeglen fra belaegning til kapacitet: fhir.Bed.ward_unit_code og
    -- fhir.Encounter_BedSpace.ward_unit_code slaar op her, filtreret paa
    -- status = 'active' AND dw_is_deleted = 0 (kilden garanterer kun hoejst én
    -- aktiv sengeenhed pr. kode). De to filterkolonner er INCLUDE'et, saa
    -- opslaget daekkes af indekset.
    CREATE NONCLUSTERED INDEX [IX_FHIR_WardUnit_AdminUnitCode]
        ON [fhir].[WardUnit] ([admin_unit_code] ASC)
        INCLUDE ([ward_unit_id], [status], [dw_is_deleted]) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_WardUnit_LastUpdated]
        ON [fhir].[WardUnit] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- WardUnit_Capacity (from ward_unit_capacity_view)
-- Observation resources coded on urn:source:ifdb:sengeenhed_kapacitet_type.
-- Not a row in fhir.Observation (that view whitelists SFI).
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[WardUnit_Capacity]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[WardUnit_Capacity] (
        [observation_id] NVARCHAR(255) NOT NULL,
        [ward_unit_id] NVARCHAR(255) NULL,
        [status] NVARCHAR(50) NULL,
        [category_code] NVARCHAR(50) NULL,
        [capacity_type] NVARCHAR(50) NULL,
        [capacity_type_system] NVARCHAR(500) NULL,
        [effective_period_start] NVARCHAR(50) NULL,
        [effective_period_end] NVARCHAR(50) NULL,
        [count] NVARCHAR(50) NULL,
        [identifier_value] NVARCHAR(255) NULL,
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_WardUnit_Capacity_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_WardUnit_Capacity_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_WardUnit_Capacity_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_WardUnit_Capacity] PRIMARY KEY CLUSTERED ([observation_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_WardUnit_Capacity_WardUnitId]
        ON [fhir].[WardUnit_Capacity] ([ward_unit_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_WardUnit_Capacity_IdentifierValue]
        ON [fhir].[WardUnit_Capacity] ([identifier_value] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_WardUnit_Capacity_LastUpdated]
        ON [fhir].[WardUnit_Capacity] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Bed (from bed_view)
-- Location resources identified by urn:source:ifdb:bed_id, physicalType=bd.
-- Not a row in fhir.Location (that view whitelists SOR identifiers).
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Bed]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[Bed] (
        [bed_id] NVARCHAR(255) NOT NULL,
        [name] NVARCHAR(500) NULL,
        [status] NVARCHAR(50) NULL,
        [physical_type] NVARCHAR(50) NULL,
        [part_of_location_id] NVARCHAR(255) NULL,
        [identifier_value] NVARCHAR(255) NULL,
        [room] NVARCHAR(512) NULL,
        [ward_unit_code] NVARCHAR(512) NULL,
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Bed_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Bed_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Bed_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Bed] PRIMARY KEY CLUSTERED ([bed_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Bed_IdentifierValue]
        ON [fhir].[Bed] ([identifier_value] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Bed_WardUnitCode]
        ON [fhir].[Bed] ([ward_unit_code] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Bed_LastUpdated]
        ON [fhir].[Bed] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Encounter_BedSpace (from encounter_bed_space_view)
-- Encounter.location entries with physicalType=bd. Department entries stay
-- in fhir.Encounter_Location (that view excludes bd).
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_BedSpace]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[Encounter_BedSpace] (
        [encounter_id] NVARCHAR(255) NOT NULL,
        [location_id] NVARCHAR(255) NOT NULL CONSTRAINT [DF_Encounter_BedSpace_location_id] DEFAULT N'',
        [location_status] NVARCHAR(50) NULL,
        [location_period_start] NVARCHAR(50) NOT NULL CONSTRAINT [DF_Encounter_BedSpace_location_period_start] DEFAULT N'',
        [location_period_end] NVARCHAR(50) NULL,
        [ward_unit_code] NVARCHAR(512) NULL,
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Encounter_BedSpace_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_BedSpace_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_BedSpace_dw_timestamp_update] DEFAULT SYSDATETIME(),
        PRIMARY KEY CLUSTERED (
            [encounter_id],
            [location_id],
            [location_period_start]
        )
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_BedSpace_EncounterId]
        ON [fhir].[Encounter_BedSpace] ([encounter_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_BedSpace_WardUnitCode]
        ON [fhir].[Encounter_BedSpace] ([ward_unit_code] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_BedSpace_LastUpdated]
        ON [fhir].[Encounter_BedSpace] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Encounter_Absence (from encounter_absence_view)
-- One row per Encounter absence parent-extension (period start is the grain).
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Absence]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[Encounter_Absence] (
        [encounter_id] NVARCHAR(255) NOT NULL,
        [period_start] NVARCHAR(50) NOT NULL CONSTRAINT [DF_Encounter_Absence_period_start] DEFAULT N'',
        [period_end] NVARCHAR(50) NULL,
        [type_code] NVARCHAR(512) NULL,
        [type_display] NVARCHAR(512) NULL,
        [expected_return] NVARCHAR(50) NULL,
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Encounter_Absence_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_Absence_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Encounter_Absence_dw_timestamp_update] DEFAULT SYSDATETIME(),
        PRIMARY KEY CLUSTERED ([encounter_id], [period_start])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_Absence_EncounterId]
        ON [fhir].[Encounter_Absence] ([encounter_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_Encounter_Absence_LastUpdated]
        ON [fhir].[Encounter_Absence] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- Practitioner (from practitioner_view) — SilkeborgData E26PERS
--
-- Grain: one row per person (practitioner_id), keyed on CprNr in the source.
-- Tjenestenummer identifies the EMPLOYMENT, not the person, and is reissued to
-- new staff, so it cannot key this table — see fhir.PractitionerRole.
--
-- No profile is declared: DK Core's dk-core-practitioner carries the
-- authorisation code from the authorisation register, and a payroll extract has
-- none. These rows say who is on the payroll, never who is licensed to
-- practise, and `active` means "present in the most recent payroll delivery".
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[Practitioner]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[Practitioner] (
        [practitioner_id] NVARCHAR(255) NOT NULL,
        [identifier_cpr] NVARCHAR(10) NULL,
        -- The CPR this person held before a correction, from the
        -- urn:source:silkeborgdata:afloest_cpr extension. Filled for 4 of 53.129
        -- persons. It is an extension and not a second SYSTEM_CPR identifier
        -- because the identifier lookup reads only the first per system. The
        -- superseded number also owns its own row here, with active = 0.
        [identifier_cpr_previous] NVARCHAR(10) NULL,
        -- Source format is "Efternavn,Fornavn(e)"; name_text keeps the raw
        -- value, so a wrong split can always be checked against the original.
        [name_text] NVARCHAR(100) NULL,
        [name_family] NVARCHAR(100) NULL,
        [name_given] NVARCHAR(100) NULL,
        -- Both derived from CPR and absent when the number fails the validity
        -- gate (10 digits + a parsable date part) or implies an age under 13.
        [gender] NVARCHAR(20) NULL,
        [birth_date] DATE NULL,
        -- BIT, not NVARCHAR: the SOF view projects FHIR's boolean `active`
        -- straight through, and fhir.Patient already lands it in a BIT column
        -- the same way. A text column would make the natural report predicate
        -- `WHERE active = 1` fail with a conversion error. (fhir.Organization
        -- is NVARCHAR(10) for historical reasons and is not touched here.)
        [active] BIT NULL,
        [source_table] NVARCHAR(100) NULL,
        -- Projected from the urn:fhir:isDeleted extension, which the transform
        -- always sets. The source never deletes rows, but the transform sets 1 on
        -- a retired identity: the CPR a person held before a correction keeps its
        -- own row, and that row is not a person to be counted. Everyone who simply
        -- left the payroll stays 0 and is told apart by active = 0 —
        -- filtering them out here would hide thousands of former staff that
        -- historical data still refers to. dqa_fhir_server_cleanup also sets 1.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Practitioner_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Practitioner_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Practitioner_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Practitioner] PRIMARY KEY CLUSTERED ([practitioner_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED
    INDEX [IX_FHIR_Practitioner_LastUpdated] ON [fhir].[Practitioner] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED
    INDEX [IX_FHIR_Practitioner_IdentifierCpr] ON [fhir].[Practitioner] ([identifier_cpr] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- PractitionerRole (from practitionerrole_view) — SilkeborgData E26PERS
--
-- Grain: one row per employment EPISODE (Inst + Tjenestenummer + the episode's
-- first delivery period). A person can hold several concurrent employments, so
-- this is NOT one row per practitioner. The same Tjenestenummer can also be
-- held by different people over time, each of whom gets their own row.
--
-- The role codes live in PractitionerRole.code, one CodeableConcept per source
-- column, told apart by coding.system — never by array position, which FHIR
-- does not guarantee. The columns below mirror that filtering one-to-one, so
-- view JSON and DDL cannot drift apart.
--
-- No organization_id / location_id: SilkeborgData's Inst and Afdelingskode are
-- neither SOR nor SHAK, so there is nothing to reference. Both are carried as
-- extensions and projected into the plain columns below.
--
-- NOT a child table: PractitionerRole is a resource in its own right, with its
-- own ViewDefinition and its own DFM checkpoint. It therefore carries
-- dw_is_deleted like the other top-level tables — the transform always writes 0
-- (the source never deletes), and dqa_fhir_server_cleanup sets 1.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]') AND type = 'U')
BEGIN
    CREATE TABLE [fhir].[PractitionerRole] (
        [practitioner_role_id] NVARCHAR(255) NOT NULL,
        [identifier_tjenestenummer] NVARCHAR(50) NULL,
        [practitioner_id] NVARCHAR(255) NULL,
        [inst] NVARCHAR(2) NULL,
        -- No separate tjenestenummer column: the identifier is the composite
        -- "{Inst}-{Tjenestenummer}-{YYYYMMDD}", and un-concatenating it in
        -- FHIRPath would add an extension purely to undo our own concatenation.
        -- Inst is always 2 characters, so SUBSTRING(identifier_tjenestenummer, 4, 5)
        -- is still exact for the bare Tjenestenummer. The third segment is the
        -- episode's first delivery period, read as
        -- SUBSTRING(identifier_tjenestenummer, 10, 8) in YYYYMMDD format.
        -- 2+1+5+1+8 = 17 characters, so NVARCHAR(50) still comfortably fits it.
        [afdelingskode] NVARCHAR(4) NULL,
        [role_stillingskode] NVARCHAR(4) NULL,
        [role_stilling_text] NVARCHAR(100) NULL,
        [role_lederkode] NVARCHAR(2) NULL,
        [specialty_disco] NVARCHAR(6) NULL,
        -- Ansaettelsesdato. Absent when the source date is a missing-marker
        -- (0001-01-01, 2050-06-01, 2099-12-31) or later than the delivery period.
        [period_start] DATE NULL,
        -- Payroll status codes, carried verbatim and never translated: the
        -- consumers of this table know them from the payroll system. They sit
        -- beside `active`, which only means "present in the most recent
        -- delivery" — employment status is read from these, not from that flag.
        [role_statuskode] NVARCHAR(1) NULL,
        [role_jobstatuskode] NVARCHAR(1) NULL,
        -- When the status took effect. Not period.end: mapping it there would
        -- assert a meaning for the status code that no code list here supports.
        [role_statusdato] DATE NULL,
        -- Two more untranslated payroll codes. Tjenestekode is 89,6 % filled
        -- (13 values), but "0" on 86,8 % of the newest period, so the informative
        -- tail is ~3 % of roles — the same order as role_lederkode. "0" is stored
        -- verbatim: whether it means "none" is not known here. Omraadekode is
        -- 100 % filled with 3 values, "1" on 90,1 %, and changes over time on
        -- 21,4 % of employments, so this column is a snapshot of a state and its
        -- history lives only in arc.silkeborgdata_e26_pers.
        [role_tjenestekode] NVARCHAR(1) NULL,
        [role_omraadekode] NVARCHAR(1) NULL,
        -- BIT for the same reason as fhir.Practitioner.active.
        [active] BIT NULL,
        [source_table] NVARCHAR(100) NULL,
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_PractitionerRole_dw_is_deleted] DEFAULT 0,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_PractitionerRole_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_PractitionerRole_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_PractitionerRole] PRIMARY KEY CLUSTERED ([practitioner_role_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED
    INDEX [IX_FHIR_PractitionerRole_LastUpdated] ON [fhir].[PractitionerRole] ([last_updated] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED
    INDEX [IX_FHIR_PractitionerRole_PractitionerId] ON [fhir].[PractitionerRole] ([practitioner_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED
    INDEX [IX_FHIR_PractitionerRole_IdentifierTjenestenummer] ON [fhir].[PractitionerRole] ([identifier_tjenestenummer] ASC) ON [PRIMARY];
END
GO

-- Columns added after the tables were first created. The CREATE blocks above
-- are IF NOT EXISTS-guarded, so an environment that already provisioned them
-- would never see a new column without these.
IF EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[Practitioner]') AND type = 'U'
)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Practitioner]') AND name = N'identifier_cpr_previous')
        ALTER TABLE [fhir].[Practitioner] ADD [identifier_cpr_previous] NVARCHAR(10) NULL;

    -- active was NVARCHAR(10) ('true'/'false') on the first cut of this table.
    -- Convert in place: an environment on an earlier commit of this branch
    -- already holds the text form, and ALTER COLUMN alone cannot cast 'true'.
    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fhir].[Practitioner]')
          AND name = N'active'
          AND system_type_id = TYPE_ID(N'nvarchar')
    )
    BEGIN
        UPDATE [fhir].[Practitioner]
        SET [active] = CASE WHEN [active] = 'true' THEN '1'
                            WHEN [active] = 'false' THEN '0' END;
        ALTER TABLE [fhir].[Practitioner] ALTER COLUMN [active] BIT NULL;
    END
END
GO

IF EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]') AND type = 'U'
)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]') AND name = N'role_statuskode')
        ALTER TABLE [fhir].[PractitionerRole] ADD [role_statuskode] NVARCHAR(1) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]') AND name = N'role_jobstatuskode')
        ALTER TABLE [fhir].[PractitionerRole] ADD [role_jobstatuskode] NVARCHAR(1) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]') AND name = N'role_statusdato')
        ALTER TABLE [fhir].[PractitionerRole] ADD [role_statusdato] DATE NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]') AND name = N'role_tjenestekode')
        ALTER TABLE [fhir].[PractitionerRole] ADD [role_tjenestekode] NVARCHAR(1) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]') AND name = N'role_omraadekode')
        ALTER TABLE [fhir].[PractitionerRole] ADD [role_omraadekode] NVARCHAR(1) NULL;

    -- See fhir.Practitioner above.
    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fhir].[PractitionerRole]')
          AND name = N'active'
          AND system_type_id = TYPE_ID(N'nvarchar')
    )
    BEGIN
        UPDATE [fhir].[PractitionerRole]
        SET [active] = CASE WHEN [active] = 'true' THEN '1'
                            WHEN [active] = 'false' THEN '0' END;
        ALTER TABLE [fhir].[PractitionerRole] ALTER COLUMN [active] BIT NULL;
    END
END
GO
