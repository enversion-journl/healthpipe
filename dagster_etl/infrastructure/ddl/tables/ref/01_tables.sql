-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Reference Tables: ref.Patient_Borger, ref.Patient_Address_Borger
--
-- The `ref` schema bridges between FHIR-native string IDs (patient_id) and
-- the INT surrogate keys used by dim.Borger and downstream fact tables.
--
--   * ref.Patient_Borger        — entity-mapping (DW_EK_Borger ↔ patient_id)
--   * ref.Patient_Address_Borger — version-mapping (DW_SK_Borger ↔ address period),
--                                  with non-overlapping DW_ValidFrom/DW_ValidTo
--                                  derived from LEAD(address_period_start).
--   * ref.Faktura_To_Postering   — fct.Faktura ↔ fct.Postering bridge.
--   * ref.Organisation           — entity-mapping (DW_EK_Organisation ↔
--                                  organization_id), replaces map.Organisation.
--
-- See data_lineage/dim/dim_borger.yml notes for the full architecture.
-- ============================================================================

-- ============================================================================
-- ref.Patient_Borger — entity-mapping
--
-- One row per patient. DW_EK_Borger is the INT IDENTITY surrogate used by
-- dim.Borger.DW_EK_Borger. patient_id is the FHIR resource id and is also
-- carried so downstream joins to fhir.* tables don't need a separate lookup.
--
-- Identifier columns (cpr, x_ecpr, d_ecpr) are denormalized copies of the
-- corresponding fhir.Patient identifier slices — at least one must be set
-- per patient (enforced by CHECK constraint). Soft-deleted rows
-- (DW_IsDeleted = 1) are exempt from the constraint so cascade-deletes
-- from FHIR don't have to clear identifier values to drop the row.
--
-- PK is composite (DW_EK_Borger, patient_id) so the DW_EK-first lookup
-- pattern used by fact-table loaders works without an extra index.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Patient_Borger]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[Patient_Borger] (
        [DW_EK_Borger] INT IDENTITY(1,1) NOT NULL,
        [patient_id] NVARCHAR(255) NOT NULL,
        [cpr] VARCHAR(10) NULL,
        [x_ecpr] NVARCHAR(50) NULL,
        [d_ecpr] NVARCHAR(50) NULL,
        [DW_IsDeleted] BIT NOT NULL CONSTRAINT [DF_Ref_Patient_Borger_DW_IsDeleted] DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
        CONSTRAINT [PK_Ref_Patient_Borger] PRIMARY KEY CLUSTERED ([DW_EK_Borger], [patient_id]),
        CONSTRAINT [UQ_Ref_Patient_Borger_PatientId] UNIQUE ([patient_id]),
        CONSTRAINT [CK_Ref_Patient_Borger_HasIdentifier] CHECK (
            [DW_IsDeleted] = 1
            OR [cpr] IS NOT NULL
            OR [x_ecpr] IS NOT NULL
            OR [d_ecpr] IS NOT NULL
        )
    ) ON [PRIMARY];
END
GO

-- ============================================================================
-- ref.Patient_Address_Borger — version-mapping with non-overlap intervals
--
-- One row per (patient × address-period). DW_SK_Borger is the INT IDENTITY
-- surrogate that carries the T2 versioning of dim.Borger.
--
-- DW_ValidFrom / DW_ValidTo are computed from address_period_start via
-- LEAD(): the next row's address_period_start becomes the previous row's
-- DW_ValidTo (half-open interval [DW_ValidFrom, DW_ValidTo)).
-- address_period_end does NOT drive DW_ValidTo — this guarantees
-- non-overlapping intervals even when source data has overlapping
-- period_end values. See data_lineage/fhir/fhir_patient_address.yml.
--
-- Sentinels: first version per patient gets DW_ValidFrom = '1900-01-01';
-- last (current) version gets DW_ValidTo = '9999-12-31 23:59:59'.
--
-- IFDB-only patients (no recorded address.period.start) get a row with
-- address_period_start = '1900-01-01' as a default; this row is replaced
-- by real periods when Datafordeleren delivers historik.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_Borger]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[Patient_Address_Borger] (
        [DW_SK_Borger] INT IDENTITY(1,1) NOT NULL,
        [patient_id] NVARCHAR(255) NOT NULL,
        [cpr] VARCHAR(10) NULL,
        [x_ecpr] NVARCHAR(50) NULL,
        [d_ecpr] NVARCHAR(50) NULL,
        [address_period_start] DATETIME2(0) NOT NULL CONSTRAINT [DF_Ref_Patient_Address_Borger_PeriodStart] DEFAULT '1900-01-01',
        [address_period_end] DATETIME2(0) NULL,
        [DW_ValidFrom] DATETIME2(0) NOT NULL,
        [DW_ValidTo] DATETIME2(0) NOT NULL,
        [DW_IsCurrent] BIT NOT NULL CONSTRAINT [DF_Ref_Patient_Address_Borger_DW_IsCurrent] DEFAULT 1,
        [DW_IsDeleted] BIT NOT NULL CONSTRAINT [DF_Ref_Patient_Address_Borger_DW_IsDeleted] DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
        CONSTRAINT [PK_Ref_Patient_Address_Borger] PRIMARY KEY CLUSTERED ([DW_SK_Borger], [patient_id]),
        CONSTRAINT [CK_Ref_Patient_Address_Borger_HasIdentifier] CHECK (
            [DW_IsDeleted] = 1
            OR [cpr] IS NOT NULL
            OR [x_ecpr] IS NOT NULL
            OR [d_ecpr] IS NOT NULL
        )
    ) ON [PRIMARY];
END
GO

-- Temporal lookup index — matches IX_Dim_Borger_Temporal pattern so fact
-- loaders can resolve DW_SK_Borger via (patient_id, event_date >= DW_ValidFrom
-- AND event_date < DW_ValidTo) without joining to dim.Borger.
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_Borger]') AND name = N'IX_Ref_Patient_Address_Borger_Temporal')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_Patient_Address_Borger_Temporal]
    ON [ref].[Patient_Address_Borger] (
        [patient_id] ASC,
        [DW_ValidFrom] ASC,
        [DW_ValidTo] ASC
    )
    INCLUDE ([DW_SK_Borger])
    ON [PRIMARY];
END
GO

-- MERGE key uniqueness — required for MERGE on (patient_id, address_period_start).
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_Borger]') AND name = N'UX_Ref_Patient_Address_Borger_PatientPeriod')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_Ref_Patient_Address_Borger_PatientPeriod]
    ON [ref].[Patient_Address_Borger] (
        [patient_id] ASC,
        [address_period_start] ASC
    )
    ON [PRIMARY];
END
GO

-- ============================================================================
-- ref.Faktura_To_Postering — fct.Faktura ↔ fct.Postering bridge
--
-- Full crosswalk of matched (DW_EK_Faktura, DW_EK_Postering) pairs with
-- "newest" flags for bidirectional FK resolution. Matched on conformed
-- (DW_EK_Kreditor, FakturaNr). Soft-delete model (DW_IsDeleted).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Faktura_To_Postering]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[Faktura_To_Postering] (
        [DW_EK_Faktura] INT NOT NULL,
        [DW_EK_Postering] INT NOT NULL,
        [DW_EK_Kreditor] INT NULL,
        [FakturaNr] NVARCHAR(100) NULL,
        [is_newest_postering_for_faktura] BIT NOT NULL DEFAULT 0,
        [is_newest_faktura_for_postering] BIT NOT NULL DEFAULT 0,
        [DW_IsDeleted] BIT NOT NULL CONSTRAINT [DF_Ref_FakturaToPostering_DW_IsDeleted] DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
        CONSTRAINT [PK_Ref_Faktura_To_Postering] PRIMARY KEY CLUSTERED ([DW_EK_Faktura] ASC, [DW_EK_Postering] ASC)
    ) ON [PRIMARY];
END
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Faktura_To_Postering]') AND name = 'IX_Ref_FakturaToPostering_Postering')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Faktura_To_Postering]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_FakturaToPostering_Postering]
    ON [ref].[Faktura_To_Postering] ([DW_EK_Postering] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- ref.Organisation — entity-mapping (DW_EK_Organisation ↔ organization_id)
--
-- One row per SOR organisation. DW_EK_Organisation is the INT IDENTITY
-- surrogate consumed by dim.Organisation.DW_EK_Organisation and every fact /
-- dim that FKs to it (sec.BrugerOrganisation, dim.Lager, dim.Rekvirent, ...).
-- organization_id is the fhir.Organization resource id and is carried so
-- downstream joins to fhir.* don't need a separate lookup.
--
-- sor_code is a denormalized copy of the fhir.Organization SOR identifier
-- slice (identifier_value_sor) — it is the SOR business key and must be set
-- on every live row (enforced by CHECK). Soft-deleted rows (DW_IsDeleted = 1)
-- are exempt so an org leaving SOR can be dropped without clearing sor_code,
-- preserving the DW_EK for historical fact FKs.
--
-- This table REPLACES map.Organisation as the source of DW_EK_Organisation.
-- Existing keys are migrated 1:1 (IDENTITY_INSERT) by the seeding migration
-- 20260618_ref_organisation.sql so downstream FKs stay valid.
--
-- PK is composite (DW_EK_Organisation, organization_id) so the DW_EK-first
-- lookup used by fact-table loaders works without an extra index. There is no
-- temporal lookup TVF — dim.Organisation is Type 1 (one current row per org).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[Organisation] (
        [DW_EK_Organisation] INT IDENTITY(1,1) NOT NULL,
        [organization_id] NVARCHAR(255) NOT NULL,
        [sor_code] VARCHAR(50) NULL,
        [DW_IsDeleted] BIT NOT NULL CONSTRAINT [DF_Ref_Organisation_DW_IsDeleted] DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
        CONSTRAINT [PK_Ref_Organisation] PRIMARY KEY CLUSTERED ([DW_EK_Organisation], [organization_id]),
        CONSTRAINT [UQ_Ref_Organisation_OrganizationId] UNIQUE ([organization_id]),
        CONSTRAINT [CK_Ref_Organisation_HasIdentifier] CHECK (
            [DW_IsDeleted] = 1
            OR [sor_code] IS NOT NULL
        )
    ) ON [PRIMARY];
END
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND name = 'IX_Ref_Organisation_SorCode')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_Organisation_SorCode]
    ON [ref].[Organisation] ([sor_code] ASC) ON [PRIMARY];
END
GO
