-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Reference Tables: ref.PatientToBorger, ref.Patient_Address_To_Borger
--
-- The `ref` schema bridges between FHIR-native string IDs (patient_id) and
-- the INT surrogate keys used by dim.Borger and downstream fact tables.
--
--   * ref.PatientToBorger        — entity-mapping (dw_ek_Borger ↔ patient_id)
--   * ref.Patient_Address_To_Borger — version-mapping (dw_sk_Borger ↔ address period),
--                                  with non-overlapping dw_valid_from/dw_valid_to
--                                  derived from LEAD(address_period_start).
--   * ref.FakturaToPostering   — fct.Faktura ↔ fct.Postering bridge.
--   * ref.Organisation           — entity-mapping (dw_ek_Organisation ↔
--                                  organization_id), replaces map.Organisation.
--   * ref.RekvirentUndtagelse    — ILS rekvUndtagne exclusion list for reelt
--                                  forbrug (rå CleanCode PK).
--   * ref.ILSAendringslog        — ILS dbo.changelog as-is (one row per
--                                  change event, raw key without FK).
--
-- See data_lineage/dim/dim_borger.yml notes for the full architecture.
-- ============================================================================

-- ============================================================================
-- ref.PatientToBorger — entity-mapping
--
-- One row per patient. dw_ek_Borger is the INT IDENTITY surrogate used by
-- dim.Borger.dw_ek_Borger. patient_id is the FHIR resource id and is also
-- carried so downstream joins to fhir.* tables don't need a separate lookup.
--
-- Identifier columns (cpr, x_ecpr, d_ecpr) are denormalized copies of the
-- corresponding fhir.Patient identifier slices — at least one must be set
-- per patient (enforced by CHECK constraint). Soft-deleted rows
-- (dw_is_deleted = 1) are exempt from the constraint so cascade-deletes
-- from FHIR don't have to clear identifier values to drop the row.
--
-- PK is composite (dw_ek_Borger, patient_id) so the DW_EK-first lookup
-- pattern used by fact-table loaders works without an extra index.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[PatientToBorger]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[PatientToBorger] (
        [dw_ek_Borger] INT IDENTITY(1,1) NOT NULL,
        [patient_id] NVARCHAR(255) NOT NULL,
        [cpr] VARCHAR(10) NULL,
        [x_ecpr] NVARCHAR(50) NULL,
        [d_ecpr] NVARCHAR(50) NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_PatientToBorger_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_PatientToBorger_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_PatientToBorger_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_PatientToBorger] PRIMARY KEY CLUSTERED ([dw_ek_Borger], [patient_id]),
        CONSTRAINT [UQ_Ref_PatientToBorger_PatientId] UNIQUE ([patient_id]),
        CONSTRAINT [CK_Ref_Patient_Borger_HasIdentifier] CHECK (
            [dw_is_deleted] = 1
            OR [cpr] IS NOT NULL
            OR [x_ecpr] IS NOT NULL
            OR [d_ecpr] IS NOT NULL
        )
    ) ON [PRIMARY];
END
GO

-- ============================================================================
-- ref.Patient_Address_To_Borger — version-mapping with non-overlap intervals
--
-- One row per (patient × address-period). dw_sk_Borger is the INT IDENTITY
-- surrogate that carries the T2 versioning of dim.Borger.
--
-- dw_valid_from / dw_valid_to are computed from address_period_start via
-- LEAD(): the next row's address_period_start becomes the previous row's
-- dw_valid_to (half-open interval [dw_valid_from, dw_valid_to)).
-- address_period_end does NOT drive dw_valid_to — this guarantees
-- non-overlapping intervals even when source data has overlapping
-- period_end values. See data_lineage/fhir/fhir_patient_address.yml.
--
-- Sentinels: first version per patient gets dw_valid_from = '1900-01-01';
-- last (current) version gets dw_valid_to = '9999-12-31 23:59:59'.
--
-- IFDB-only patients (no recorded address.period.start) get a row with
-- address_period_start = '1900-01-01' as a default; this row is replaced
-- by real periods when Datafordeleren delivers historik.
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[Patient_Address_To_Borger] (
        [dw_sk_Borger] INT IDENTITY(1,1) NOT NULL,
        [patient_id] NVARCHAR(255) NOT NULL,
        [cpr] VARCHAR(10) NULL,
        [x_ecpr] NVARCHAR(50) NULL,
        [d_ecpr] NVARCHAR(50) NULL,
        [address_period_start] DATETIME2(0) NOT NULL CONSTRAINT [DF_Patient_Address_To_Borger_address_period_start] DEFAULT '1900-01-01',
        [address_period_end] DATETIME2(0) NULL,
        [dw_valid_from] DATETIME2(0) NOT NULL,
        [dw_valid_to] DATETIME2(0) NOT NULL,
        [dw_is_current] BIT NOT NULL CONSTRAINT [DF_Patient_Address_To_Borger_dw_is_current] DEFAULT 1,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_Patient_Address_To_Borger_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_Patient_Address_To_Borger_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_Patient_Address_To_Borger_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_Patient_Address_To_Borger] PRIMARY KEY CLUSTERED ([dw_sk_Borger], [patient_id]),
        CONSTRAINT [CK_Ref_Patient_Address_Borger_HasIdentifier] CHECK (
            [dw_is_deleted] = 1
            OR [cpr] IS NOT NULL
            OR [x_ecpr] IS NOT NULL
            OR [d_ecpr] IS NOT NULL
        ),
        CONSTRAINT [CK_Ref_Patient_Address_Borger_CurrentNotDeleted] CHECK (
            [dw_is_deleted] = 0 OR [dw_is_current] = 0
        )
    ) ON [PRIMARY];
END
GO

-- Temporal lookup index — matches IX_Dim_Borger_Temporal pattern so fact
-- loaders can resolve dw_sk_Borger via (patient_id, event_date >= dw_valid_from
-- AND event_date < dw_valid_to) without joining to dim.Borger.
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND name = N'IX_Ref_Patient_Address_To_Borger_Temporal')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_Patient_Address_To_Borger_Temporal]
    ON [ref].[Patient_Address_To_Borger] (
        [patient_id] ASC,
        [dw_valid_from] ASC,
        [dw_valid_to] ASC
    )
    INCLUDE ([dw_sk_Borger])
    ON [PRIMARY];
END
GO

-- MERGE key uniqueness — required for MERGE on (patient_id, address_period_start).
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND name = N'UX_Ref_Patient_Address_To_Borger_PatientPeriod')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_Ref_Patient_Address_To_Borger_PatientPeriod]
    ON [ref].[Patient_Address_To_Borger] (
        [patient_id] ASC,
        [address_period_start] ASC
    )
    ON [PRIMARY];
END
GO

-- At most one current version per patient. Skip while duplicates remain;
-- re-run infra after a rebuild. Existing tables are patched by
-- 20260813_borger_one_current_version.sql.
IF OBJECT_ID(N'[ref].[Patient_Address_To_Borger]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
         AND name = N'UX_Ref_Patient_Address_Borger_CurrentPatient'
   )
   AND NOT EXISTS (
       SELECT 1
       FROM [ref].[Patient_Address_To_Borger]
       WHERE [dw_is_current] = 1
       GROUP BY [patient_id]
       HAVING COUNT(*) > 1
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_Ref_Patient_Address_Borger_CurrentPatient]
    ON [ref].[Patient_Address_To_Borger] ([patient_id] ASC)
    WHERE [dw_is_current] = 1
    ON [PRIMARY];
END
GO

-- ============================================================================
-- ref.FakturaToPostering — fct.Faktura ↔ fct.Postering bridge
--
-- Full crosswalk of matched (dw_ek_Faktura, dw_ek_Postering) pairs with
-- "newest" flags for bidirectional FK resolution. Matched on conformed
-- (dw_ek_Kreditor, faktura_nr). Soft-delete model (dw_is_deleted).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[FakturaToPostering] (
        [dw_ek_Faktura] INT NOT NULL,
        [dw_ek_Postering] INT NOT NULL,
        [dw_ek_Kreditor] INT NOT NULL CONSTRAINT [DF_FakturaToPostering_dw_ek_Kreditor] DEFAULT -1,
        [faktura_nr] NVARCHAR(100) NULL,
        [is_newest_postering_for_faktura] BIT NOT NULL CONSTRAINT [DF_FakturaToPostering_is_newest_postering_for_faktura] DEFAULT 0,
        [is_newest_faktura_for_postering] BIT NOT NULL CONSTRAINT [DF_FakturaToPostering_is_newest_faktura_for_postering] DEFAULT 0,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_FakturaToPostering_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_FakturaToPostering_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_FakturaToPostering_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_FakturaToPostering] PRIMARY KEY CLUSTERED ([dw_ek_Faktura] ASC, [dw_ek_Postering] ASC)
    ) ON [PRIMARY];
END
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND name = 'IX_Ref_FakturaToPostering_Postering')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_FakturaToPostering_Postering]
    ON [ref].[FakturaToPostering] ([dw_ek_Postering] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- ref.KMDFakturalinjeUNSPSCMatch — KMD → MySupply UNSPSC tiered lookup
--
-- Pre-computed match keys for fct.Fakturalinje UNSPSC apply-pass. Soft-delete
-- model (dw_is_deleted). Populated by ref_kmd_fakturalinje_unspsc_match.
-- ============================================================================
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[KMDFakturalinjeUNSPSCMatch] (
        [dw_sk_KMDFakturalinjeUNSPSCMatch] INT IDENTITY(1, 1) NOT NULL,
        [clean_code_kreditor] NVARCHAR(200)  NOT NULL,
        [clean_code_faktura]  NVARCHAR(500)  NOT NULL,
        [match_tier]          TINYINT        NOT NULL,
        [line_amount_sum]     DECIMAL(18, 4) NULL,
        [invoiced_quantity]   DECIMAL(18, 4) NULL,
        [unit_price]          DECIMAL(18, 4) NULL,
        [invoice_total]       DECIMAL(18, 4) NULL,
        [dw_ek_UNSPSC]       INT            NOT NULL,
        [dw_is_deleted]       BIT            NOT NULL
            CONSTRAINT [DF_KMDFakturalinjeUNSPSCMatch_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL
            CONSTRAINT [DF_KMDFakturalinjeUNSPSCMatch_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_KMDFakturalinjeUNSPSCMatch_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Ref_KmdFakturalinjeUnspscMatch]
            PRIMARY KEY CLUSTERED ([dw_sk_KMDFakturalinjeUNSPSCMatch] ASC)
    ) ON [PRIMARY];
END
GO
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
      AND name = 'UX_ref_KMDFakturalinjeUNSPSCMatch_keys'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND type = 'U'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_ref_KMDFakturalinjeUNSPSCMatch_keys]
        ON [ref].[KMDFakturalinjeUNSPSCMatch] (
            [clean_code_kreditor],
            [match_tier],
            [clean_code_faktura],
            [line_amount_sum],
            [invoiced_quantity],
            [unit_price],
            [invoice_total]
        )
        WHERE [dw_is_deleted] = 0;
END
GO
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
      AND name = 'IX_ref_KMDFakturalinjeUNSPSCMatch_lookup'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_lookup]
        ON [ref].[KMDFakturalinjeUNSPSCMatch] (
            [match_tier],
            [clean_code_kreditor],
            [clean_code_faktura],
            [line_amount_sum],
            [invoiced_quantity],
            [unit_price],
            [invoice_total]
        )
        WHERE [dw_is_deleted] = 0;
END
GO
-- Delta-indeks for fct_apply_fakturalinje_unspsc's #RefChangedKeys-udtræk.
-- BEVIDST UFILTRERET: apply-passet skal se soft-deletede rækker, for det er dem
-- der får en linje til at falde tilbage til -1 (der er ikke længere et separat
-- demote-pass). Et `WHERE dw_is_deleted = 0`-filter kan derfor ikke bruges af den
-- forespørgsel, og med kun filtrerede indeks på tabellen faldt planen tilbage på
-- en fuld clustered scan ved hver kørsel.
-- De to clean_code-kolonner ligger i INCLUDE, ikke i nøglen: forespørgslen
-- projicerer dem kun, og NVARCHAR(200)+NVARCHAR(500) ville lægge 1400 bytes i en
-- nøgle med en grænse på 1700.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
      AND name = 'IX_ref_KMDFakturalinjeUNSPSCMatch_delta'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_delta]
        ON [ref].[KMDFakturalinjeUNSPSCMatch] ([dw_timestamp_update])
        INCLUDE ([clean_code_kreditor], [clean_code_faktura]);
END
GO

-- ============================================================================
-- ref.FakturalinjeMatchKeys — komplet match-nøgle-tabel pr. fakturalinje
--
-- Persisterer de nøgler fct_apply_fakturalinje_unspsc skal bruge for ALLE
-- fakturalinjer (grain dw_ek_Fakturalinje, delt PK med fct.Fakturalinje).
-- Gør apply uafhængig af delta-staging, så sent-ankommet KMD-ref kan berige
-- gamle linjer. Vedligeholdes af ref_fakturalinje_matchkeys.
-- ============================================================================
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[FakturalinjeMatchKeys] (
        [dw_ek_Fakturalinje]         INT            NOT NULL,
        [clean_code_Kreditor]        NVARCHAR(200)  NULL,
        [clean_code_Kreditor_header] NVARCHAR(200)  NULL,
        [clean_code_Faktura]         NVARCHAR(500)  NULL,
        [linje_beloeb]               DECIMAL(18, 4) NULL,
        [enheds_pris]                DECIMAL(18, 4) NULL,
        [antal_enheder_faktureret]   DECIMAL(18, 4) NULL,
        [faktura_total]              DECIMAL(18, 4) NULL,
        [dw_ek_Dato_faktura]         INT            NOT NULL
            CONSTRAINT [DF_FakturalinjeMatchKeys_dw_ek_Dato_faktura] DEFAULT -1,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL
            CONSTRAINT [DF_FakturalinjeMatchKeys_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_FakturalinjeMatchKeys_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_Ref_FakturalinjeMatchKeys]
            PRIMARY KEY CLUSTERED ([dw_ek_Fakturalinje] ASC)
    ) ON [PRIMARY];
END
GO
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
      AND name = 'IX_ref_fakturalinje_matchkeys_primary'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_fakturalinje_matchkeys_primary]
        ON [ref].[FakturalinjeMatchKeys] ([clean_code_Kreditor], [clean_code_Faktura])
        INCLUDE ([linje_beloeb], [enheds_pris], [antal_enheder_faktureret], [faktura_total]);
END
GO
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
      AND name = 'IX_ref_fakturalinje_matchkeys_header'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_fakturalinje_matchkeys_header]
        ON [ref].[FakturalinjeMatchKeys] ([clean_code_Kreditor_header], [clean_code_Faktura])
        INCLUDE ([linje_beloeb], [enheds_pris], [antal_enheder_faktureret], [faktura_total]);
END
GO
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
      AND name = 'IX_ref_fakturalinje_matchkeys_timestamp'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_fakturalinje_matchkeys_timestamp]
        ON [ref].[FakturalinjeMatchKeys] ([dw_timestamp_update])
        INCLUDE ([dw_ek_Dato_faktura]);
END
GO

-- ============================================================================
-- ref.Organisation — entity-mapping (dw_ek_Organisation ↔ organization_id)
--
-- One row per SOR organisation. dw_ek_Organisation is the INT IDENTITY
-- surrogate consumed by dim.Organisation.dw_ek_Organisation and every fact /
-- dim that FKs to it (dim.Lager, dim.Rekvirent, ...).
-- organization_id is the fhir.Organization resource id and is carried so
-- downstream joins to fhir.* don't need a separate lookup.
--
-- sor_code is a denormalized copy of the fhir.Organization SOR identifier
-- slice (identifier_value_sor) — it is the SOR business key and must be set
-- on every live row (enforced by CHECK). Soft-deleted rows (dw_is_deleted = 1)
-- are exempt so an org leaving SOR can be dropped without clearing sor_code,
-- preserving the DW_EK for historical fact FKs.
--
-- This table REPLACES map.Organisation as the source of dw_ek_Organisation.
-- Existing keys are migrated 1:1 (IDENTITY_INSERT) by the seeding migration
-- 20260618_ref_organisation.sql so downstream FKs stay valid.
--
-- PK is composite (dw_ek_Organisation, organization_id) so the DW_EK-first
-- lookup used by fact-table loaders works without an extra index. There is no
-- temporal lookup TVF — dim.Organisation is Type 1 (one current row per org).
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[Organisation] (
        [dw_ek_Organisation] INT IDENTITY(1,1) NOT NULL,
        [organization_id] NVARCHAR(255) NOT NULL,
        [sor_code] VARCHAR(50) NULL,
        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_Organisation_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_Organisation_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_Organisation_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_Organisation] PRIMARY KEY CLUSTERED ([dw_ek_Organisation], [organization_id]),
        CONSTRAINT [UQ_Ref_Organisation_OrganizationId] UNIQUE ([organization_id]),
        CONSTRAINT [CK_Ref_Organisation_HasIdentifier] CHECK (
            [dw_is_deleted] = 1
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
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND name = 'UQ_Ref_Organisation_SorCode_Live')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND type = 'U')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_Ref_Organisation_SorCode_Live]
    ON [ref].[Organisation] ([sor_code])
    WHERE [dw_is_deleted] = 0 AND [sor_code] IS NOT NULL;
END
GO

-- ============================================================================
-- ref.RekvirentUndtagelse — ILS exclusion list for "reelt forbrug"
--
-- Source: arc.ils_rekvundtagne LEFT JOIN arc.ils_rekv (navn) + map.Rekvirent
-- (dw_ek when present). Orphans stay on the list with dw_ek_Rekvirent NULL.
--
-- PK is clean_code_Rekvirent (SGH_{sgh}_RNR_{rnr} with a raw rnr), matching
-- stg_dim_Rekvirent / DSA/SQL/common/ils_clean_code_shared — never padded,
-- never RIGHT(...,4)-truncated.
-- ============================================================================
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[RekvirentUndtagelse] (
        [clean_code_Rekvirent] VARCHAR(100)  NOT NULL,
        [sgh]                  VARCHAR(8)    NOT NULL,
        [rnr]                  VARCHAR(16)   NOT NULL,
        [navn]                 VARCHAR(80)   NULL,
        [navn_manuelt]         VARCHAR(80)   NULL,
        [dw_ek_Rekvirent]      INT           NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_RekvirentUndtagelse_dw_timestamp_insert]
            DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_RekvirentUndtagelse_dw_timestamp_update]
            DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_RekvirentUndtagelse]
            PRIMARY KEY CLUSTERED ([clean_code_Rekvirent] ASC),
        CONSTRAINT [UQ_Ref_RekvirentUndtagelse_SghRnr]
            UNIQUE ([sgh], [rnr])
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]')
      AND name = 'IX_Ref_RekvirentUndtagelse_DW_EK'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_RekvirentUndtagelse_DW_EK]
        ON [ref].[RekvirentUndtagelse] ([dw_ek_Rekvirent] ASC)
        WHERE [dw_ek_Rekvirent] IS NOT NULL;
END
GO

-- ============================================================================
-- ref.PatientIdentity — which identities belong to the same citizen
--
-- One row per identity (a CPR or an EPJ patient id) and the person it belongs
-- to, keyed by the current CPR, or the EPJ id for a person without one. Built by
-- ref_patient_identity from EPJ's archive rows and CPR record 022; identities
-- only, no demographics.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'ref' AND TABLE_NAME = 'PatientIdentity'
)
BEGIN
    CREATE TABLE [ref].[PatientIdentity] (
        [identity_system]      NVARCHAR(128) NOT NULL,
        [identity_value]       NVARCHAR(64)  NOT NULL,
        -- The current CPR, or 'epj:<patient-id>' for a person without one.
        [person_key]           NVARCHAR(70)  NOT NULL,
        -- 1 for the current CPR and the primary patient id; 0 otherwise.
        [is_current]           BIT           NOT NULL,
        -- 'epj' (archive row) or 'cpr_022'.
        [source]               NVARCHAR(16)  NOT NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_PatientIdentity_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_PatientIdentity_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_PatientIdentity] PRIMARY KEY CLUSTERED ([identity_system], [identity_value])
    ) ON [PRIMARY];
    -- Lookups run both ways: identity to person and person to identities.
    CREATE NONCLUSTERED INDEX [IX_Ref_PatientIdentity_person]
        ON [ref].[PatientIdentity] ([person_key])
        INCLUDE ([identity_system], [identity_value], [is_current]);
END
GO

-- ============================================================================
-- ref.KommuneToPopulationsansvar — the hospital with population responsibility
-- per municipality
--
-- A hospital is responsible for the citizens living in the municipalities that
-- belong to it. kommunekode is the four-digit municipality code. The rows are a
-- full snapshot of the mapping in ref_KommuneToPopulationsansvar; dim_Borger
-- joins the citizen's current municipality to this table.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[KommuneToPopulationsansvar]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[KommuneToPopulationsansvar] (
        [kommunekode]          VARCHAR(4)    NOT NULL,
        [populationsansvar]    NVARCHAR(50)  NOT NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_KommuneToPopulationsansvar_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL CONSTRAINT [DF_KommuneToPopulationsansvar_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_KommuneToPopulationsansvar] PRIMARY KEY CLUSTERED ([kommunekode] ASC)
    ) ON [PRIMARY];
END
GO

-- ============================================================================
-- ref.ILSAendringslog — ILS change log (dbo.changelog) as-is
--
-- Source: arc.ils_changelog. One table for every tabelnavn, not split per
-- tabelnavn, so tables added to the source log flow through on their own.
-- noegle is the raw primaernoegle without FK resolution: its meaning depends
-- on tabelnavn (some combine it with feltnavn/feltlabel). Resolution against
-- dimensions (e.g. vare -> dim.ILSVare) belongs in views on top.
--
-- tidspunkt is the source klok (seconds after midnight) as 'hh:mm:ss'.
-- ============================================================================
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[ILSAendringslog] (
        [prrowid]              VARCHAR(36)   NOT NULL,
        [tabelnavn]            VARCHAR(40)   NULL,
        [noegle]               VARCHAR(30)   NULL,
        [feltnavn]             VARCHAR(30)   NULL,
        [feltlabel]            VARCHAR(60)   NULL,
        [fra]                  VARCHAR(40)   NULL,
        [til]                  VARCHAR(30)   NULL,
        [opret]                BIT           NULL,
        [ajourfoert_af]        VARCHAR(6)    NULL,
        [dato]                 DATE          NULL,
        [tidspunkt]            CHAR(8)       NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_ILSAendringslog_dw_timestamp_insert]
            DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_ILSAendringslog_dw_timestamp_update]
            DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_ILSAendringslog]
            PRIMARY KEY CLUSTERED ([prrowid] ASC)
    ) ON [PRIMARY];
END
GO

-- Watermark for the DSO sync (mode='watermark' on dw_timestamp_update).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]')
      AND name = 'IX_Ref_ILSAendringslog_DW_Timestamp_Update'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_ILSAendringslog_DW_Timestamp_Update]
        ON [ref].[ILSAendringslog] ([dw_timestamp_update] DESC);
END
GO

-- Point-in-time lookups filter on tabelnavn + noegle (e.g. a vare's basisenhed).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]')
      AND name = 'IX_Ref_ILSAendringslog_tabelnavn_noegle'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_ILSAendringslog_tabelnavn_noegle]
        ON [ref].[ILSAendringslog] ([tabelnavn] ASC, [noegle] ASC, [dato] ASC, [tidspunkt] ASC);
END
GO
