-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Shared Dimensions: Dimension Tables
-- dim.Borger (Type 2 SCD), dim.Dato (Type 1 — Date Dimension)
-- ============================================================================

-- ============================================================================
-- dim.Borger (Type 2 SCD with temporal validity)
--
-- One row per (dw_ek_Borger × address-period). T2 scope is intentionally
-- narrow: only address fields generate new versions; all other attributes
-- are Type 1 and overwritten on update.
--
-- Keys (INT surrogates assigned in ref tables — see data_lineage/dim/dim_borger.yml):
--   * dw_sk_Borger — INT, version-PK (carried from ref.Patient_Address_To_Borger; no local IDENTITY)
--   * dw_ek_Borger — INT, entity-key (assigned by ref.PatientToBorger; stable across T2 versions)
--
-- Identifier columns (cpr_nummer, x_ecpr_nummer, d_ecpr_nummer) — at least
-- one must be set per borger; enforced upstream by ref.PatientToBorger.
--
-- Sentinel rows:
--   * dw_sk_Borger = -1, dw_ek_Borger = -1  (UNKNOWN)
--   * dw_sk_Borger = -2, dw_ek_Borger = -2  (N/A — used for facts where
--                                            borger-relation explicitly does not apply)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Borger](
        -- Technical keys
        [dw_sk_Borger]                  INT NOT NULL,
        [dw_ek_Borger]                  INT NOT NULL,

        -- Identifiers (Type 1)
        [cpr_nummer]                     VARCHAR(10) NULL,
        [tidligere_cpr]                  VARCHAR(10) NULL,
        [x_ecpr_nummer]                 NVARCHAR(50) NULL,
        [d_ecpr_nummer]                 NVARCHAR(50) NULL,
        [patient_id_ifdb]                    BIGINT NULL,

        -- Name (Type 1 — name:official slice fra dk-core)
        [fornavne]                      NVARCHAR(500) NULL,
        [efternavn]                     NVARCHAR(255) NULL,
        [adresseringsnavn]              NVARCHAR(500) NULL,

        -- Demographics (Type 1)
        [foedselsdato]                  DATE NULL,
        [koen_navn]                      NVARCHAR(20) NULL,
        [active]                        BIT NULL,
        [deceased]                      BIT NULL,

        -- Family relations (Type 1)
        [mor_cpr_nummer]                  VARCHAR(10) NULL,
        [far_cpr_nummer]                  VARCHAR(10) NULL,

        -- Marital status (Type 1)
        [civil_status_kode]               NVARCHAR(8) NULL,
        [civil_status]                   NVARCHAR(100) NULL,

        -- Citizenship (Type 1)
        [statsborgerskab_kode]           NVARCHAR(8) NULL,
        [statsborgerskab]               NVARCHAR(255) NULL,

        -- Address (T2 — one row per period from ref.Patient_Address_To_Borger)
        [dato_tid_tilflytning]           DATETIME2 NULL,
        [dato_tid_fraflytning]           DATETIME2 NULL,
        [gade]                          NVARCHAR(500) NULL,
        [postnr]                        VARCHAR(10) NULL,
        [postdistrikt]                  NVARCHAR(255) NULL,
        [kommune_id]                     VARCHAR(8) NULL,
        [kommunenavn]                   NVARCHAR(255) NULL,
        [region_id]                      VARCHAR(8) NULL,
        [regionnavn]                    NVARCHAR(255) NULL,
        [land]                          NVARCHAR(8) NULL,
        [kvhx]                          VARCHAR(64) NULL,

        -- Current address denormalization (Type 1 copy of current address
        -- repeated on every T2 row so reports avoid double-join)
        [gade_nuv]                      NVARCHAR(500) NULL,
        [postnr_nuv]                    VARCHAR(10) NULL,
        [postdistrikt_nuv]              NVARCHAR(255) NULL,
        [kommune_id_nuv]                 VARCHAR(8) NULL,
        [kommunenavn_nuv]               NVARCHAR(255) NULL,
        [region_id_nuv]                  VARCHAR(8) NULL,
        [regionnavn_nuv]                NVARCHAR(255) NULL,

        -- General practitioner (Type 1)
        [laege_navn]                     NVARCHAR(500) NULL,
        [laege_nummer]                   VARCHAR(50) NULL,

        -- District (RSyd-local, Type 1)
        [person_distriktkode]           VARCHAR(20) NULL,
        [person_distriktkodetekst]      NVARCHAR(100) NULL,
        [person_socialdistriktkode]     VARCHAR(32) NULL,
        [person_socialdistriktkodetekst] NVARCHAR(100) NULL,

        -- Privacy / context flags (Type 1)
        [adressebeskyttelse]            BIT NOT NULL CONSTRAINT [DF_Borger_adressebeskyttelse] DEFAULT 0,
        [onsker_diskretion]              BIT NULL,
        [medcom_testperson]             BIT NULL,

        -- Audit / temporal
        [opdateret_fra_cpr]               DATETIME2 NULL,
        [dw_valid_from]                  DATETIME2(0) NOT NULL,
        [dw_valid_to]                    DATETIME2(0) NOT NULL,
        [dw_is_current]                  BIT NOT NULL CONSTRAINT [DF_Borger_dw_is_current] DEFAULT 1,
        [dw_is_deleted]                  BIT NOT NULL CONSTRAINT [DF_Borger_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert]           DATETIME2(0) NOT NULL CONSTRAINT [DF_Borger_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]           DATETIME2(0) NOT NULL CONSTRAINT [DF_Borger_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Dim_Borger] PRIMARY KEY CLUSTERED ([dw_sk_Borger] ASC),
        CONSTRAINT [CK_Dim_Borger_CurrentNotDeleted] CHECK (
            [dw_is_deleted] = 0 OR [dw_is_current] = 0
        )
    ) ON [PRIMARY];

    -- Insert sentinel rows. Identifier-systems live centrally in
    -- utils/fhir/identifier_systems.py; sentinel rows use literal markers
    -- that don't resolve to any real identifier system.

    -- -1 = UNKNOWN: fact rows where borger is unknown but should be linked
    INSERT INTO [dim].[Borger] (
        [dw_sk_Borger], [dw_ek_Borger], [cpr_nummer],
        [fornavne], [efternavn],
        [dw_valid_from], [dw_valid_to], [dw_is_current], [dw_is_deleted],
        [dw_timestamp_insert]
    ) VALUES (
        -1, -1, 'UNKNOWN',
        N'Ukendt', N'Borger',
        CAST('1900-01-01' AS datetime2(0)), CAST('9999-12-31 23:59:59' AS datetime2(0)), 1, 0,
        CAST(SYSDATETIME() AS datetime2(0))
    );

    -- -2 = N/A: fact rows where borger-relation explicitly does not apply
    INSERT INTO [dim].[Borger] (
        [dw_sk_Borger], [dw_ek_Borger], [cpr_nummer],
        [fornavne], [efternavn],
        [dw_valid_from], [dw_valid_to], [dw_is_current], [dw_is_deleted],
        [dw_timestamp_insert]
    ) VALUES (
        -2, -2, 'NA',
        N'Ikke', N'relevant',
        CAST('1900-01-01' AS datetime2(0)), CAST('9999-12-31 23:59:59' AS datetime2(0)), 1, 0,
        CAST(SYSDATETIME() AS datetime2(0))
    );
END
GO

-- Temporal lookup index for dim.Borger (used by GetBorgerSK_TVF)
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = N'IX_Dim_Borger_Temporal')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Dim_Borger_Temporal]
    ON [dim].[Borger] (
        [dw_ek_Borger] ASC,
        [dw_valid_from] ASC,
        [dw_valid_to] ASC
    )
    INCLUDE ([dw_sk_Borger])
    ON [PRIMARY];
END
GO

-- At most one current version per borger. Skip while duplicates remain;
-- re-run infra after a rebuild. Existing tables are patched by
-- 20260813_borger_one_current_version.sql.
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
         AND name = N'UX_Dim_Borger_CurrentEntity'
   )
   AND NOT EXISTS (
       SELECT 1
       FROM [dim].[Borger]
       WHERE [dw_is_current] = 1
       GROUP BY [dw_ek_Borger]
       HAVING COUNT(*) > 1
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_Dim_Borger_CurrentEntity]
    ON [dim].[Borger] ([dw_ek_Borger] ASC)
    WHERE [dw_is_current] = 1
    ON [PRIMARY];
END
GO

-- ============================================================================
-- dim.Dato (Type 1 — Date Dimension, smart key YYYYMMDD)
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Dato]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Dato](
        [dw_ek_Dato]              INT NOT NULL,
        [dato]                    DATE NULL,

        -- Year
        [aar]                     SMALLINT NULL,
        [aar_startdato]           DATE NULL,
        [aar_slutdato]            DATE NULL,
        [aar_afsluttet]           BIT NULL,
        [aar_antal_dage]          SMALLINT NULL,
        [aar_relativ]             SMALLINT NULL,

        -- Quarter
        [kvartal_id]              TINYINT NULL,
        [kvartal]                 NVARCHAR(50) NULL,
        [kvartals_startdato]      DATE NULL,
        [kvartals_slutdato]       DATE NULL,
        [kvartal_afsluttet]       BIT NULL,
        [kvartal_aar_id]          INT NULL,
        [kvartal_aar]             NVARCHAR(50) NULL,
        [kvartal_antal_dage]      TINYINT NULL,
        [kvartal_relativ]         SMALLINT NULL,

        -- Month
        [maaned_id]               TINYINT NULL,
        [maaned_lang]             NVARCHAR(50) NULL,
        [maaned]                  NVARCHAR(50) NULL,
        [maaned_startdato]        DATE NULL,
        [maaned_slutdato]         DATE NULL,
        [maaned_afsluttet]        BIT NULL,
        [maaned_aar_id]           INT NULL,
        [maaned_aar]              NVARCHAR(50) NULL,
        [maaned_antal_dage]       TINYINT NULL,
        [maaned_relativ]          SMALLINT NULL,

        -- Week (ISO 8601)
        [uge_id]                  TINYINT NULL,
        [uge]                     NVARCHAR(50) NULL,
        [uge_startdato]           DATE NULL,
        [uge_slutdato]            DATE NULL,
        [uge_afsluttet]           BIT NULL,
        [uge_aar_id]              INT NULL,
        [uge_aar]                 NVARCHAR(50) NULL,
        [uge_relativ]             SMALLINT NULL,

        -- Day and weekday
        [dag_aar]                 SMALLINT NULL,
        [dag_kvartal]             TINYINT NULL,
        [dag_maaned]              TINYINT NULL,
        [dag_uge]                 TINYINT NULL,
        [dag_relativ]             SMALLINT NULL,
        [ugedag_lang]             NVARCHAR(50) NULL,
        [ugedag]                  NVARCHAR(50) NULL,
        [er_hverdag]              BIT NULL,
        [hverdag_weekend]         NVARCHAR(50) NULL,

        -- Holidays
        [er_helligdag]            BIT NULL,
        [helligdag]               NVARCHAR(50) NULL,
        [er_beredskabshelligdag]  BIT NULL,
        [beredskabshelligdag]     NVARCHAR(50) NULL,

        -- Audit
        [dw_timestamp_insert]     DATETIME2(0) NOT NULL CONSTRAINT [DF_Dato_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]     DATETIME2(0) NOT NULL CONSTRAINT [DF_Dato_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Dim_Dato] PRIMARY KEY CLUSTERED ([dw_ek_Dato] ASC) WITH(DATA_COMPRESSION=PAGE)
    ) ON [PRIMARY];

    -- Insert sentinel rows for out-of-range and non-calendar states.
    INSERT INTO [dim].[Dato] (
        [dw_ek_Dato],
        [kvartal], [kvartal_aar],
        [maaned_lang], [maaned], [maaned_aar],
        [uge], [uge_aar],
        [ugedag_lang], [ugedag],
        [hverdag_weekend],
        [helligdag],
        [beredskabshelligdag]
    ) VALUES (
        -1,
        N'Ukendt', N'Ukendt',
        N'Ukendt', N'Ukendt', N'Ukendt',
        N'Ukendt', N'Ukendt',
        N'Ukendt', N'Ukendt',
        N'Ukendt',
        N'Ukendt',
        N'Ukendt'
    ), (
        -2,
        N'Før min dato', N'Før min dato',
        N'Før min dato', N'Før min dato', N'Før min dato',
        N'Før min dato', N'Før min dato',
        N'Før min dato', N'Før min dato',
        N'Før min dato',
        N'Før min dato',
        N'Før min dato'
    ), (
        99999998,
        N'Blank', N'Blank',
        N'Blank', N'Blank', N'Blank',
        N'Blank', N'Blank',
        N'Blank', N'Blank',
        N'Blank',
        N'Blank',
        N'Blank'
    ), (
        99999999,
        N'Efter max dato', N'Efter max dato',
        N'Efter max dato', N'Efter max dato', N'Efter max dato',
        N'Efter max dato', N'Efter max dato',
        N'Efter max dato', N'Efter max dato',
        N'Efter max dato',
        N'Efter max dato',
        N'Efter max dato'
    );
END
GO
