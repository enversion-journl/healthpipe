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
-- One row per (DW_EK_Borger × address-period). T2 scope is intentionally
-- narrow: only address fields generate new versions; all other attributes
-- are Type 1 and overwritten on update.
--
-- Keys (INT surrogates assigned in ref tables — see data_lineage/dim/dim_borger.yml):
--   * DW_SK_Borger — INT, version-PK (carried from ref.Patient_Address_Borger; no local IDENTITY)
--   * DW_EK_Borger — INT, entity-key (assigned by ref.Patient_Borger; stable across T2 versions)
--
-- Identifier columns (CPRnummer, X_eCPR_Nummer, D_eCPR_Nummer) — at least
-- one must be set per borger; enforced upstream by ref.Patient_Borger.
--
-- Sentinel rows:
--   * DW_SK_Borger = -1, DW_EK_Borger = -1  (UNKNOWN)
--   * DW_SK_Borger = -2, DW_EK_Borger = -2  (N/A — used for facts where
--                                            borger-relation explicitly does not apply)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Borger](
        -- Technical keys
        [DW_SK_Borger]                  INT NOT NULL,
        [DW_EK_Borger]                  INT NOT NULL,

        -- Identifiers (Type 1)
        [CPRnummer]                     VARCHAR(10) NULL,
        [TidligereCPR]                  VARCHAR(10) NULL,
        [X_eCPR_Nummer]                 NVARCHAR(50) NULL,
        [D_eCPR_Nummer]                 NVARCHAR(50) NULL,
        [PATIENT_ID]                    BIGINT NULL,

        -- Name (Type 1 — name:official slice fra dk-core)
        [Fornavne]                      NVARCHAR(500) NULL,
        [Efternavn]                     NVARCHAR(255) NULL,
        [Adresseringsnavn]              NVARCHAR(500) NULL,

        -- Demographics (Type 1)
        [Foedselsdato]                  DATE NULL,
        [KoenNavn]                      NVARCHAR(20) NULL,
        [Active]                        BIT NULL,
        [Deceased]                      BIT NULL,

        -- Family relations (Type 1)
        [MorCPRNummer]                  VARCHAR(10) NULL,
        [FarCPRNummer]                  VARCHAR(10) NULL,

        -- Marital status (Type 1)
        [CivilStatusKode]               NVARCHAR(8) NULL,
        [CivilStatus]                   NVARCHAR(100) NULL,

        -- Citizenship (Type 1)
        [StatsborgerskabKode]           NVARCHAR(8) NULL,
        [Statsborgerskab]               NVARCHAR(255) NULL,

        -- Address (T2 — one row per period from ref.Patient_Address_Borger)
        [DatoTid_Tilflytning]           DATETIME2 NULL,
        [DatoTid_Fraflytning]           DATETIME2 NULL,
        [Gade]                          NVARCHAR(500) NULL,
        [Postnr]                        VARCHAR(10) NULL,
        [Postdistrikt]                  NVARCHAR(255) NULL,
        [KommuneID]                     VARCHAR(8) NULL,
        [KommuneNavn]                   NVARCHAR(255) NULL,
        [RegionID]                      VARCHAR(8) NULL,
        [RegionNavn]                    NVARCHAR(255) NULL,
        [Land]                          NVARCHAR(8) NULL,
        [kvhx]                          VARCHAR(64) NULL,

        -- Current address denormalization (Type 1 copy of current address
        -- repeated on every T2 row so reports avoid double-join)
        [Gade_Nuv]                      NVARCHAR(500) NULL,
        [Postnr_Nuv]                    VARCHAR(10) NULL,
        [Postdistrikt_Nuv]              NVARCHAR(255) NULL,
        [KommuneID_Nuv]                 VARCHAR(8) NULL,
        [KommuneNavn_Nuv]               NVARCHAR(255) NULL,
        [RegionID_Nuv]                  VARCHAR(8) NULL,
        [RegionNavn_Nuv]                NVARCHAR(255) NULL,

        -- General practitioner (Type 1)
        [LaegeNavn]                     NVARCHAR(500) NULL,
        [LaegeNummer]                   VARCHAR(50) NULL,

        -- District (RSyd-local, Type 1)
        [Person_Distriktkode]           VARCHAR(8) NULL,
        [Person_Distriktkodetekst]      NVARCHAR(100) NULL,
        [Person_Socialdistriktkode]     VARCHAR(8) NULL,
        [Person_Socialdistriktkodetekst] NVARCHAR(100) NULL,

        -- Privacy / context flags (Type 1)
        [Adressebeskyttelse]            BIT NULL,
        [OnskerDiskretion]              BIT NULL,
        [Medcom_Testperson]             BIT NULL,

        -- Audit / temporal
        [OpdateretFraCPR]               DATETIME2 NULL,
        [DW_ValidFrom]                  DATETIME2(0) NOT NULL,
        [DW_ValidTo]                    DATETIME2(0) NOT NULL,
        [DW_IsCurrent]                  BIT NOT NULL CONSTRAINT DF_Borger_DW_IsCurrent DEFAULT 1,
        [DW_IsDeleted]                  BIT NOT NULL CONSTRAINT DF_Borger_DW_IsDeleted DEFAULT 0,
        [DW_Timestamp_Insert]           DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update]           DATETIME2(0) NULL,
        CONSTRAINT [PK_Dim_Borger] PRIMARY KEY CLUSTERED ([DW_SK_Borger] ASC)
    ) ON [PRIMARY];

    -- Insert sentinel rows. Identifier-systems live centrally in
    -- utils/fhir/identifier_systems.py; sentinel rows use literal markers
    -- that don't resolve to any real identifier system.

    -- -1 = UNKNOWN: fact rows where borger is unknown but should be linked
    INSERT INTO [dim].[Borger] (
        [DW_SK_Borger], [DW_EK_Borger], [CPRnummer],
        [Fornavne], [Efternavn],
        [DW_ValidFrom], [DW_ValidTo], [DW_IsCurrent], [DW_IsDeleted],
        [DW_Timestamp_Insert]
    ) VALUES (
        -1, -1, 'UNKNOWN',
        N'Ukendt', N'Borger',
        CAST('1900-01-01' AS datetime2(0)), CAST('9999-12-31 23:59:59' AS datetime2(0)), 1, 0,
        CAST(SYSDATETIME() AS datetime2(0))
    );

    -- -2 = N/A: fact rows where borger-relation explicitly does not apply
    INSERT INTO [dim].[Borger] (
        [DW_SK_Borger], [DW_EK_Borger], [CPRnummer],
        [Fornavne], [Efternavn],
        [DW_ValidFrom], [DW_ValidTo], [DW_IsCurrent], [DW_IsDeleted],
        [DW_Timestamp_Insert]
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
        [DW_EK_Borger] ASC,
        [DW_ValidFrom] ASC,
        [DW_ValidTo] ASC
    )
    INCLUDE ([DW_SK_Borger])
    ON [PRIMARY];
END
GO

-- ============================================================================
-- dim.Dato (Type 1 — Date Dimension, smart key YYYYMMDD)
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Dato]') AND type = 'U')
BEGIN
    CREATE TABLE [dim].[Dato](
        [DW_EK_Dato]            INT NOT NULL,
        [Dato]                  DATE NULL,

        -- Year
        [AarID]                 SMALLINT NULL,
        [AarAntalDage]          SMALLINT NULL,

        -- Quarter
        [KvartalAarID]          SMALLINT NULL,
        [KvartalAarNavn]        NVARCHAR(50) NULL,
        [KvartalAntalDage]      SMALLINT NULL,
        [KvartalID]             SMALLINT NULL,
        [KvartalNavn]           NVARCHAR(50) NULL,

        -- Month
        [MaanedAarID]           INT NULL,
        [MaanedAarNavn]         NVARCHAR(50) NULL,
        [MaanedAntalDage]       SMALLINT NULL,
        [MaanedID]              SMALLINT NULL,
        [MaanedNavn]            NVARCHAR(50) NULL,

        -- Day
        [DagIAarID]             SMALLINT NULL,
        [DagIMaanedID]          SMALLINT NULL,

        -- Weekday
        [UgedagID]              SMALLINT NULL,
        [UgedagNavn]            NVARCHAR(50) NULL,

        -- Week (ISO 8601)
        [UgeAarID]              INT NULL,
        [UgeAarNavn]            NVARCHAR(50) NULL,
        [UgeID]                 SMALLINT NULL,
        [UgeNavn]               NVARCHAR(50) NULL,

        -- Flags
        [Hverdag]               TINYINT NULL,
        [Weekend]               TINYINT NULL,
        [HelligdagDK]           TINYINT NULL,
        [HelligdagDKNavn]       NVARCHAR(50) NULL,

        -- Audit
        [DW_Timestamp_Insert]   DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update]   DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Dim_Dato] PRIMARY KEY CLUSTERED ([DW_EK_Dato] ASC) WITH(DATA_COMPRESSION=PAGE)
    ) ON [PRIMARY];

    -- Insert UNKNOWN row (DW_EK_Dato = -1, all values NULL/Ukendt)
    INSERT INTO [dim].[Dato] (
        [DW_EK_Dato],
        [KvartalAarNavn], [KvartalNavn],
        [MaanedAarNavn], [MaanedNavn],
        [UgedagNavn], [UgeAarNavn], [UgeNavn],
        [HelligdagDKNavn]
    ) VALUES (
        -1,
        N'Ukendt kvartal', N'Ukendt kvartal',
        N'Ukendt måned', N'Ukendt måned',
        N'Ukendt ugedag', N'Ukendt uge', N'Ukendt uge',
        N'Ukendt'
    );
END
GO
