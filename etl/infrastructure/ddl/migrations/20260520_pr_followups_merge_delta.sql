-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- [internal ticket] follow-ups: ref.Patient_Address_Borger + dim.Borger MERGE-readiness
--
-- Three changes:
--   1. Add DW_IsDeleted column to ref.Patient_Address_Borger (required for
--      MERGE soft-delete branch).
--   2. Add UNIQUE index on (patient_id, address_period_start) — the MERGE key.
--   3. Drop IDENTITY on dim.Borger.DW_SK_Borger — SK is now carried from
--      ref.Patient_Address_Borger, not generated locally.
--
-- All steps are idempotent via sys.columns / sys.indexes / COLUMNPROPERTY checks.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. ref.Patient_Address_Borger — add DW_IsDeleted BIT NOT NULL DEFAULT 0
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[ref].[Patient_Address_Borger]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_Borger]')
          AND name = N'DW_IsDeleted'
    )
BEGIN
    ALTER TABLE [ref].[Patient_Address_Borger]
        ADD [DW_IsDeleted] BIT NOT NULL
            CONSTRAINT [DF_Ref_Patient_Address_Borger_DW_IsDeleted] DEFAULT 0;
    PRINT 'Added DW_IsDeleted column to ref.Patient_Address_Borger';
END
GO

-- ----------------------------------------------------------------------------
-- 2. ref.Patient_Address_Borger — UNIQUE index on MERGE key
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[ref].[Patient_Address_Borger]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_Borger]')
          AND name = N'UX_Ref_Patient_Address_Borger_PatientPeriod'
    )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UX_Ref_Patient_Address_Borger_PatientPeriod]
        ON [ref].[Patient_Address_Borger] ([patient_id] ASC, [address_period_start] ASC)
        ON [PRIMARY];
    PRINT 'Created unique index UX_Ref_Patient_Address_Borger_PatientPeriod';
END
GO

-- ----------------------------------------------------------------------------
-- 3. dim.Borger — drop IDENTITY on DW_SK_Borger via temp-table swap
--
-- Steps: create new table with same schema minus IDENTITY → copy data →
-- drop old → rename new → re-add PK + indexes.
-- Sentinel rows (-1, -2) are preserved by the data copy.
-- ----------------------------------------------------------------------------
IF COLUMNPROPERTY(OBJECT_ID(N'[dim].[Borger]'), N'DW_SK_Borger', 'IsIdentity') = 1
BEGIN
    -- 3a. Drop dependent objects that reference dim.Borger
    IF OBJECT_ID(N'[vw_dim].[Borger]', 'V') IS NOT NULL
    BEGIN
        DROP VIEW [vw_dim].[Borger];
        PRINT 'Dropped view vw_dim.Borger (will be recreated by next infra deployment)';
    END

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = N'IX_Dim_Borger_Temporal')
    BEGIN
        DROP INDEX [IX_Dim_Borger_Temporal] ON [dim].[Borger];
        PRINT 'Dropped index IX_Dim_Borger_Temporal';
    END

    -- 3b. Create replacement table (no IDENTITY on DW_SK_Borger)
    CREATE TABLE [dim].[Borger_New](
        [DW_SK_Borger]                  INT NOT NULL,
        [DW_EK_Borger]                  INT NOT NULL,
        [CPRnummer]                     VARCHAR(10) NULL,
        [TidligereCPR]                  VARCHAR(10) NULL,
        [X_eCPR_Nummer]                 NVARCHAR(50) NULL,
        [D_eCPR_Nummer]                 NVARCHAR(50) NULL,
        [PATIENT_ID]                    BIGINT NULL,
        [Fornavne]                      NVARCHAR(500) NULL,
        [Efternavn]                     NVARCHAR(255) NULL,
        [Adresseringsnavn]              NVARCHAR(500) NULL,
        [Foedselsdato]                  DATE NULL,
        [KoenNavn]                      NVARCHAR(20) NULL,
        [Active]                        BIT NULL,
        [Deceased]                      BIT NULL,
        [MorCPRNummer]                  VARCHAR(10) NULL,
        [FarCPRNummer]                  VARCHAR(10) NULL,
        [CivilStatusKode]               NVARCHAR(8) NULL,
        [CivilStatus]                   NVARCHAR(100) NULL,
        [StatsborgerskabKode]           NVARCHAR(8) NULL,
        [Statsborgerskab]               NVARCHAR(255) NULL,
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
        [Gade_Nuv]                      NVARCHAR(500) NULL,
        [Postnr_Nuv]                    VARCHAR(10) NULL,
        [Postdistrikt_Nuv]              NVARCHAR(255) NULL,
        [KommuneID_Nuv]                 VARCHAR(8) NULL,
        [KommuneNavn_Nuv]               NVARCHAR(255) NULL,
        [RegionID_Nuv]                  VARCHAR(8) NULL,
        [RegionNavn_Nuv]                NVARCHAR(255) NULL,
        [LaegeNavn]                     NVARCHAR(500) NULL,
        [LaegeNummer]                   VARCHAR(50) NULL,
        [Person_Distriktkode]           VARCHAR(8) NULL,
        [Person_Distriktkodetekst]      NVARCHAR(100) NULL,
        [Person_Socialdistriktkode]     VARCHAR(8) NULL,
        [Person_Socialdistriktkodetekst] NVARCHAR(100) NULL,
        [Adressebeskyttelse]            BIT NULL,
        [OnskerDiskretion]              BIT NULL,
        [Medcom_Testperson]             BIT NULL,
        [OpdateretFraCPR]               DATETIME2 NULL,
        [DW_ValidFrom]                  DATETIME2(0) NOT NULL,
        [DW_ValidTo]                    DATETIME2(0) NOT NULL,
        [DW_IsCurrent]                  BIT NOT NULL DEFAULT 1,
        [DW_IsDeleted]                  BIT NOT NULL DEFAULT 0,
        [DW_Timestamp_Insert]           DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update]           DATETIME2(0) NULL,
        CONSTRAINT [PK_Dim_Borger_New] PRIMARY KEY CLUSTERED ([DW_SK_Borger] ASC)
    );

    -- 3c. Copy all data (including sentinel rows -1, -2)
    INSERT INTO [dim].[Borger_New] (
        [DW_SK_Borger], [DW_EK_Borger],
        [CPRnummer], [TidligereCPR], [X_eCPR_Nummer], [D_eCPR_Nummer], [PATIENT_ID],
        [Fornavne], [Efternavn], [Adresseringsnavn],
        [Foedselsdato], [KoenNavn], [Active], [Deceased],
        [MorCPRNummer], [FarCPRNummer],
        [CivilStatusKode], [CivilStatus],
        [StatsborgerskabKode], [Statsborgerskab],
        [DatoTid_Tilflytning], [DatoTid_Fraflytning],
        [Gade], [Postnr], [Postdistrikt], [KommuneID], [KommuneNavn],
        [RegionID], [RegionNavn], [Land], [kvhx],
        [Gade_Nuv], [Postnr_Nuv], [Postdistrikt_Nuv],
        [KommuneID_Nuv], [KommuneNavn_Nuv], [RegionID_Nuv], [RegionNavn_Nuv],
        [LaegeNavn], [LaegeNummer],
        [Person_Distriktkode], [Person_Distriktkodetekst],
        [Person_Socialdistriktkode], [Person_Socialdistriktkodetekst],
        [Adressebeskyttelse], [OnskerDiskretion], [Medcom_Testperson],
        [OpdateretFraCPR],
        [DW_ValidFrom], [DW_ValidTo], [DW_IsCurrent], [DW_IsDeleted],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    SELECT
        [DW_SK_Borger], [DW_EK_Borger],
        [CPRnummer], [TidligereCPR], [X_eCPR_Nummer], [D_eCPR_Nummer], [PATIENT_ID],
        [Fornavne], [Efternavn], [Adresseringsnavn],
        [Foedselsdato], [KoenNavn], [Active], [Deceased],
        [MorCPRNummer], [FarCPRNummer],
        [CivilStatusKode], [CivilStatus],
        [StatsborgerskabKode], [Statsborgerskab],
        [DatoTid_Tilflytning], [DatoTid_Fraflytning],
        [Gade], [Postnr], [Postdistrikt], [KommuneID], [KommuneNavn],
        [RegionID], [RegionNavn], [Land], [kvhx],
        [Gade_Nuv], [Postnr_Nuv], [Postdistrikt_Nuv],
        [KommuneID_Nuv], [KommuneNavn_Nuv], [RegionID_Nuv], [RegionNavn_Nuv],
        [LaegeNavn], [LaegeNummer],
        [Person_Distriktkode], [Person_Distriktkodetekst],
        [Person_Socialdistriktkode], [Person_Socialdistriktkodetekst],
        [Adressebeskyttelse], [OnskerDiskretion], [Medcom_Testperson],
        [OpdateretFraCPR],
        [DW_ValidFrom], [DW_ValidTo], [DW_IsCurrent], [DW_IsDeleted],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    FROM [dim].[Borger];

    -- 3d. Drop old table, rename new
    DROP TABLE [dim].[Borger];
    EXEC sp_rename 'dim.Borger_New', 'Borger';
    EXEC sp_rename 'dim.PK_Dim_Borger_New', 'PK_Dim_Borger', 'OBJECT';

    -- 3e. Re-create temporal lookup index
    CREATE NONCLUSTERED INDEX [IX_Dim_Borger_Temporal]
    ON [dim].[Borger] (
        [DW_EK_Borger] ASC,
        [DW_ValidFrom] ASC,
        [DW_ValidTo] ASC
    )
    INCLUDE ([DW_SK_Borger])
    ON [PRIMARY];

    PRINT 'Rebuilt dim.Borger: DW_SK_Borger is now INT (no IDENTITY). Data preserved.';
END
GO
