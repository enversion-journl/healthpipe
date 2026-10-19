-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Borger: new column fhir_Patient_id at position 3.
--   * fhir_Patient_id  fhir.Patient.patient_id — the FHIR resource id behind
--                      the citizen, like dim.Organisation.fhir_Organization_id.
--   * order: dw_sk_Borger, dw_ek_Borger, fhir_Patient_id, then the rest
--     unchanged.
--
-- SQL Server cannot insert a column in the middle of a table, so dim.Borger
-- is rebuilt (same DDL as tables/shared_dimensions/02_dims.sql). The data are
-- kept: the table is copied to dim.Borger_migr_fhir_id, recreated and filled
-- back column by column by name. No checkpoint is reset.
--
-- fhir_Patient_id is filled:
--   * staging: from ref.Patient_Address_To_Borger on dw_sk_Borger (the same
--     key that dim_Borger builds on).
--   * DSO: NULL until the next dso_sync_dim_borger. The DSO ref tables have
--     their own surrogate keys, so the lookup would hit the wrong rows; the
--     sync runs in merge mode over all rows and fills the column.
--
-- Views that are not schemabound keep the column list from their creation.
-- They are refreshed by 20260929_dim_borger_refresh_views.sql.
--
-- Idempotent and resumable: every step is guarded, and the copy is dropped
-- only once it has been read back. If the table has unknown columns, the
-- migration stops before anything is touched. Runs on staging and DSO.
-- ============================================================================

SET XACT_ABORT ON;
GO

-- Columns from dim.Borger version 1 that the model no longer has. No code
-- fills them, and they disappear with the rebuild.
-- CleanCode_Borger, CPR, Koen_Kode, Koen_Kodetekst, Vej, Stednavn, Kommunekode,
-- Person_Statuskode, Person_Statuskodetekst

-- 0. Stop before anything is touched if the table is to be rebuilt and has
--    columns other than those the new one gets.
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[Borger_migr_fhir_id]', 'U') IS NULL
   AND ISNULL((
        SELECT pos FROM (
            SELECT c.name, ROW_NUMBER() OVER (ORDER BY c.column_id) AS pos
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(N'[dim].[Borger]')
        ) AS k
        WHERE k.name = N'fhir_Patient_id'
   ), 0) <> 3
   AND EXISTS (
        SELECT 1 FROM sys.columns AS g
        WHERE g.object_id = OBJECT_ID(N'[dim].[Borger]')
          AND g.name NOT IN (
              N'dw_sk_Borger', N'dw_ek_Borger', N'fhir_Patient_id', N'cpr_nummer',
              N'tidligere_cpr', N'x_ecpr_nummer', N'd_ecpr_nummer', N'patient_id_ifdb',
              N'fornavne', N'efternavn', N'adresseringsnavn', N'foedselsdato', N'koen_navn',
              N'active', N'deceased', N'mor_cpr_nummer', N'far_cpr_nummer',
              N'civil_status_kode', N'civil_status', N'statsborgerskab_kode',
              N'statsborgerskab', N'dato_tid_tilflytning', N'dato_tid_fraflytning', N'gade',
              N'postnr', N'postdistrikt', N'kommune_id', N'kommunenavn', N'region_id',
              N'regionnavn', N'land', N'kvhx', N'gade_nuv', N'postnr_nuv',
              N'postdistrikt_nuv', N'kommune_id_nuv', N'kommunenavn_nuv', N'region_id_nuv',
              N'regionnavn_nuv', N'laege_navn', N'laege_nummer', N'person_distriktkode',
              N'person_distriktkodetekst', N'person_socialdistriktkode',
              N'person_socialdistriktkodetekst', N'adressebeskyttelse', N'onsker_diskretion',
              N'medcom_testperson', N'opdateret_fra_cpr', N'dw_valid_from', N'dw_valid_to',
              N'dw_is_current', N'dw_is_deleted', N'dw_timestamp_insert',
              N'dw_timestamp_update',
              N'CleanCode_Borger', N'CPR', N'Koen_Kode', N'Koen_Kodetekst', N'Vej',
              N'Stednavn', N'Kommunekode', N'Person_Statuskode', N'Person_Statuskodetekst'
          )
   )
    THROW 50003, N'dim.Borger has columns that the new dim.Borger does not get. Nothing has been changed. Reconcile manually.', 1;
GO

-- 1. Copy and drop the old table if the column is missing or misplaced.
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[Borger_migr_fhir_id]', 'U') IS NULL
   AND ISNULL((
        SELECT pos FROM (
            SELECT c.name, ROW_NUMBER() OVER (ORDER BY c.column_id) AS pos
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(N'[dim].[Borger]')
        ) AS k
        WHERE k.name = N'fhir_Patient_id'
   ), 0) <> 3
BEGIN
    PRINT 'dim.Borger lacks fhir_Patient_id at position 3 — rebuilding.';
    BEGIN TRANSACTION;
        SELECT * INTO [dim].[Borger_migr_fhir_id] FROM [dim].[Borger];
        DROP TABLE [dim].[Borger];
    COMMIT TRANSACTION;
END
GO

-- 2. Recreate the table with the new column order (without sentinel rows;
--    they are in the copy).
IF OBJECT_ID(N'[dim].[Borger_migr_fhir_id]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[Borger]', 'U') IS NULL
BEGIN
    CREATE TABLE [dim].[Borger](
        -- Technical keys
        [dw_sk_Borger]                  INT NOT NULL,
        [dw_ek_Borger]                  INT NOT NULL,
        [fhir_Patient_id]               NVARCHAR(255) NULL,

        -- Identifiers (Type 1)
        [cpr_nummer]                     VARCHAR(10) NULL,
        [tidligere_cpr]                  VARCHAR(10) NULL,
        [x_ecpr_nummer]                 NVARCHAR(50) NULL,
        [d_ecpr_nummer]                 NVARCHAR(50) NULL,
        [patient_id_ifdb]                    BIGINT NULL,

        -- Name (Type 1 — name:official slice from dk-core)
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

        -- District (region-local, Type 1)
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
END
GO

-- 3. Fill back. The column list is the intersection of the copy and the new
--    table, so a deviating deployment does not break the step. Dynamic SQL,
--    because the columns of the copy are known only at run time.
IF OBJECT_ID(N'[dim].[Borger_migr_fhir_id]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
BEGIN
    DECLARE @cols NVARCHAR(MAX) = (
        SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), QUOTENAME(n.name)), N', ')
                   WITHIN GROUP (ORDER BY n.column_id)
        FROM sys.columns AS n
        INNER JOIN sys.columns AS g
            ON g.name = n.name
           AND g.object_id = OBJECT_ID(N'[dim].[Borger_migr_fhir_id]')
        WHERE n.object_id = OBJECT_ID(N'[dim].[Borger]')
          AND n.name <> N'fhir_Patient_id'
    );

    -- dw_sk_Borger is IDENTITY in the ref table and therefore unique; the join
    -- cannot multiply rows.
    DECLARE @fill_from_ref BIT =
        CASE
            WHEN DB_NAME() LIKE N'%staging%'
                 AND OBJECT_ID(N'[ref].[Patient_Address_To_Borger]', 'U') IS NOT NULL
            THEN 1 ELSE 0
        END;

    DECLARE @sql NVARCHAR(MAX) =
          N'INSERT INTO [dim].[Borger] WITH (TABLOCK) (' + @cols + N', [fhir_Patient_id]) '
        + N'SELECT ' + REPLACE(@cols, N'[', N'g.[') + N', '
        + CASE WHEN @fill_from_ref = 1
               THEN N'r.patient_id FROM [dim].[Borger_migr_fhir_id] AS g '
                  + N'LEFT JOIN [ref].[Patient_Address_To_Borger] AS r '
                  + N'ON r.dw_sk_Borger = g.dw_sk_Borger AND g.dw_sk_Borger > 0;'
               ELSE N'CAST(NULL AS NVARCHAR(255)) FROM [dim].[Borger_migr_fhir_id] AS g;'
          END;

    -- A column that only the copy has would disappear. Stop, and keep the copy.
    IF EXISTS (
        SELECT 1 FROM sys.columns AS g
        WHERE g.object_id = OBJECT_ID(N'[dim].[Borger_migr_fhir_id]')
          AND g.name NOT IN (
              N'CleanCode_Borger', N'CPR', N'Koen_Kode', N'Koen_Kodetekst', N'Vej',
              N'Stednavn', N'Kommunekode', N'Person_Statuskode', N'Person_Statuskodetekst'
          )
          AND NOT EXISTS (
              SELECT 1 FROM sys.columns AS n
              WHERE n.object_id = OBJECT_ID(N'[dim].[Borger]') AND n.name = g.name
          )
    )
        THROW 50002, N'dim.Borger_migr_fhir_id has columns that the new dim.Borger does not have. Reconcile manually.', 1;

    -- If the new table already holds rows, someone got there before the
    -- fill-back. Stop, and keep the copy, rather than guess.
    IF EXISTS (SELECT 1 FROM [dim].[Borger])
        THROW 50001, N'dim.Borger is not empty, but the copy dim.Borger_migr_fhir_id still exists. Reconcile manually.', 1;

    BEGIN TRANSACTION;
        EXEC sys.sp_executesql @sql;
        DROP TABLE [dim].[Borger_migr_fhir_id];
    COMMIT TRANSACTION;
END
GO

-- 4. The indexes from the base DDL (same names and guards as there).
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
         AND name = N'IX_Dim_Borger_Temporal'
   )
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
