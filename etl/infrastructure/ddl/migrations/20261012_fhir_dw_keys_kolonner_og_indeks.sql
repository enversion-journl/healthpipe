-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dw-key columns on fhir.EpisodeOfCare and fhir.Location, and the dimension
-- indexes the lookup uses. Additive only.
--
--   * fhir.EpisodeOfCare: dw_ek_Borger, dw_sk_Borger, dw_ek_Organisation.
--   * fhir.Location: dw_ek_Organisation.
--     All INT NOT NULL DEFAULT -1, set by the fhir_dw_keys_* assets; the SoF
--     MERGE lists only the view's columns and leaves them alone.
--   * dim.Borger: IX_Dim_Borger_FhirPatientId_Temporal, IX_Dim_Borger_TimestampUpdate.
--   * dim.Organisation: IX_dim_Organisation_FhirOrganizationId.
--   * vw_fhir.EpisodeOfCare / vw_fhir.Location (SELECT *) are refreshed.
--
-- The period columns of fhir.EpisodeOfCare change type in
-- 20261013_fhir_episodeofcare_period_datetime2.sql.
--
-- Base DDL: tables/fhir/01_tables.sql, tables/shared_dimensions/02_dims.sql,
-- tables/organisation/02_dims.sql. Runs on staging and DSO and can be rerun.
-- ============================================================================

SET XACT_ABORT ON;
GO

-- 1. dw-key columns.
IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.EpisodeOfCare', N'dw_ek_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ADD [dw_ek_Borger] INT NOT NULL
        CONSTRAINT [DF_EpisodeOfCare_dw_ek_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.EpisodeOfCare', N'dw_sk_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ADD [dw_sk_Borger] INT NOT NULL
        CONSTRAINT [DF_EpisodeOfCare_dw_sk_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.EpisodeOfCare', N'dw_ek_Organisation') IS NULL
    EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ADD [dw_ek_Organisation] INT NOT NULL
        CONSTRAINT [DF_EpisodeOfCare_dw_ek_Organisation] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Location', N'dw_ek_Organisation') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Location] ADD [dw_ek_Organisation] INT NOT NULL
        CONSTRAINT [DF_Location_dw_ek_Organisation] DEFAULT -1;');
GO

-- 2. Dimension indexes for the lookup and for finding changed rows.
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND COL_LENGTH(N'dim.Borger', N'fhir_Patient_id') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = N'IX_Dim_Borger_FhirPatientId_Temporal'
   )
    EXEC(N'CREATE NONCLUSTERED INDEX [IX_Dim_Borger_FhirPatientId_Temporal]
        ON [dim].[Borger] ([fhir_Patient_id] ASC, [dw_valid_from] ASC, [dw_valid_to] ASC)
        INCLUDE ([dw_ek_Borger], [dw_sk_Borger], [dw_is_deleted]) ON [PRIMARY];');
GO

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND COL_LENGTH(N'dim.Borger', N'fhir_Patient_id') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = N'IX_Dim_Borger_TimestampUpdate'
   )
    EXEC(N'CREATE NONCLUSTERED INDEX [IX_Dim_Borger_TimestampUpdate]
        ON [dim].[Borger] ([dw_timestamp_update] ASC)
        INCLUDE ([fhir_Patient_id]) ON [PRIMARY];');
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = N'IX_dim_Organisation_FhirOrganizationId'
   )
    EXEC(N'CREATE INDEX [IX_dim_Organisation_FhirOrganizationId]
        ON [dim].[Organisation] ([fhir_Organization_id])
        INCLUDE ([dw_is_deleted]);');
GO

-- 3. SELECT * views keep the column list from their creation.
IF OBJECT_ID(N'[vw_fhir].[EpisodeOfCare]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[EpisodeOfCare]';
GO

IF OBJECT_ID(N'[vw_fhir].[Location]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[Location]';
GO
