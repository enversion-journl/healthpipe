-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dw-key columns on fhir.Appointment. Additive only.
--
--   * dw_ek_Borger, dw_sk_Borger: looked up from patient_id and start.
--   * dw_ek_Organisation: inherited from fhir.Location.dw_ek_Organisation through
--     performing_location_id.
--     All INT NOT NULL DEFAULT -1, set by the fhir_dw_keys_appointment asset;
--     the SoF MERGE lists only the view's columns and leaves them alone.
--   * vw_fhir.Appointment (SELECT *) is refreshed.
--
-- start stays NVARCHAR; the lookup reads its clock time. The dimension indexes
-- came with 20261012_fhir_dw_keys_kolonner_og_indeks.sql.
--
-- Base DDL: tables/fhir/01_tables.sql. Runs on staging and DSO and can be rerun.
-- ============================================================================

SET XACT_ABORT ON;
GO

-- 1. dw-key columns.
IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Appointment', N'dw_ek_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Appointment] ADD [dw_ek_Borger] INT NOT NULL
        CONSTRAINT [DF_Appointment_dw_ek_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Appointment', N'dw_sk_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Appointment] ADD [dw_sk_Borger] INT NOT NULL
        CONSTRAINT [DF_Appointment_dw_sk_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Appointment', N'dw_ek_Organisation') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Appointment] ADD [dw_ek_Organisation] INT NOT NULL
        CONSTRAINT [DF_Appointment_dw_ek_Organisation] DEFAULT -1;');
GO

-- 2. SELECT * views keep the column list from their creation.
IF OBJECT_ID(N'[vw_fhir].[Appointment]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[Appointment]';
GO
