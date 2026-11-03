-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dw-key columns on fhir.ServiceRequest. Additive only.
--
--   * dw_ek_Borger, dw_sk_Borger: looked up from patient_id and authored_on.
--   * dw_ek_Organisation_henvist_til (performer_id) and
--     dw_ek_Organisation_henvist_fra (requester_id): one organisation key per role.
--     All INT NOT NULL DEFAULT -1, set by the fhir_dw_keys_service_request asset;
--     the SoF MERGE lists only the view's columns and leaves them alone.
--   * vw_fhir.ServiceRequest (sr.*) is refreshed.
--
-- authored_on stays NVARCHAR; the lookup reads its clock time. The dimension
-- indexes came with 20261012_fhir_dw_keys_kolonner_og_indeks.sql.
--
-- Base DDL: tables/fhir/01_tables.sql. Runs on staging and DSO and can be rerun.
-- ============================================================================

SET XACT_ABORT ON;
GO

-- 1. dw-key columns.
IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.ServiceRequest', N'dw_ek_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD [dw_ek_Borger] INT NOT NULL
        CONSTRAINT [DF_ServiceRequest_dw_ek_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.ServiceRequest', N'dw_sk_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD [dw_sk_Borger] INT NOT NULL
        CONSTRAINT [DF_ServiceRequest_dw_sk_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.ServiceRequest', N'dw_ek_Organisation_henvist_til') IS NULL
    EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD [dw_ek_Organisation_henvist_til] INT NOT NULL
        CONSTRAINT [DF_ServiceRequest_dw_ek_Organisation_henvist_til] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.ServiceRequest', N'dw_ek_Organisation_henvist_fra') IS NULL
    EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD [dw_ek_Organisation_henvist_fra] INT NOT NULL
        CONSTRAINT [DF_ServiceRequest_dw_ek_Organisation_henvist_fra] DEFAULT -1;');
GO

-- 2. A view over sr.* keeps the column list from its creation.
IF OBJECT_ID(N'[vw_fhir].[ServiceRequest]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[ServiceRequest]';
GO
