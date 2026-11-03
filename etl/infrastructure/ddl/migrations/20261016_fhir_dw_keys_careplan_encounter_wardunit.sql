-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dw-key columns on fhir.CarePlan, fhir.CarePlan_Activity, fhir.Encounter and
-- fhir.WardUnit. Additive only.
--
--   * CarePlan, CarePlan_Activity, Encounter: dw_ek_Borger, dw_sk_Borger,
--     dw_ek_Organisation.
--   * WardUnit: dw_ek_Organisation.
--     All INT NOT NULL DEFAULT -1, set by the fhir_dw_keys_* assets; the SoF
--     MERGE lists only the view's columns and leaves them alone.
--   * The vw_fhir views of the four tables (SELECT *) are refreshed.
--
-- The time columns the lookup reads stay NVARCHAR; the lookup reads their clock
-- time. The dimension indexes came with 20261012_fhir_dw_keys_kolonner_og_indeks.sql.
--
-- Base DDL: tables/fhir/01_tables.sql. Runs on staging and DSO and can be rerun.
-- ============================================================================

SET XACT_ABORT ON;
GO

-- 1. dw-key columns.
IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.CarePlan', N'dw_ek_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[CarePlan] ADD [dw_ek_Borger] INT NOT NULL
        CONSTRAINT [DF_CarePlan_dw_ek_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.CarePlan', N'dw_sk_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[CarePlan] ADD [dw_sk_Borger] INT NOT NULL
        CONSTRAINT [DF_CarePlan_dw_sk_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.CarePlan', N'dw_ek_Organisation') IS NULL
    EXEC(N'ALTER TABLE [fhir].[CarePlan] ADD [dw_ek_Organisation] INT NOT NULL
        CONSTRAINT [DF_CarePlan_dw_ek_Organisation] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.CarePlan_Activity', N'dw_ek_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ADD [dw_ek_Borger] INT NOT NULL
        CONSTRAINT [DF_CarePlan_Activity_dw_ek_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.CarePlan_Activity', N'dw_sk_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ADD [dw_sk_Borger] INT NOT NULL
        CONSTRAINT [DF_CarePlan_Activity_dw_sk_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.CarePlan_Activity', N'dw_ek_Organisation') IS NULL
    EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ADD [dw_ek_Organisation] INT NOT NULL
        CONSTRAINT [DF_CarePlan_Activity_dw_ek_Organisation] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Encounter', N'dw_ek_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Encounter] ADD [dw_ek_Borger] INT NOT NULL
        CONSTRAINT [DF_Encounter_dw_ek_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Encounter', N'dw_sk_Borger') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Encounter] ADD [dw_sk_Borger] INT NOT NULL
        CONSTRAINT [DF_Encounter_dw_sk_Borger] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.Encounter', N'dw_ek_Organisation') IS NULL
    EXEC(N'ALTER TABLE [fhir].[Encounter] ADD [dw_ek_Organisation] INT NOT NULL
        CONSTRAINT [DF_Encounter_dw_ek_Organisation] DEFAULT -1;');
GO

IF OBJECT_ID(N'[fhir].[WardUnit]', 'U') IS NOT NULL
   AND COL_LENGTH(N'fhir.WardUnit', N'dw_ek_Organisation') IS NULL
    EXEC(N'ALTER TABLE [fhir].[WardUnit] ADD [dw_ek_Organisation] INT NOT NULL
        CONSTRAINT [DF_WardUnit_dw_ek_Organisation] DEFAULT -1;');
GO

-- 2. SELECT * views keep the column list from their creation.
IF OBJECT_ID(N'[vw_fhir].[CarePlan]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[CarePlan]';
GO

IF OBJECT_ID(N'[vw_fhir].[CarePlan_Activity]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[CarePlan_Activity]';
GO

IF OBJECT_ID(N'[vw_fhir].[Encounter]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[Encounter]';
GO

IF OBJECT_ID(N'[vw_fhir].[WardUnit]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[WardUnit]';
GO
