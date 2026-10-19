-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: address and contact columns on dim.Rekvirent
--
-- Test feedback from DOL asked for altrnr, adr, adr2, postdi, kontakt, ktosted,
-- land and by in dim.Rekvirent. The first seven come 1:1 from arc.ils_rekv;
-- "bynavn" is looked up in the new arc.ils_postdi on (land, postdi).
--
-- Runs automatically on BOTH staging and DSO via the central migrations runner
-- (staging_migrations / dso_migrations). Upgrade path for EXISTING databases
-- only -- fresh databases are baselined (migrations stamped without executing)
-- and get the columns from tables/erp/02_dims.sql. Every step is guarded with
-- OBJECT_ID(...) IS NOT NULL so it no-ops on a database provisioned without the
-- erp domain, where dim.Rekvirent does not exist (same pattern as
-- 20260518_add_dw_ek_okosted_to_facts.sql). Safe to re-run.
--
-- The staging table needs no step here: stg_dim_Rekvirent does DROP TABLE IF
-- EXISTS + CREATE on every materialization, so it picks up the new column set
-- by itself.
--
-- The column names are spelled out rather than carrying the ILS abbreviations
-- (adr, postdi, ktosted); the translation to the source columns lives in
-- data_lineage/dim/dim_rekvirent.yml and in stg_dim_Rekvirent.
--
-- GDPR: adresse, adresse2 and kontaktperson may hold the home patient's or a
-- relative's details when rtyp = 'HP'/'OHP' (flagged by Strategisk Indkoeb).
-- The dim table itself is unmasked -- masking happens in
-- vw_dim.Rekvirent (views/security/01_vw_dim_security.sql), which gates on the
-- U role in DPA_ERP exactly as it does for the requisitioner name (display).
-- Consumers must read vw_dim.Rekvirent, not dim.Rekvirent.
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'alternativt_rekvirentnummer') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [alternativt_rekvirentnummer] NVARCHAR(16) NULL;
GO
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'adresse') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [adresse] NVARCHAR(80) NULL;
GO
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'adresse2') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [adresse2] NVARCHAR(80) NULL;
GO
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'land') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [land] NVARCHAR(6) NULL;
GO
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'postnummer') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [postnummer] NVARCHAR(10) NULL;
GO
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'bynavn') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [bynavn] NVARCHAR(58) NULL;
GO
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'kontaktperson') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [kontaktperson] NVARCHAR(80) NULL;
GO
IF OBJECT_ID(N'[dim].[Rekvirent]') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Rekvirent]', 'kontosted') IS NULL
    ALTER TABLE [dim].[Rekvirent] ADD [kontosted] NVARCHAR(34) NULL;
GO
