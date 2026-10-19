-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: lafth enrichment columns on dim.Aftale
--
-- dim.Aftale previously carried only lnr, aftale, aftdto, ansvar, afttype, txt,
-- start_ and slut from dbo.lafth. This adds the remaining columns the review
-- decided to keep (see etl/docs/source_systems/ils/dim_aftale_kolonner_beslutningsnotat.md):
-- standard_leveringsdage (deflevdage), dw_ek_Dato_sysdato + dato_tid_sysdato
-- (sysdato, per the date-key convention), bruger, aktiv, and the freight/fee
-- terms (fragtbel, sw_fragt, gebyrbel, sw_gebyr, graensebel, fragtgraensebel).
--
-- Runs automatically on BOTH staging and DSO via the central migrations runner
-- (staging_migrations / dso_migrations). Upgrade path for EXISTING databases
-- only -- fresh databases are baselined (migrations stamped without executing)
-- and get the columns from tables/erp/02_dims.sql. Every step is guarded with
-- OBJECT_ID(...) IS NOT NULL so it no-ops on a database provisioned without the
-- erp domain, where dim.Aftale does not exist. Safe to re-run.
--
-- The staging table needs no step here: stg_dim_Aftale_ils does DROP TABLE IF
-- EXISTS + SELECT INTO on every materialization, so it picks up the new column
-- set by itself.
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'standard_leveringsdage') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [standard_leveringsdage] INT NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'dw_ek_Dato_sysdato') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [dw_ek_Dato_sysdato] INT NOT NULL
        CONSTRAINT [DF_Aftale_dw_ek_Dato_sysdato] DEFAULT -1;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'dato_tid_sysdato') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [dato_tid_sysdato] DATETIME2 NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'bruger') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [bruger] NVARCHAR(100) NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'aktiv') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [aktiv] BIT NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'fragt_beloeb') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [fragt_beloeb] DECIMAL(17,2) NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'sw_fragt') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [sw_fragt] BIT NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'gebyr_beloeb') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [gebyr_beloeb] DECIMAL(17,2) NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'sw_gebyr') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [sw_gebyr] BIT NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'graense_beloeb') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [graense_beloeb] DECIMAL(17,2) NULL;
GO
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Aftale]', 'fragt_graense_beloeb') IS NULL
    ALTER TABLE [dim].[Aftale] ADD [fragt_graense_beloeb] DECIMAL(17,2) NULL;
GO
