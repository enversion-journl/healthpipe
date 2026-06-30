-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: rebuild 13 SydAX arc.* tables onto the history-preserving grain.
--
-- The merge key for these archives changed to include the StartDateTime version
-- discriminator (and, for several tables, switched the entity column to the
-- table's own surrogate id). The old grain kept only one row per entity, so the
-- correct way to populate the new grain is a full reload from source that
-- backfills every temporal version.
--
-- For each affected table: if it still has the OLD schema (StartDateTime is
-- nullable), DROP it so the domain DDL (00_archive.sql, which runs AFTER
-- migrations) recreates it with the new PK, and clear its dfm_state checkpoint
-- so the next ext.*_sydax_dfm extract runs a full first-run load.
--
-- Idempotent: once the table is recreated with StartDateTime NOT NULL the guard
-- is false, so re-runs are no-ops and never drop a populated, migrated table.
-- WARNING: this discards current arc.* contents for these tables on first run;
-- they are rebuilt from source by the SydAX archive load (run
-- full_infrastructure_job, then the SydAX ext + arc assets).

IF OBJECT_ID(N'[arc].[art_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[art_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[art_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'art_sydax';
END
GO

IF OBJECT_ID(N'[arc].[gruppering_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[gruppering_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[gruppering_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'gruppering_sydax';
END
GO

IF OBJECT_ID(N'[arc].[hovedkonto_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[hovedkonto_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[hovedkonto_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'hovedkonto_sydax';
END
GO

IF OBJECT_ID(N'[arc].[gruppering_autoriseret_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[gruppering_autoriseret_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[gruppering_autoriseret_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'gruppering_autoriseret_sydax';
END
GO

IF OBJECT_ID(N'[arc].[funktion_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[funktion_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[funktion_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'funktion_sydax';
END
GO

IF OBJECT_ID(N'[arc].[dranst_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[dranst_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[dranst_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'dranst_sydax';
END
GO

IF OBJECT_ID(N'[arc].[afdeling_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[afdeling_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[afdeling_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'afdeling_sydax';
END
GO

IF OBJECT_ID(N'[arc].[finanskonto_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[finanskonto_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[finanskonto_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'finanskonto_sydax';
END
GO

IF OBJECT_ID(N'[arc].[kreditor_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[kreditor_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[kreditor_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'kreditor_sydax';
END
GO

IF OBJECT_ID(N'[arc].[finanskladde_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[finanskladde_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[finanskladde_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'finanskladde_sydax';
END
GO

IF OBJECT_ID(N'[arc].[finanstransaktionstype_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[finanstransaktionstype_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[finanstransaktionstype_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'finanstransaktionstype_sydax';
END
GO

IF OBJECT_ID(N'[arc].[finanskonto_omkostningssted_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[finanskonto_omkostningssted_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[finanskonto_omkostningssted_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'finanskonto_omkostningssted_sydax';
END
GO

IF OBJECT_ID(N'[arc].[omkostningssted_hierarki_sydax]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[omkostningssted_hierarki_sydax]')
         AND name = 'StartDateTime'
         AND is_nullable = 1
   )
BEGIN
    DROP TABLE [arc].[omkostningssted_hierarki_sydax];
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging' AND [schema_name] = 'arc' AND [table_name] = 'omkostningssted_hierarki_sydax';
END
GO
