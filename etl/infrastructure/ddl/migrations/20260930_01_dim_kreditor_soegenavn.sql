-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: soegenavn_sydax on dim.Kreditor
--
-- Test case TC-022 in the test feedback from DOL found the SydAX search name
-- missing from dim.Kreditor. Only SydAX carries the field ([Kreditor].[Kreditor].Soegenavn,
-- already archived 1:1 in arc.sydax_kreditor), so it lands as the per-source
-- column soegenavn_sydax with no COALESCE counterpart.
--
-- Runs automatically on BOTH staging and DSO via the central migrations runner.
-- Idempotent -- guarded on the column's presence. The CREATE TABLE in
-- tables/erp/02_dims.sql already declares the end state, so on a fresh install
-- the script is a no-op.
IF OBJECT_ID(N'[dim].[Kreditor]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Kreditor'
             AND COLUMN_NAME = 'soegenavn_sydax')
BEGIN
    ALTER TABLE [dim].[Kreditor] ADD [soegenavn_sydax] NVARCHAR(255) NULL;
    PRINT 'Added dim.Kreditor.soegenavn_sydax';
END
GO

-- dim_Kreditor is delta-driven off map.Kreditor's dw_timestamp_update, so
-- existing rows would keep soegenavn_sydax NULL until their CVR next drifts.
-- Clearing the staging-scope dim checkpoint makes the next run reprocess every
-- map row and backfill the column. A missing row is a no-op with the same
-- outcome (a checkpoint-less run is a full reprocess). The scope/target_type
-- filter matters: the same staging table also holds the DSO sync watermark row
-- ('dso', 'dimension', 'dim', 'Kreditor'), which must survive. The DSO
-- database has its own (unused) meta.DFMState, where the DELETE matches
-- nothing.
IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
    DELETE FROM [meta].[DFMState]
    WHERE [scope] = 'staging' AND [target_type] = 'dimension'
      AND [schema_name] = 'dim' AND [table_name] = 'Kreditor';
GO
