-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Backfill DW_Timestamp_Update on existing rows where it is NULL.
--
-- The Postering and Finanskladde update assets previously left
-- DW_Timestamp_Update NULL on first INSERT. The new convention
-- (per sql_etl_guide.md) is "Timestamp of last update — equals Insert on
-- first insert", and the asset code now sets both columns on INSERT.
--
-- For rows already in the database with NULL DW_Timestamp_Update, set
-- DW_Timestamp_Update = DW_Timestamp_Insert so they look like never-updated
-- inserts under the new convention.
--
-- Safe to re-run: WHERE DW_Timestamp_Update IS NULL guards every UPDATE.
-- Apply to: staging database. A matching migration exists under migrations/dso/.
-- Task: #27805
-- ============================================================================

-- ---- map.Postering ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND type = 'U')
BEGIN
    UPDATE [map].[Postering]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on map.Postering: ', @@ROWCOUNT, ' rows');
END
GO

-- ---- dim.Postering ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND type = 'U')
BEGIN
    UPDATE [dim].[Postering]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on dim.Postering: ', @@ROWCOUNT, ' rows');
END
GO

-- ---- fct.Postering ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
BEGIN
    UPDATE [fct].[Postering]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on fct.Postering: ', @@ROWCOUNT, ' rows');
END
GO

-- ---- map.Finanskladde / map.Finanskladdelinje ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]') AND type = 'U')
BEGIN
    UPDATE [map].[Finanskladde]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on map.Finanskladde: ', @@ROWCOUNT, ' rows');
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND type = 'U')
BEGIN
    UPDATE [map].[Finanskladdelinje]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on map.Finanskladdelinje: ', @@ROWCOUNT, ' rows');
END
GO

-- ---- dim.Finanskladde / dim.Finanskladdelinje ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND type = 'U')
BEGIN
    UPDATE [dim].[Finanskladde]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on dim.Finanskladde: ', @@ROWCOUNT, ' rows');
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND type = 'U')
BEGIN
    UPDATE [dim].[Finanskladdelinje]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on dim.Finanskladdelinje: ', @@ROWCOUNT, ' rows');
END
GO

-- ---- fct.Finanskladde / fct.Finanskladdelinje ----

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Finanskladde]') AND type = 'U')
BEGIN
    UPDATE [fct].[Finanskladde]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on fct.Finanskladde: ', @@ROWCOUNT, ' rows');
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND type = 'U')
BEGIN
    UPDATE [fct].[Finanskladdelinje]
    SET [DW_Timestamp_Update] = [DW_Timestamp_Insert]
    WHERE [DW_Timestamp_Update] IS NULL;
    PRINT CONCAT('Backfilled DW_Timestamp_Update on fct.Finanskladdelinje: ', @@ROWCOUNT, ' rows');
END
GO
