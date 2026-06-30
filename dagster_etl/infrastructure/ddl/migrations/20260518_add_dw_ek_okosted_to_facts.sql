-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Add DW_EK_Okosted column to fct.Postering, fct.Budget, fct.Finanskladdelinje
-- Part of the dim.Okosted extraction from dim.Organisation.
--
-- Upgrade path for EXISTING databases only. On a fresh DB the base table DDL
-- (tables/erp/03_facts.sql) already declares DW_EK_Okosted, and migrations run
-- BEFORE the erp tables are created (staging_infra_erp deps on staging_migrations),
-- so each step is guarded with OBJECT_ID(...) IS NOT NULL to no-op when the
-- table does not yet exist.
-- Safe to re-run: every step is guarded.
-- ============================================================================

-- fct.Postering
IF OBJECT_ID(N'[fct].[Postering]') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Okosted'
   )
BEGIN
    ALTER TABLE [fct].[Postering] ADD [DW_EK_Okosted] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Okosted to fct.Postering';
END
GO

-- fct.Budget
IF OBJECT_ID(N'[fct].[Budget]') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'DW_EK_Okosted'
   )
BEGIN
    ALTER TABLE [fct].[Budget] ADD [DW_EK_Okosted] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Okosted to fct.Budget';
END
GO

-- fct.Finanskladdelinje
IF OBJECT_ID(N'[fct].[Finanskladdelinje]') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = 'DW_EK_Okosted'
   )
BEGIN
    ALTER TABLE [fct].[Finanskladdelinje] ADD [DW_EK_Okosted] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Okosted to fct.Finanskladdelinje';
END
GO
