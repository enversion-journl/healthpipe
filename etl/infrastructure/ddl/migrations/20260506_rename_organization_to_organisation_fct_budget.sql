-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename DW_EK_Organization → DW_EK_Organisation on fct.Budget.
--
-- The original rename migration (20260427_rename_organization_to_organisation)
-- covered dim, map, sec, fct.Fakturalinje, fct.Postering, and fct.Faktura but
-- missed fct.Budget (added later). On databases where fct.Budget was created
-- before the end-state DDL switched to DW_EK_Organisation, the column still
-- has the old spelling and the FctUpd_budget asset fails with:
--   Invalid object name 'map.Organization'
-- (map.Organization was already renamed to map.Organisation).
--
-- Also defensively covers fct.Lagerbeholdning and fct.AfdelingLagerbeholdning
-- in case they were created before the DDL was updated.
--
-- Safe to re-run: every step is guarded per sp_rename patterns in
-- sql_etl_guide.md.
-- ============================================================================

-- ---- fct.Budget: rename DW_EK_Organization → DW_EK_Organisation ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[fct].[Budget].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Budget';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Fct_Budget_Organization' AND object_id = OBJECT_ID(N'[fct].[Budget]'))
BEGIN
    EXEC sp_rename N'[fct].[Budget].[IX_Fct_Budget_Organization]', N'IX_Fct_Budget_Organisation', N'INDEX';
    PRINT 'Renamed index IX_Fct_Budget_Organization -> IX_Fct_Budget_Organisation on fct.Budget';
END
GO

-- Handle dual-column state: if both old and new columns coexist, drop the old one
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'DW_EK_Organization')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = 'DW_EK_Organisation')
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_Fct_Budget_Organization' AND object_id = OBJECT_ID(N'[fct].[Budget]'))
        DROP INDEX [IX_Fct_Budget_Organization] ON [fct].[Budget];

    DECLARE @df_budget NVARCHAR(200);
    SELECT @df_budget = dc.name
      FROM sys.default_constraints dc
      JOIN sys.columns c ON c.default_object_id = dc.object_id
     WHERE c.object_id = OBJECT_ID(N'[fct].[Budget]')
       AND c.name = N'DW_EK_Organization';
    IF @df_budget IS NOT NULL
        EXEC(N'ALTER TABLE [fct].[Budget] DROP CONSTRAINT [' + @df_budget + N']');

    ALTER TABLE [fct].[Budget] DROP COLUMN [DW_EK_Organization];
    PRINT 'Dropped duplicate DW_EK_Organization from fct.Budget (DW_EK_Organisation already exists)';
END
GO

-- ---- fct.Lagerbeholdning: defensive rename ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[fct].[Lagerbeholdning].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Lagerbeholdning';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Fct_Lagerbeholdning_Organization' AND object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]'))
BEGIN
    EXEC sp_rename N'[fct].[Lagerbeholdning].[IX_Fct_Lagerbeholdning_Organization]', N'IX_Fct_Lagerbeholdning_Organisation', N'INDEX';
    PRINT 'Renamed index IX_Fct_Lagerbeholdning_Organization -> IX_Fct_Lagerbeholdning_Organisation on fct.Lagerbeholdning';
END
GO

-- ---- fct.AfdelingLagerbeholdning: defensive rename ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND name = 'DW_EK_Organisation')
BEGIN
    EXEC sp_rename '[fct].[AfdelingLagerbeholdning].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.AfdelingLagerbeholdning';
END
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Fct_AfdelingLagerbeholdning_Organization' AND object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]'))
BEGIN
    EXEC sp_rename N'[fct].[AfdelingLagerbeholdning].[IX_Fct_AfdelingLagerbeholdning_Organization]', N'IX_Fct_AfdelingLagerbeholdning_Organisation', N'INDEX';
    PRINT 'Renamed index IX_Fct_AfdelingLagerbeholdning_Organization -> IX_Fct_AfdelingLagerbeholdning_Organisation on fct.AfdelingLagerbeholdning';
END
GO
