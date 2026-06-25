-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Cleanup: drop legacy/OKOSTED-era columns from dim.Organisation.
--
-- Schema-drift fix. 20260518_cleanup_okosted_from_organisation dropped these
-- columns, but 20260529_fix_missing_organisation_and_conceptmap_columns then
-- re-ADDed them (guarded by IF NOT EXISTS) while chasing a different drift.
-- They are not in the end-state DDL (tables/organisation/02_dims.sql), nothing
-- populates them (dim_organisation writes only Code/DW_EK_Org_Parent/ParentCode/
-- Name/Type/Active), and nothing reads them — dim.Organisation is SOR-only,
-- Type 1, sourced from fhir.Organization + ref.Organisation (US #28193).
--
-- Columns removed: SourceType, CodeSource, CodeSOR, DW_EK_Org_ParentSOR,
--                  DW_EK_Org_ParentSource, ParentSOR, ParentSource
--
-- Safe to re-run: every step is guarded; brings existing tables in line with
-- the already-correct end-state DDL (fresh DBs never get these columns).
-- ============================================================================

-- ============================================================================
-- STEP 1: Drop DEFAULT constraints on the target columns
-- (20260529 added SourceType/CodeSource with inline DEFAULT 'UNKNOWN', which
--  creates auto-named default constraints that block DROP COLUMN.)
-- ============================================================================
DECLARE @drop_defaults NVARCHAR(MAX) = N'';
SELECT @drop_defaults = @drop_defaults
    + N'ALTER TABLE [dim].[Organisation] DROP CONSTRAINT [' + dc.name + N'];' + CHAR(10)
FROM sys.default_constraints dc
JOIN sys.columns c
    ON c.object_id = dc.parent_object_id
   AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Organisation]')
  AND c.name IN (
      'SourceType', 'CodeSource', 'CodeSOR',
      'DW_EK_Org_ParentSOR', 'DW_EK_Org_ParentSource',
      'ParentSOR', 'ParentSource'
  );

IF @drop_defaults <> N''
BEGIN
    EXEC sp_executesql @drop_defaults;
    PRINT 'Dropped default constraints on legacy dim.Organisation columns';
END
GO

-- ============================================================================
-- STEP 2: Drop indexes that reference the legacy columns (block DROP COLUMN)
-- ============================================================================
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organisation_SourceType' AND object_id = OBJECT_ID(N'[dim].[Organisation]'))
    DROP INDEX IX_dim_Organisation_SourceType ON [dim].[Organisation];
GO
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organisation_CodeSource' AND object_id = OBJECT_ID(N'[dim].[Organisation]'))
    DROP INDEX IX_dim_Organisation_CodeSource ON [dim].[Organisation];
GO
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organisation_ParentSource' AND object_id = OBJECT_ID(N'[dim].[Organisation]'))
    DROP INDEX IX_dim_Organisation_ParentSource ON [dim].[Organisation];
GO
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dim_Organisation_ParentSOR' AND object_id = OBJECT_ID(N'[dim].[Organisation]'))
    DROP INDEX IX_dim_Organisation_ParentSOR ON [dim].[Organisation];
GO

-- ============================================================================
-- STEP 3: Drop the legacy columns
-- ============================================================================
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'SourceType')
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [SourceType];
    PRINT 'Dropped dim.Organisation.SourceType';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'CodeSource')
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [CodeSource];
    PRINT 'Dropped dim.Organisation.CodeSource';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'CodeSOR')
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [CodeSOR];
    PRINT 'Dropped dim.Organisation.CodeSOR';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSOR')
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [DW_EK_Org_ParentSOR];
    PRINT 'Dropped dim.Organisation.DW_EK_Org_ParentSOR';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'DW_EK_Org_ParentSource')
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [DW_EK_Org_ParentSource];
    PRINT 'Dropped dim.Organisation.DW_EK_Org_ParentSource';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'ParentSOR')
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [ParentSOR];
    PRINT 'Dropped dim.Organisation.ParentSOR';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = 'ParentSource')
BEGIN
    ALTER TABLE [dim].[Organisation] DROP COLUMN [ParentSource];
    PRINT 'Dropped dim.Organisation.ParentSource';
END
GO

-- ============================================================================
-- ROLLBACK (manual) — re-add the columns as NULLable (data is not restored):
--
--   ALTER TABLE [dim].[Organisation] ADD
--       [SourceType] VARCHAR(10) NULL, [CodeSource] VARCHAR(50) NULL,
--       [CodeSOR] VARCHAR(50) NULL, [DW_EK_Org_ParentSOR] INT NULL,
--       [DW_EK_Org_ParentSource] INT NULL, [ParentSOR] VARCHAR(50) NULL,
--       [ParentSource] VARCHAR(50) NULL;
-- ============================================================================
