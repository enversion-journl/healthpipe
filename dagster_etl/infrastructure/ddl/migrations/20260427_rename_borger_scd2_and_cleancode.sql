-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename SCD2 columns and CleanCode in dim.Borger and map.Borger.
--
-- dim.Borger:  ValidFrom -> DW_ValidFrom, ValidTo -> DW_ValidTo,
--              IsCurrent -> DW_IsCurrent, Borger_CleanCode -> CleanCode_Borger
-- map.Borger:  Borger_CleanCode -> CleanCode_Borger
-- Re-adds named DEFAULT constraint DF_Borger_DW_IsCurrent on the renamed column
-- (replaces the previous auto-named DEFAULT so fresh-deploy and migrated-deploy
-- converge).
-- Drops the dependent view/function so the underlying columns can be renamed;
-- recreation is handled by the end-state DDL files which run after this
-- migration (CREATE OR ALTER on each):
--   - infrastructure/ddl/views/security/01_vw_dim_security.sql       (vw_dim.Borger)
--   - infrastructure/ddl/common/functions/GetBorgerSK_TVF.sql        (dbo.GetBorgerSK_TVF)
-- Safe to re-run.
-- Task: #27722
-- ============================================================================

-- ---- 1. Drop dependents ----

IF OBJECT_ID(N'[vw_dim].[Borger]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_dim].[Borger];
    PRINT 'Dropped view vw_dim.Borger';
END
GO

IF OBJECT_ID(N'[dbo].[GetBorgerSK_TVF]', 'IF') IS NOT NULL
BEGIN
    DROP FUNCTION [dbo].[GetBorgerSK_TVF];
    PRINT 'Dropped function dbo.GetBorgerSK_TVF';
END
GO

-- ---- 2. Drop the auto-named DEFAULT on dim.Borger.IsCurrent (or DW_IsCurrent if re-run) ----

DECLARE @DefaultName NVARCHAR(256);
SELECT @DefaultName = dc.name
FROM sys.default_constraints dc
INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
WHERE c.object_id = OBJECT_ID(N'[dim].[Borger]')
  AND c.name IN (N'IsCurrent', N'DW_IsCurrent');

IF @DefaultName IS NOT NULL AND @DefaultName <> 'DF_Borger_DW_IsCurrent'
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'ALTER TABLE [dim].[Borger] DROP CONSTRAINT [' + @DefaultName + N']';
    EXEC sp_executesql @sql;
    PRINT 'Dropped auto-named DEFAULT constraint ' + @DefaultName + ' on dim.Borger.IsCurrent';
END
GO

-- ---- 3. Rename dim.Borger columns ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = 'ValidFrom')
BEGIN
    EXEC sp_rename '[dim].[Borger].[ValidFrom]', 'DW_ValidFrom', 'COLUMN';
    PRINT 'Renamed ValidFrom -> DW_ValidFrom on dim.Borger';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = 'ValidTo')
BEGIN
    EXEC sp_rename '[dim].[Borger].[ValidTo]', 'DW_ValidTo', 'COLUMN';
    PRINT 'Renamed ValidTo -> DW_ValidTo on dim.Borger';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = 'IsCurrent')
BEGIN
    -- Legacy filtered unique index from the deleted kraeftpakker infrastructure
    -- (PR 10485 removed the asset code but not the index from existing servers).
    -- Its WHERE clause references [IsCurrent]; sp_rename does not propagate to
    -- filtered-index expressions, so we must drop it before the rename. The
    -- end-state schema in tables/shared_dimensions/02_dims.sql does not include
    -- this index, so it is not recreated.
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
          AND name = N'IX_Dim_Borger_EK_Current'
    )
    BEGIN
        DROP INDEX [IX_Dim_Borger_EK_Current] ON [dim].[Borger];
        PRINT 'Dropped legacy IX_Dim_Borger_EK_Current for IsCurrent rename';
    END

    EXEC sp_rename '[dim].[Borger].[IsCurrent]', 'DW_IsCurrent', 'COLUMN';
    PRINT 'Renamed IsCurrent -> DW_IsCurrent on dim.Borger';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = 'Borger_CleanCode')
BEGIN
    EXEC sp_rename '[dim].[Borger].[Borger_CleanCode]', 'CleanCode_Borger', 'COLUMN';
    PRINT 'Renamed Borger_CleanCode -> CleanCode_Borger on dim.Borger';
END
GO

-- ---- 4. Rename map.Borger column ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Borger]') AND name = 'Borger_CleanCode')
BEGIN
    EXEC sp_rename '[map].[Borger].[Borger_CleanCode]', 'CleanCode_Borger', 'COLUMN';
    PRINT 'Renamed Borger_CleanCode -> CleanCode_Borger on map.Borger';
END
GO

-- ---- 5. Re-add explicitly named DEFAULT on dim.Borger.DW_IsCurrent ----

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE name = 'DF_Borger_DW_IsCurrent')
BEGIN
    ALTER TABLE [dim].[Borger]
        ADD CONSTRAINT DF_Borger_DW_IsCurrent DEFAULT 1 FOR [DW_IsCurrent];
    PRINT 'Added named DEFAULT DF_Borger_DW_IsCurrent on dim.Borger.DW_IsCurrent';
END
GO
