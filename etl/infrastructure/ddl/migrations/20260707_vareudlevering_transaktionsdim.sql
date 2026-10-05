-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Vareudlevering transaktionsdimension — afslut navngivning ([internal ticket], Task 3)
--
-- map.Vareudlevering has been shared between dim and fct since migration
-- 20260504_01_vareudlevering-egenskaber-fk.sql, but the naming was never
-- finished: the dimension was still called dim.VareudleveringEgenskaber, and
-- fct.Vareudlevering carried a redundant DW_EK_VareudleveringEgenskaber FK
-- column that was always identical to its own PK (DW_EK_Vareudlevering).
--
-- This migration:
--   A. Renames map.Vareudlevering.DW_EK_Vareudlevering -> dw_ek_Vareudlevering
--   B. Renames dim.VareudleveringEgenskaber -> dim.Vareudlevering (table,
--      PK/CleanCode columns, PK constraint, filtered unique index)
--   C. Renames fct.Vareudlevering.DW_EK_Vareudlevering -> dw_ek_Vareudlevering
--   D. Drops the redundant fct.Vareudlevering.DW_EK_VareudleveringEgenskaber
--      FK column and its supporting index
--   E. Renames the dim checkpoint entry in meta.dfm_state / meta.dfm_run_log
--      (VareudleveringEgenskaber -> Vareudlevering) so existing watermark
--      checkpoints survive the rename — same pattern as
--      20260630_rename_ils_lagerbeholdning_tables.sql
--
-- All three tables' shared key (map/dim/fct) move to the new lowercase
-- `dw_ek_` naming convention (docs/planning/navngivningsstandard.md) since
-- this is the key being actively touched by this migration — consistent with
-- 20260706_fakturalinje_transaktionsdim.sql.
--
-- NOTE: No data backfill needed — fct.Vareudlevering.DW_EK_Vareudlevering has
-- been sourced from map.Vareudlevering since 20260504_27574 (pure renames
-- only, values unchanged). Only the redundant FK column is dropped.
--
-- Idempotent: guarded by sys.objects / sys.columns / sys.indexes checks.
-- ============================================================================

-- ---------- A: map.Vareudlevering — rename DW_EK_Vareudlevering -> dw_ek_Vareudlevering ----------
-- Pure rename (IDENTITY column, values unchanged); non-filtered index follows
-- the column automatically via sp_rename.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]')
      AND name = N'DW_EK_Vareudlevering'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]')
      AND name = N'dw_ek_Vareudlevering'
)
BEGIN
    EXEC sp_rename
        N'[map].[Vareudlevering].[DW_EK_Vareudlevering]',
        N'dw_ek_Vareudlevering',
        N'COLUMN';
    PRINT 'Renamed map.Vareudlevering.DW_EK_Vareudlevering -> dw_ek_Vareudlevering';
END
GO

-- ---------- B.1: Rename table dim.VareudleveringEgenskaber -> dim.Vareudlevering ----------
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[VareudleveringEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND type = 'U')
BEGIN
    EXEC sp_rename N'dim.VareudleveringEgenskaber', N'Vareudlevering';
    PRINT 'Renamed dim.VareudleveringEgenskaber -> dim.Vareudlevering';
END
GO

-- ---------- B.2: Rename PK column DW_EK_VareudleveringEgenskaber -> dw_ek_Vareudlevering ----------
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = 'DW_EK_VareudleveringEgenskaber')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = 'dw_ek_Vareudlevering')
BEGIN
    EXEC sp_rename N'dim.Vareudlevering.DW_EK_VareudleveringEgenskaber', N'dw_ek_Vareudlevering', N'COLUMN';
    PRINT 'Renamed dim.Vareudlevering.DW_EK_VareudleveringEgenskaber -> dw_ek_Vareudlevering';
END
GO

-- ---------- B.3: Rename PK constraint PK_Dim_VareudleveringEgenskaber -> PK_Dim_Vareudlevering ----------
IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_Dim_VareudleveringEgenskaber' AND parent_object_id = OBJECT_ID(N'[dim].[Vareudlevering]'))
BEGIN
    EXEC sp_rename N'dim.PK_Dim_VareudleveringEgenskaber', N'PK_Dim_Vareudlevering', N'OBJECT';
    PRINT 'Renamed PK constraint -> PK_Dim_Vareudlevering';
END
GO

-- ---------- B.4: Rename CleanCode_VareudleveringEgenskaber -> CleanCode_Vareudlevering ----------
-- The filtered unique index references this column in its WHERE clause;
-- sp_rename does not propagate to filtered index expressions, so drop the
-- index first and recreate it in B.5 (same approach as 20260504_27574).
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = 'CleanCode_VareudleveringEgenskaber')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = 'CleanCode_Vareudlevering')
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = 'IX_Dim_VareudleveringEgenskaber_CleanCode')
    BEGIN
        DROP INDEX [IX_Dim_VareudleveringEgenskaber_CleanCode] ON [dim].[Vareudlevering];
        PRINT 'Dropped IX_Dim_VareudleveringEgenskaber_CleanCode for column rename';
    END

    EXEC sp_rename N'dim.Vareudlevering.CleanCode_VareudleveringEgenskaber', N'CleanCode_Vareudlevering', N'COLUMN';
    PRINT 'Renamed dim.Vareudlevering.CleanCode_VareudleveringEgenskaber -> CleanCode_Vareudlevering';
END
GO

-- ---------- B.5: Recreate the filtered unique index under its new name ----------
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = 'CleanCode_Vareudlevering')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = 'IX_Dim_Vareudlevering_CleanCode')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_Vareudlevering_CleanCode]
        ON [dim].[Vareudlevering] ([CleanCode_Vareudlevering] ASC)
        WHERE [CleanCode_Vareudlevering] IS NOT NULL
        ON [PRIMARY];
    PRINT 'Recreated IX_Dim_Vareudlevering_CleanCode on renamed column';
END
GO

-- ---------- C: fct.Vareudlevering — rename DW_EK_Vareudlevering -> dw_ek_Vareudlevering ----------
-- Pure rename (already sourced from map.Vareudlevering since 20260504_27574,
-- values unchanged); PK constraint follows the column automatically via
-- sp_rename.
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'DW_EK_Vareudlevering')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'dw_ek_Vareudlevering')
BEGIN
    EXEC sp_rename N'fct.Vareudlevering.DW_EK_Vareudlevering', N'dw_ek_Vareudlevering', N'COLUMN';
    PRINT 'Renamed fct.Vareudlevering.DW_EK_Vareudlevering -> dw_ek_Vareudlevering';
END
GO

-- ---------- D: fct.Vareudlevering — drop the redundant Egenskaber FK column ----------
-- Always identical to the fact's own PK (both sourced from map.Vareudlevering)
-- — no longer needed now that dim.Vareudlevering shares the PK directly.
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'IX_Fct_Vareudlevering_VareudleveringEgenskaber')
BEGIN
    DROP INDEX [IX_Fct_Vareudlevering_VareudleveringEgenskaber] ON [fct].[Vareudlevering];
    PRINT 'Dropped IX_Fct_Vareudlevering_VareudleveringEgenskaber';
END
GO

-- The column was added NOT NULL DEFAULT -1 by 20260504_27574, so its
-- auto-named DEFAULT constraint must be dropped before the column can be.
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = 'DW_EK_VareudleveringEgenskaber')
BEGIN
    DECLARE @df_name NVARCHAR(128) = (
        SELECT dc.name
        FROM sys.default_constraints dc
        JOIN sys.columns c
            ON c.object_id = dc.parent_object_id
           AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
          AND c.name = 'DW_EK_VareudleveringEgenskaber'
    );
    IF @df_name IS NOT NULL
    BEGIN
        DECLARE @drop_df NVARCHAR(400) =
            N'ALTER TABLE [fct].[Vareudlevering] DROP CONSTRAINT ' + QUOTENAME(@df_name);
        EXEC sp_executesql @drop_df;
        PRINT 'Dropped default constraint on DW_EK_VareudleveringEgenskaber';
    END

    ALTER TABLE [fct].[Vareudlevering] DROP COLUMN [DW_EK_VareudleveringEgenskaber];
    PRINT 'Dropped fct.Vareudlevering.DW_EK_VareudleveringEgenskaber (redundant FK)';
END
GO

-- ---------- E: DFM checkpoint continuity — rename dim checkpoint entries ----------
-- Exact-match updates (schema_name + table_name), idempotent: re-running
-- matches 0 rows once renamed. Same pattern as
-- 20260630_rename_ils_lagerbeholdning_tables.sql.
IF OBJECT_ID(N'[meta].[dfm_state]', N'U') IS NOT NULL
BEGIN
    UPDATE [meta].[dfm_state]
    SET [table_name] = 'Vareudlevering'
    WHERE [schema_name] = 'dim' AND [table_name] = 'VareudleveringEgenskaber';
END
GO

IF OBJECT_ID(N'[meta].[dfm_run_log]', N'U') IS NOT NULL
BEGIN
    UPDATE [meta].[dfm_run_log]
    SET [table_name] = 'Vareudlevering'
    WHERE [schema_name] = 'dim' AND [table_name] = 'VareudleveringEgenskaber';
END
GO
