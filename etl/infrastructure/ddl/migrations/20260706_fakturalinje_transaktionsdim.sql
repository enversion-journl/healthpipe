-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Fakturalinje transaktionsdimension ([internal ticket], Task 4)
--
-- fct.Fakturalinje.DW_EK_Fakturalinje was a local, independent IDENTITY(1,1) —
-- NOT derived from map.Fakturalinje like dim.Fakturalinje is. Both tables share
-- the same grain (CleanCode_Fakturalinje, from the same MySupply staging
-- source), but had two unrelated key sequences. Any report joining
-- vw_fct.Fakturalinje to vw_dim.Fakturalinje on DW_EK_Fakturalinje got
-- coincidental, not real, matches.
--
-- This migration converts fct.Fakturalinje to the transaction-dim pattern
-- (mirrors fct.Faktura / dim.Faktura): DW_EK_Fakturalinje is replaced with the
-- value from map.Fakturalinje, resolved via CleanCode_Fakturalinje.
--
-- Also renames the shared key to the new lowercase `dw_ek_` naming convention
-- (docs/planning/navngivningsstandard.md) across all three tables that make up
-- this key (map.Fakturalinje, dim.Fakturalinje, fct.Fakturalinje) — this is the
-- key being actively touched by this migration, so it moves to the new casing
-- now rather than being left for a separate historical case-rename branch.
--
-- NOTE: fct.Fakturalinje.dw_ek_Fakturalinje values WILL CHANGE (new key, and
-- renamed from DW_EK_Fakturalinje). Any external consumer relying on the old
-- IDENTITY values or the old uppercase column name must rejoin via
-- CleanCode_Fakturalinje / update column references after this migration runs.
--
-- Idempotent: guarded by sys.columns / sys.indexes checks, same pattern as
-- 20260504_01_vareudlevering-egenskaber-fk.sql (Section B).
-- ============================================================================

-- ---------- A: map.Fakturalinje — rename DW_EK_Fakturalinje -> dw_ek_Fakturalinje ----------
-- Pure rename (IDENTITY column, values unchanged); index follows the column
-- automatically via sp_rename.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]')
      AND name = N'DW_EK_Fakturalinje'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]')
      AND name = N'dw_ek_Fakturalinje'
)
BEGIN
    EXEC sp_rename
        N'[map].[Fakturalinje].[DW_EK_Fakturalinje]',
        N'dw_ek_Fakturalinje',
        N'COLUMN';
    PRINT 'Renamed map.Fakturalinje.DW_EK_Fakturalinje -> dw_ek_Fakturalinje';
END
GO

-- ---------- B: dim.Fakturalinje — rename DW_EK_Fakturalinje -> dw_ek_Fakturalinje ----------
-- Pure rename (PK column, values unchanged); PK constraint follows the column
-- automatically via sp_rename.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
      AND name = N'DW_EK_Fakturalinje'
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
      AND name = N'dw_ek_Fakturalinje'
)
BEGIN
    EXEC sp_rename
        N'[dim].[Fakturalinje].[DW_EK_Fakturalinje]',
        N'dw_ek_Fakturalinje',
        N'COLUMN';
    PRINT 'Renamed dim.Fakturalinje.DW_EK_Fakturalinje -> dw_ek_Fakturalinje';
END
GO

-- ---------- C.1: fct.Fakturalinje — add transitional column for the new PK value ----------
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = N'DW_EK_Fakturalinje'
      AND is_identity = 1
)
AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = N'dw_ek_Fakturalinje_new'
)
BEGIN
    ALTER TABLE [fct].[Fakturalinje] ADD [dw_ek_Fakturalinje_new] INT NULL;
    PRINT 'Added transitional column dw_ek_Fakturalinje_new INT NULL';
END
GO

-- ---------- C.2: Backfill the new PK from map.Fakturalinje via CleanCode ----------
-- Dynamic SQL: dw_ek_Fakturalinje_new is renamed by C.4, so static DML would
-- fail compile-time validation on re-run.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = N'dw_ek_Fakturalinje_new'
)
BEGIN
    EXEC(N'UPDATE f
    SET f.dw_ek_Fakturalinje_new = m.dw_ek_Fakturalinje
    FROM [fct].[Fakturalinje] f
    INNER JOIN [map].[Fakturalinje] m
        ON m.CleanCode_Fakturalinje = f.CleanCode_Fakturalinje
    WHERE f.dw_ek_Fakturalinje_new IS NULL');
    PRINT 'Backfilled dw_ek_Fakturalinje_new from map.Fakturalinje';
END
GO

-- ---------- C.3: Verify backfill is complete (any unmapped row blocks the swap) ----------
-- Dynamic SQL: dw_ek_Fakturalinje_new is renamed by C.4, so static DML would
-- fail compile-time validation on re-run.
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = N'dw_ek_Fakturalinje_new'
)
BEGIN
    DECLARE @nulls INT;
    EXEC sp_executesql
        N'SELECT @n = COUNT(*) FROM [fct].[Fakturalinje] WHERE [dw_ek_Fakturalinje_new] IS NULL',
        N'@n INT OUTPUT',
        @n = @nulls OUTPUT;
    IF @nulls > 0
    BEGIN
        DECLARE @msg NVARCHAR(400) = CONCAT(
            'fct.Fakturalinje: ', @nulls,
            ' rows have no match in map.Fakturalinje on CleanCode_Fakturalinje — ',
            'populate map_fakturalinje first (run map_fakturalinje asset), then re-run this migration.'
        );
        THROW 50000, @msg, 1;
    END
END
GO

-- ---------- C.4: Drop old IDENTITY PK, promote the new column into its place ----------
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND name = N'DW_EK_Fakturalinje'
      AND is_identity = 1
)
BEGIN
    DECLARE @pk_name NVARCHAR(128) = (
        SELECT name FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
          AND type = 'PK'
    );
    IF @pk_name IS NOT NULL
    BEGIN
        DECLARE @drop_pk_sql NVARCHAR(400) =
            N'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT ' + QUOTENAME(@pk_name);
        EXEC sp_executesql @drop_pk_sql;
    END

    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [DW_EK_Fakturalinje];
    EXEC sp_rename
        N'[fct].[Fakturalinje].[dw_ek_Fakturalinje_new]',
        N'dw_ek_Fakturalinje',
        N'COLUMN';
    ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [dw_ek_Fakturalinje] INT NOT NULL;
    ALTER TABLE [fct].[Fakturalinje]
        ADD CONSTRAINT [PK_Fct_Fakturalinje]
        PRIMARY KEY CLUSTERED ([dw_ek_Fakturalinje] ASC);
    PRINT 'Replaced fct.Fakturalinje.DW_EK_Fakturalinje IDENTITY with dw_ek_Fakturalinje INT (from map.Fakturalinje)';
END
GO
