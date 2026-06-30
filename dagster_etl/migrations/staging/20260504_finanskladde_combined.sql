-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Finanskladde / Finanskladdelinje migration (combined)
--
-- Apply to: staging database. A matching combined migration exists under
-- migrations/dso/.
-- Task: #27805
--
-- Run order (each section is idempotent and guarded):
--   1. Widen NVARCHAR cols on dim.Finanskladde / dim.Finanskladdelinje and
--      seed UNKNOWN on the -1 sentinel rows.
--   2. Drop CleanCode_* columns from fct.Finanskladde / fct.Finanskladdelinje.
--   3. Backfill DW_Timestamp_Update = DW_Timestamp_Insert on Postering and
--      Finanskladde map/dim/fct rows where Update is NULL.
--   4. Add DW_EK_Dato_Oprettet / DW_EK_Dato_Aendret to dim.Finanskladde.
--   5. Add DW_EK_Finanskladdelinje (+ index) to fct.Postering.
-- ============================================================================


-- ============================================================================
-- 1. Widen dim columns and seed UNKNOWN on -1 sentinel rows
--    (was: 20260429_finanskladde_widen_dim_columns_and_seed_unknown.sql)
-- ============================================================================

-- ---- dim.Finanskladde: widen DataAreaId / Maskinnummer ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
      AND name = 'DataAreaId'
      AND max_length < 40  -- nvarchar(20) = 40 bytes
)
BEGIN
    ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [DataAreaId] NVARCHAR(20) NULL;
    PRINT 'Widened dim.Finanskladde.DataAreaId to NVARCHAR(20)';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
      AND name = 'Maskinnummer'
      AND max_length < 40
)
BEGIN
    ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [Maskinnummer] NVARCHAR(20) NULL;
    PRINT 'Widened dim.Finanskladde.Maskinnummer to NVARCHAR(20)';
END
GO

-- ---- dim.Finanskladdelinje: widen Maskinnummer ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]')
      AND name = 'Maskinnummer'
      AND max_length < 40
)
BEGIN
    ALTER TABLE [dim].[Finanskladdelinje] ALTER COLUMN [Maskinnummer] NVARCHAR(20) NULL;
    PRINT 'Widened dim.Finanskladdelinje.Maskinnummer to NVARCHAR(20)';
END
GO

-- ---- dim.Finanskladde: backfill UNKNOWN on the -1 sentinel row ----

IF EXISTS (
    SELECT 1 FROM [dim].[Finanskladde] WHERE [DW_EK_Finanskladde] = -1
)
BEGIN
    UPDATE [dim].[Finanskladde]
    SET [bk_ref_LedgerjournaltableId] = COALESCE([bk_ref_LedgerjournaltableId], N'UNKNOWN'),
        [Kladdenummer]                = COALESCE([Kladdenummer], N'UNKNOWN'),
        [Kladdetekst]                 = COALESCE([Kladdetekst], N'UNKNOWN'),
        [Kladdenavn]                  = COALESCE([Kladdenavn], N'UNKNOWN'),
        [DataAreaId]                  = COALESCE([DataAreaId], N'UNKNOWN'),
        [Maskinnummer]                = COALESCE([Maskinnummer], N'UNKNOWN'),
        [DW_Timestamp_Update]         = CAST(SYSDATETIME() AS DATETIME2)
    WHERE [DW_EK_Finanskladde] = -1
      AND (
            [bk_ref_LedgerjournaltableId] IS NULL
         OR [Kladdenummer] IS NULL
         OR [Kladdetekst] IS NULL
         OR [Kladdenavn] IS NULL
         OR [DataAreaId] IS NULL
         OR [Maskinnummer] IS NULL
      );
    PRINT 'Seeded UNKNOWN values on dim.Finanskladde -1 sentinel row';
END
GO

-- ---- dim.Finanskladdelinje: backfill UNKNOWN on the -1 sentinel row ----

IF EXISTS (
    SELECT 1 FROM [dim].[Finanskladdelinje] WHERE [DW_EK_Finanskladdelinje] = -1
)
BEGIN
    UPDATE [dim].[Finanskladdelinje]
    SET [bk_ref_LedgertransId]    = COALESCE([bk_ref_LedgertransId], N'UNKNOWN'),
        [Beskrivelse]              = COALESCE([Beskrivelse], N'UNKNOWN'),
        [Ekspeditionsnummer]       = COALESCE([Ekspeditionsnummer], N'UNKNOWN'),
        [Transaktionstypetekst]    = COALESCE([Transaktionstypetekst], N'UNKNOWN'),
        [Maskinnummer]             = COALESCE([Maskinnummer], N'UNKNOWN'),
        [DW_Timestamp_Update]      = CAST(SYSDATETIME() AS DATETIME2)
    WHERE [DW_EK_Finanskladdelinje] = -1
      AND (
            [bk_ref_LedgertransId] IS NULL
         OR [Beskrivelse] IS NULL
         OR [Ekspeditionsnummer] IS NULL
         OR [Transaktionstypetekst] IS NULL
         OR [Maskinnummer] IS NULL
      );
    PRINT 'Seeded UNKNOWN values on dim.Finanskladdelinje -1 sentinel row';
END
GO


-- ============================================================================
-- 2. Drop CleanCode_* columns from fct.Finanskladde / fct.Finanskladdelinje
--    (was: 20260429_finanskladde_drop_cleancode_from_facts.sql)
-- ============================================================================

-- ---- fct.Finanskladde ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Finanskladde]')
      AND name = 'CleanCode_Finanskladde'
)
BEGIN
    DECLARE @sql_fk NVARCHAR(MAX) = N'';
    SELECT @sql_fk = @sql_fk + 'DROP INDEX [' + i.name + '] ON [fct].[Finanskladde]; '
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fct].[Finanskladde]')
      AND c.name = 'CleanCode_Finanskladde'
      AND i.is_primary_key = 0;
    IF LEN(@sql_fk) > 0 EXEC sp_executesql @sql_fk;

    ALTER TABLE [fct].[Finanskladde] DROP COLUMN [CleanCode_Finanskladde];
    PRINT 'Dropped CleanCode_Finanskladde from fct.Finanskladde';
END
GO

-- ---- fct.Finanskladdelinje ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
      AND name = 'CleanCode_Finanskladdelinje'
)
BEGIN
    DECLARE @sql_fkl NVARCHAR(MAX) = N'';
    SELECT @sql_fkl = @sql_fkl + 'DROP INDEX [' + i.name + '] ON [fct].[Finanskladdelinje]; '
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
      AND c.name = 'CleanCode_Finanskladdelinje'
      AND i.is_primary_key = 0;
    IF LEN(@sql_fkl) > 0 EXEC sp_executesql @sql_fkl;

    ALTER TABLE [fct].[Finanskladdelinje] DROP COLUMN [CleanCode_Finanskladdelinje];
    PRINT 'Dropped CleanCode_Finanskladdelinje from fct.Finanskladdelinje';
END
GO


-- ============================================================================
-- 3. Backfill DW_Timestamp_Update on Postering / Finanskladde rows where NULL
--    (was: 20260429_backfill_dw_timestamp_update_postering_finanskladde.sql)
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


-- ============================================================================
-- 4. Add DW_EK_Dato_Oprettet / DW_EK_Dato_Aendret to dim.Finanskladde
--    (was: 20260504_dim_finanskladde_add_date_columns.sql)
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND name = 'DW_EK_Dato_Oprettet')
BEGIN
    ALTER TABLE [dim].[Finanskladde] ADD [DW_EK_Dato_Oprettet] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Dato_Oprettet column to dim.Finanskladde';
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND name = 'DW_EK_Dato_Aendret')
BEGIN
    ALTER TABLE [dim].[Finanskladde] ADD [DW_EK_Dato_Aendret] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Dato_Aendret column to dim.Finanskladde';
END
GO


-- ============================================================================
-- 5. Add DW_EK_Finanskladdelinje (+ index) to fct.Postering
--    (was: 20260504_fct_postering_add_finanskladdelinje.sql)
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Finanskladdelinje')
BEGIN
    ALTER TABLE [fct].[Postering] ADD [DW_EK_Finanskladdelinje] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Finanskladdelinje column to fct.Postering';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Finanskladdelinje')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Finanskladdelinje]
    ON [fct].[Postering] ([DW_EK_Finanskladdelinje] ASC) ON [PRIMARY];
    PRINT 'Created IX_Fct_Postering_Finanskladdelinje index on fct.Postering';
END
GO
