-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Faktura remodel — fact table FK columns
--
-- fct.Faktura:      drop DW_EK_FakturaEgenskaber, add DW_EK_Postering
-- fct.Postering:    rename DW_EK_FakturaEgenskaber → DW_EK_Faktura + reset to -1
-- fct.Fakturalinje: drop DW_EK_FakturaEgenskaber (keeps existing DW_EK_Faktura)
--
-- Idempotent: all operations guarded. The fct.Postering reset is inside the
-- rename guard so a re-run (after rows are healed) does NOT re-nullify.
-- ============================================================================

-- ---------- fct.Faktura: drop DW_EK_FakturaEgenskaber (default-safe) ----------
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_FakturaEgenskaber')
BEGIN
    DECLARE @df_faktura sysname;
    SELECT @df_faktura = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]') AND c.name = 'DW_EK_FakturaEgenskaber';
    IF @df_faktura IS NOT NULL EXEC('ALTER TABLE [fct].[Faktura] DROP CONSTRAINT [' + @df_faktura + ']');
    ALTER TABLE [fct].[Faktura] DROP COLUMN [DW_EK_FakturaEgenskaber];
    PRINT 'Dropped fct.Faktura.DW_EK_FakturaEgenskaber (+ default constraint)';
END
GO

-- ---------- fct.Faktura: add DW_EK_Postering ----------
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Postering')
    ALTER TABLE [fct].[Faktura] ADD [DW_EK_Postering] INT NOT NULL CONSTRAINT [DF_Fct_Faktura_DW_EK_Postering] DEFAULT -1;
GO

-- ---------- fct.Postering: rename (separate batch from reset) ----------
-- sp_rename + UPDATE must live in separate GO batches because the mssql_python
-- driver does not reliably resolve the new column name within the same batch,
-- even via EXEC(). A temp-table flag carries the "rename just happened" signal
-- into the next batch so the reset only fires once.
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_FakturaEgenskaber')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Faktura')
BEGIN
    EXEC sp_rename N'fct.Postering.DW_EK_FakturaEgenskaber', N'DW_EK_Faktura', N'COLUMN';
    CREATE TABLE #faktura_remodel_did_rename (flag BIT);
    INSERT INTO #faktura_remodel_did_rename VALUES (1);
    PRINT 'Renamed fct.Postering.DW_EK_FakturaEgenskaber -> DW_EK_Faktura';
END
GO

-- ---------- fct.Postering: one-time reset to -1 (only after rename) ----------
-- The temp-table flag ensures this fires exactly once: when the rename just
-- happened in the batch above. On re-run the flag table does not exist so the
-- reset is skipped, preventing re-nullification of values healed by the apply-pass.
-- Dynamic SQL avoids compile-time resolution of #faktura_remodel_did_rename when
-- the temp table does not exist (re-run / fresh-DB scenario).
IF OBJECT_ID('tempdb..#faktura_remodel_did_rename') IS NOT NULL
BEGIN
    EXEC('
        IF EXISTS (SELECT 1 FROM #faktura_remodel_did_rename)
        BEGIN
            UPDATE [fct].[Postering] SET DW_EK_Faktura = -1;
            PRINT ''Reset fct.Postering.DW_EK_Faktura to -1 (one-time migration)'';
        END
        DROP TABLE #faktura_remodel_did_rename;
    ');
END
GO

-- ---------- fct.Postering: catch-all reset for partial-run recovery ----------
-- If a previous run renamed the column but failed before the reset (e.g. the
-- mssql_python driver raised on sp_rename's warning), the temp-table flag was
-- never created. Detect this state: column DW_EK_Faktura exists, the old
-- default constraint name still references FakturaEgenskaber (proving the column
-- was just renamed, not created fresh). Reset to -1 so the apply-pass can heal.
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Faktura')
   AND EXISTS (
       SELECT 1 FROM sys.default_constraints dc
       JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
       WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = 'DW_EK_Faktura'
         AND dc.name LIKE '%FakturaEgenskaber%'
   )
BEGIN
    UPDATE [fct].[Postering] SET DW_EK_Faktura = -1;
    PRINT 'Catch-all: reset fct.Postering.DW_EK_Faktura to -1 (partial-run recovery)';
END
GO

-- ---------- fct.Fakturalinje: drop DW_EK_FakturaEgenskaber (default-safe) ----------
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_FakturaEgenskaber')
BEGIN
    DECLARE @df_linje sysname;
    SELECT @df_linje = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'DW_EK_FakturaEgenskaber';
    IF @df_linje IS NOT NULL EXEC('ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + @df_linje + ']');
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [DW_EK_FakturaEgenskaber];
    PRINT 'Dropped fct.Fakturalinje.DW_EK_FakturaEgenskaber (+ default constraint)';
END
GO
