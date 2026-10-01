-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Fact Table Migrations
--
-- All fct.* schema migrations in dependency order.
-- Every statement is idempotent. Safe to delete once applied to all environments.
-- ============================================================================

-- ============================================================================
-- 1. Drop fct.Fakturalinje if it has incompatible legacy schema
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
   AND EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'statistic_invoice_line_id')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Fakturalinje')
BEGIN
    DROP TABLE [fct].[Fakturalinje];
    PRINT 'Dropped legacy fct.Fakturalinje table (incompatible schema)';
END
GO

-- ============================================================================
-- 2. Add DW_EK_Organization and DW_EK_Kontoplan columns to fct.Fakturalinje
-- ============================================================================

-- Skip when DW_EK_Organisation (modern spelling) already exists on the table —
-- otherwise we'd add the legacy column on top, creating a dual-column state
-- that breaks 20260427_rename_organization_to_organisation.sql.
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organisation')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] ADD [DW_EK_Organization] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Organization column to fct.Fakturalinje';
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Kontoplan')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] ADD [DW_EK_Kontoplan] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Kontoplan column to fct.Fakturalinje';
END
GO

-- ============================================================================
-- 3. Fix NULL DW_EK values in fct.Fakturalinje (set to -1 UNKNOWN)
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    UPDATE [fct].[Fakturalinje] SET [DW_EK_Kreditor] = -1 WHERE [DW_EK_Kreditor] IS NULL;
    UPDATE [fct].[Fakturalinje] SET [DW_EK_Vare] = -1 WHERE [DW_EK_Vare] IS NULL;
    UPDATE [fct].[Fakturalinje] SET [DW_EK_Faktura] = -1 WHERE [DW_EK_Faktura] IS NULL;
    UPDATE [fct].[Fakturalinje] SET [DW_EK_Kontoplan] = -1 WHERE [DW_EK_Kontoplan] IS NULL;
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organization')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Organization] = -1 WHERE [DW_EK_Organization] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organisation')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Organisation] = -1 WHERE [DW_EK_Organisation] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Faktura')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Faktura] = -1 WHERE [DW_EK_Date_Faktura] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Faktura')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Faktura] = -1 WHERE [DW_EK_Dato_Faktura] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Ordre')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Ordre] = -1 WHERE [DW_EK_Date_Ordre] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Ordre')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Ordre] = -1 WHERE [DW_EK_Dato_Ordre] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Betalings')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Betalings] = -1 WHERE [DW_EK_Date_Betalings] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Betaling')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Betaling] = -1 WHERE [DW_EK_Dato_Betaling] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Modtagelses')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Modtagelses] = -1 WHERE [DW_EK_Date_Modtagelses] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Modtagelse')
        EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Modtagelse] = -1 WHERE [DW_EK_Dato_Modtagelse] IS NULL');
    PRINT 'Fixed NULL DW_EK values in fct.Fakturalinje (set to -1)';
END
GO

-- ============================================================================
-- 4. Drop legacy PK, indexes, and source system columns from fct.Fakturalinje
-- ============================================================================

IF EXISTS (
    SELECT 1
    FROM sys.index_columns ic
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE ic.object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND ic.index_id = (SELECT index_id FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND is_primary_key = 1)
      AND c.name = 'statistic_invoice_line_id'
)
BEGIN
    DECLARE @pk_name NVARCHAR(128);
    SELECT @pk_name = name FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND is_primary_key = 1;
    IF @pk_name IS NOT NULL
        EXEC('ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + @pk_name + ']');
    PRINT 'Dropped old primary key constraint: ' + @pk_name;
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_StatisticInvoice')
        DROP INDEX [IX_Fct_Fakturalinje_StatisticInvoice] ON [fct].[Fakturalinje];
    IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'statistic_invoice_line_id')
        ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [statistic_invoice_line_id];
    IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'statistic_invoice_header_id')
        ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [statistic_invoice_header_id];
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND is_primary_key = 1)
   AND EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] ADD CONSTRAINT [PK_Fct_Fakturalinje] PRIMARY KEY CLUSTERED ([DW_EK_Fakturalinje] ASC);
    PRINT 'Created PK_Fct_Fakturalinje on DW_EK_Fakturalinje';
END
GO

-- ============================================================================
-- 5. Rename legacy columns in fct.Fakturalinje to canonical naming
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_CleanCode')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Fakturalinje_CleanCode')
BEGIN
    DROP INDEX [IX_Fct_Fakturalinje_CleanCode] ON [fct].[Fakturalinje];
    PRINT 'Dropped IX_Fct_Fakturalinje_CleanCode (depends on legacy column)';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Fakturalinje_CleanCode')
    EXEC sp_rename '[fct].[Fakturalinje].[Fakturalinje_CleanCode]', 'CleanCode_Fakturalinje', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'FakturaLines_CleanCode')
    EXEC sp_rename '[fct].[Fakturalinje].[FakturaLines_CleanCode]', 'bk_FakturaLines', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'FakturaHeader_CleanCode')
    EXEC sp_rename '[fct].[Fakturalinje].[FakturaHeader_CleanCode]', 'bk_FakturaHeader', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'FakturaMetadata_CleanCode')
    EXEC sp_rename '[fct].[Fakturalinje].[FakturaMetadata_CleanCode]', 'bk_FakturaMetadata', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'CleanCode_FakturaLines')
    EXEC sp_rename '[fct].[Fakturalinje].[CleanCode_FakturaLines]', 'bk_FakturaLines', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'CleanCode_FakturaHeader')
    EXEC sp_rename '[fct].[Fakturalinje].[CleanCode_FakturaHeader]', 'bk_FakturaHeader', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'CleanCode_FakturaMetadata')
    EXEC sp_rename '[fct].[Fakturalinje].[CleanCode_FakturaMetadata]', 'bk_FakturaMetadata', 'COLUMN';
GO

-- ============================================================================
-- 6. Add canonical CleanCode + bk columns to fct.Fakturalinje
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name IN ('Fakturalinje_CleanCode', 'CleanCode_Fakturalinje'))
BEGIN
    ALTER TABLE [fct].[Fakturalinje] ADD [CleanCode_Fakturalinje] NVARCHAR(500) NULL;
    ALTER TABLE [fct].[Fakturalinje] ADD [bk_FakturaLines] NVARCHAR(500) NULL;
    ALTER TABLE [fct].[Fakturalinje] ADD [bk_FakturaHeader] NVARCHAR(500) NULL;
    ALTER TABLE [fct].[Fakturalinje] ADD [bk_FakturaMetadata] NVARCHAR(500) NULL;
    PRINT 'Added canonical CleanCode + bk_* columns to fct.Fakturalinje';
END
GO

-- ============================================================================
-- 7. Add LinjeTotal and Valutakode_Faktura columns to fct.Fakturalinje
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND type = 'U')
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'LinjeTotal')
        ALTER TABLE [fct].[Fakturalinje] ADD [LinjeTotal] DECIMAL(18,4) NULL;
    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_Faktura')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_Faktura] NVARCHAR(10) NULL;
END
GO

-- ============================================================================
-- 8. Fix NULL DW_EK values in fct.Postering (set to -1 UNKNOWN)
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
BEGIN
    UPDATE [fct].[Postering] SET [DW_EK_Kreditor] = -1 WHERE [DW_EK_Kreditor] IS NULL;
    UPDATE [fct].[Postering] SET [DW_EK_Kontoplan] = -1 WHERE [DW_EK_Kontoplan] IS NULL;
    UPDATE [fct].[Postering] SET [DW_EK_Faktura] = -1 WHERE [DW_EK_Faktura] IS NULL;
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Godkendelses')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Godkendelses] = -1 WHERE [DW_EK_Date_Godkendelses] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Godkendelse')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Godkendelse] = -1 WHERE [DW_EK_Dato_Godkendelse] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Lukke')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Lukke] = -1 WHERE [DW_EK_Date_Lukke] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Lukket')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Lukket] = -1 WHERE [DW_EK_Dato_Lukket] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_TransOprettet')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_TransOprettet] = -1 WHERE [DW_EK_Date_TransOprettet] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_TransOprettet')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_TransOprettet] = -1 WHERE [DW_EK_Dato_TransOprettet] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Forfalds')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Forfalds] = -1 WHERE [DW_EK_Date_Forfalds] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Forfald')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Forfald] = -1 WHERE [DW_EK_Dato_Forfald] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_JournalFaktura')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_JournalFaktura] = -1 WHERE [DW_EK_Date_JournalFaktura] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_JournalFaktura')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_JournalFaktura] = -1 WHERE [DW_EK_Dato_JournalFaktura] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_SidstUdlignet')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_SidstUdlignet] = -1 WHERE [DW_EK_Date_SidstUdlignet] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_SidstUdlignet')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_SidstUdlignet] = -1 WHERE [DW_EK_Dato_SidstUdlignet] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Transaktions')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Transaktions] = -1 WHERE [DW_EK_Date_Transaktions] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Transaktion')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Transaktion] = -1 WHERE [DW_EK_Dato_Transaktion] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Bogfoerings')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Bogfoerings] = -1 WHERE [DW_EK_Date_Bogfoerings] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Bogfoering')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Bogfoering] = -1 WHERE [DW_EK_Dato_Bogfoering] IS NULL');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_PostOprettet')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_PostOprettet] = -1 WHERE [DW_EK_Date_PostOprettet] IS NULL');
    ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_PostOprettet')
        EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_PostOprettet] = -1 WHERE [DW_EK_Dato_PostOprettet] IS NULL');
    PRINT 'Fixed NULL DW_EK values in fct.Postering (set to -1)';
END
GO

-- ============================================================================
-- 9. Add DW_haendelsestidspunkt column to fct.Postering
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_haendelsestidspunkt')
BEGIN
    ALTER TABLE [fct].[Postering] ADD [DW_haendelsestidspunkt] DATETIME2 NULL;
    PRINT 'Added DW_haendelsestidspunkt column to fct.Postering';
END
GO

-- ============================================================================
-- 10. Add DW_EK_Organization column and index to fct.Postering
-- ============================================================================

-- Skip when DW_EK_Organisation (modern spelling) already exists on the table —
-- otherwise we'd add the legacy column on top, creating a dual-column state
-- that breaks 20260427_rename_organization_to_organisation.sql.
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organisation')
BEGIN
    ALTER TABLE [fct].[Postering] ADD [DW_EK_Organization] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Organization column to fct.Postering';
END
GO

IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Organization')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Organization]
    ON [fct].[Postering] ([DW_EK_Organization] ASC) ON [PRIMARY];
    PRINT 'Created IX_Fct_Postering_Organization index';
END
GO

-- ============================================================================
-- 11. Drop obsolete fct.Vareaftaler table
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Vareaftaler]') AND type = 'U')
BEGIN
    DROP TABLE [fct].[Vareaftaler];
    PRINT 'Dropped obsolete fct.Vareaftaler table';
END
GO

-- ============================================================================
-- 12. Add DW_EK_Rekvirent column and index to fct.Udlevering
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Udlevering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Udlevering]') AND name = 'DW_EK_Rekvirent')
BEGIN
    ALTER TABLE [fct].[Udlevering] ADD [DW_EK_Rekvirent] INT NOT NULL DEFAULT -1;
    CREATE NONCLUSTERED INDEX [IX_Fct_Udlevering_Rekvirent] ON [fct].[Udlevering]
    ([DW_EK_Rekvirent] ASC) ON [PRIMARY];
    PRINT 'Added DW_EK_Rekvirent + index to fct.Udlevering';
END
GO

-- ============================================================================
-- 13. Narrow DW_EK columns from BIGINT to INT in fct.Fakturalinje & fct.Postering
-- ============================================================================

-- fct.Fakturalinje
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Fakturalinje'
      AND COLUMN_NAME IN ('DW_EK_Kreditor', 'DW_EK_Vare', 'DW_EK_Faktura', 'DW_EK_Kontoplan')
      AND DATA_TYPE = 'bigint'
)
BEGIN
    DECLARE @sql_fl NVARCHAR(MAX) = N'';
    SELECT @sql_fl = @sql_fl + 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + ']; '
    FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
      AND c.name IN ('DW_EK_Kreditor', 'DW_EK_Vare', 'DW_EK_Faktura', 'DW_EK_Kontoplan');
    IF LEN(@sql_fl) > 0 EXEC sp_executesql @sql_fl;

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'IX_Fct_Fakturalinje_Kreditor')
        DROP INDEX [IX_Fct_Fakturalinje_Kreditor] ON [fct].[Fakturalinje];

    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Fakturalinje' AND COLUMN_NAME = 'DW_EK_Kreditor' AND DATA_TYPE = 'bigint')
        ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [DW_EK_Kreditor] INT NOT NULL;
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Fakturalinje' AND COLUMN_NAME = 'DW_EK_Vare' AND DATA_TYPE = 'bigint')
        ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [DW_EK_Vare] INT NOT NULL;
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Fakturalinje' AND COLUMN_NAME = 'DW_EK_Faktura' AND DATA_TYPE = 'bigint')
        ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [DW_EK_Faktura] INT NOT NULL;
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Fakturalinje' AND COLUMN_NAME = 'DW_EK_Kontoplan' AND DATA_TYPE = 'bigint')
        ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [DW_EK_Kontoplan] INT NOT NULL;

    ALTER TABLE [fct].[Fakturalinje] ADD DEFAULT -1 FOR [DW_EK_Kreditor];
    ALTER TABLE [fct].[Fakturalinje] ADD DEFAULT -1 FOR [DW_EK_Vare];
    ALTER TABLE [fct].[Fakturalinje] ADD DEFAULT -1 FOR [DW_EK_Faktura];
    ALTER TABLE [fct].[Fakturalinje] ADD DEFAULT -1 FOR [DW_EK_Kontoplan];

    CREATE NONCLUSTERED INDEX [IX_Fct_Fakturalinje_Kreditor]
    ON [fct].[Fakturalinje] ([DW_EK_Kreditor] ASC) INCLUDE ([DW_EK_Vare], [FakturaNr]) ON [PRIMARY];

    PRINT 'Narrowed fct.Fakturalinje DW_EK columns from BIGINT to INT';
END
GO

-- fct.Postering
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Postering'
      AND COLUMN_NAME IN ('DW_EK_Kreditor', 'DW_EK_Kontoplan', 'DW_EK_Faktura')
      AND DATA_TYPE = 'bigint'
)
BEGIN
    DECLARE @sql_po NVARCHAR(MAX) = N'';
    SELECT @sql_po = @sql_po + 'ALTER TABLE [fct].[Postering] DROP CONSTRAINT [' + dc.name + ']; '
    FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]')
      AND c.name IN ('DW_EK_Kreditor', 'DW_EK_Kontoplan', 'DW_EK_Faktura');
    IF LEN(@sql_po) > 0 EXEC sp_executesql @sql_po;

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Kreditor')
        DROP INDEX [IX_Fct_Postering_Kreditor] ON [fct].[Postering];

    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Postering' AND COLUMN_NAME = 'DW_EK_Kreditor' AND DATA_TYPE = 'bigint')
        ALTER TABLE [fct].[Postering] ALTER COLUMN [DW_EK_Kreditor] INT NOT NULL;
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Postering' AND COLUMN_NAME = 'DW_EK_Kontoplan' AND DATA_TYPE = 'bigint')
        ALTER TABLE [fct].[Postering] ALTER COLUMN [DW_EK_Kontoplan] INT NOT NULL;
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'fct' AND TABLE_NAME = 'Postering' AND COLUMN_NAME = 'DW_EK_Faktura' AND DATA_TYPE = 'bigint')
        ALTER TABLE [fct].[Postering] ALTER COLUMN [DW_EK_Faktura] INT NOT NULL;

    ALTER TABLE [fct].[Postering] ADD DEFAULT -1 FOR [DW_EK_Kreditor];
    ALTER TABLE [fct].[Postering] ADD DEFAULT -1 FOR [DW_EK_Kontoplan];
    ALTER TABLE [fct].[Postering] ADD DEFAULT -1 FOR [DW_EK_Faktura];

    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Kreditor]
    ON [fct].[Postering] ([DW_EK_Kreditor] ASC, [DW_EK_Kontoplan] ASC)
    INCLUDE ([FinansposteringId], [FakturaNr]) ON [PRIMARY];

    PRINT 'Narrowed fct.Postering DW_EK columns from BIGINT to INT';
END
GO

-- ============================================================================
-- fct.Postering: Migrate grain from FinansposteringId to ref_LedgertransId
--
-- ref_LedgertransId (BIGINT, unique) replaces FinansposteringId (NVARCHAR)
-- as the primary key. Truncate + rebuild since the PK type changes.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'ref_LedgertransId')
BEGIN
    TRUNCATE TABLE [fct].[Postering];

    -- Drop existing PK constraint
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_Fct_Postering' AND parent_object_id = OBJECT_ID(N'[fct].[Postering]'))
        ALTER TABLE [fct].[Postering] DROP CONSTRAINT [PK_Fct_Postering];

    -- Drop existing indexes that reference FinansposteringId
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Kreditor')
        DROP INDEX [IX_Fct_Postering_Kreditor] ON [fct].[Postering];

    -- Add new grain column
    ALTER TABLE [fct].[Postering] ADD [ref_LedgertransId] BIGINT NOT NULL DEFAULT 0;

    -- Make FinansposteringId nullable (kept as attribute)
    ALTER TABLE [fct].[Postering] ALTER COLUMN [FinansposteringId] NVARCHAR(100) NULL;

    -- Recreate PK on ref_LedgertransId
    ALTER TABLE [fct].[Postering] ADD CONSTRAINT [PK_Fct_Postering] PRIMARY KEY CLUSTERED ([ref_LedgertransId] ASC);

    -- Recreate FK lookup indexes
    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Kreditor] ON [fct].[Postering] (
        [DW_EK_Kreditor] ASC,
        [DW_EK_Kontoplan] ASC
    ) INCLUDE (
        [ref_LedgertransId],
        [FakturaNr]
    ) ON [PRIMARY];

    PRINT 'Migrated fct.Postering grain from FinansposteringId to ref_LedgertransId';
END
GO

-- ============================================================================
-- fct.Postering: Drop FinansposteringId and update indexes
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'FinansposteringId')
BEGIN
    -- Drop indexes that include FinansposteringId
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Kreditor')
        DROP INDEX [IX_Fct_Postering_Kreditor] ON [fct].[Postering];
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_FakturaNr')
        DROP INDEX [IX_Fct_Postering_FakturaNr] ON [fct].[Postering];

    ALTER TABLE [fct].[Postering] DROP COLUMN [FinansposteringId];

    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Kreditor]
    ON [fct].[Postering] ([DW_EK_Kreditor] ASC, [DW_EK_Kontoplan] ASC)
    INCLUDE ([FakturaNr]) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_FakturaNr]
    ON [fct].[Postering] ([FakturaNr] ASC)
    INCLUDE ([DW_EK_Kontoplan])
    WHERE [FakturaNr] IS NOT NULL
    ON [PRIMARY];

    PRINT 'Dropped FinansposteringId from fct.Postering and updated indexes';
END
GO
