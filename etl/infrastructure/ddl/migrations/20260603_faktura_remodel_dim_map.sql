-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Faktura remodel — dim rename + map drop
--
-- 1. sp_rename dim.FakturaEgenskaber → dim.Faktura
--    (PK column, PK constraint, unique index)
-- 2. DELETE non-sentinel rows (content re-grained; repopulated by dim_faktura ETL)
-- 3. DROP TABLE map.FakturaEgenskaber
--
-- Idempotent: guarded by sys.objects / sys.columns / sys.indexes checks.
-- ============================================================================

-- 1a. Rename table dim.FakturaEgenskaber → dim.Faktura
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[FakturaEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
BEGIN
    EXEC sp_rename N'dim.FakturaEgenskaber', N'Faktura';
    PRINT 'Renamed dim.FakturaEgenskaber → dim.Faktura';
END
GO

-- 1b. Rename PK column DW_EK_FakturaEgenskaber → DW_EK_Faktura
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'DW_EK_FakturaEgenskaber')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'DW_EK_Faktura')
BEGIN
    EXEC sp_rename N'dim.Faktura.DW_EK_FakturaEgenskaber', N'DW_EK_Faktura', N'COLUMN';
    PRINT 'Renamed dim.Faktura.DW_EK_FakturaEgenskaber → DW_EK_Faktura';
END
GO

-- 1c. Rename PK constraint PK_Dim_FakturaEgenskaber → PK_Dim_Faktura
IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_Dim_FakturaEgenskaber' AND parent_object_id = OBJECT_ID(N'[dim].[Faktura]'))
BEGIN
    EXEC sp_rename N'dim.PK_Dim_FakturaEgenskaber', N'PK_Dim_Faktura', N'OBJECT';
    PRINT 'Renamed PK constraint → PK_Dim_Faktura';
END
GO

-- 1d. Rename unique index IX_Dim_FakturaEgenskaber_DW_EK → IX_Dim_Faktura_DW_EK
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'IX_Dim_FakturaEgenskaber_DW_EK')
BEGIN
    EXEC sp_rename N'dim.Faktura.IX_Dim_FakturaEgenskaber_DW_EK', N'IX_Dim_Faktura_DW_EK', N'INDEX';
    PRINT 'Renamed index → IX_Dim_Faktura_DW_EK';
END
GO

-- 2. Delete non-sentinel rows (content will be repopulated at header grain by dim_faktura ETL)
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND type = 'U')
BEGIN
    DELETE FROM [dim].[Faktura] WHERE [DW_EK_Faktura] <> -1;
    PRINT 'Deleted non-sentinel rows from dim.Faktura (will be repopulated at header grain)';
END
GO

-- 3. Drop map.FakturaEgenskaber (retired — dim now keys from map.Faktura)
DROP TABLE IF EXISTS [map].[FakturaEgenskaber];
GO
