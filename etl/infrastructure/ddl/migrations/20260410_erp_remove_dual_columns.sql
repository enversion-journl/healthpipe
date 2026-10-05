-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 12. Remove duplicate (non-bk_) business key columns from map tables
--     The bk_* prefixed columns already carry the same values.
--     No data migration needed — bk_* columns were backfilled in migration 11.
-- ============================================================================

-- ---- map.Kontoplan: drop FinanskontoId, Kontonummer ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'FinanskontoId')
BEGIN
    ALTER TABLE [map].[Kontoplan] DROP COLUMN [FinanskontoId];
    PRINT 'Dropped FinanskontoId from map.Kontoplan';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = 'Kontonummer')
BEGIN
    ALTER TABLE [map].[Kontoplan] DROP COLUMN [Kontonummer];
    PRINT 'Dropped Kontonummer from map.Kontoplan';
END
GO

-- ---- map.Udlevering: drop prrowid ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Udlevering]') AND name = 'prrowid')
BEGIN
    ALTER TABLE [map].[Udlevering] DROP COLUMN [prrowid];
    PRINT 'Dropped prrowid from map.Udlevering';
END
GO

-- ---- map.Lager: drop Code ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = 'Code')
BEGIN
    ALTER TABLE [map].[Lager] DROP COLUMN [Code];
    PRINT 'Dropped Code from map.Lager';
END
GO

-- ---- map.Rekvirent: drop Code ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = 'Code')
BEGIN
    ALTER TABLE [map].[Rekvirent] DROP COLUMN [Code];
    PRINT 'Dropped Code from map.Rekvirent';
END
GO
