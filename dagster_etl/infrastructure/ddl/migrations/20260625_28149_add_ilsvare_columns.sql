-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Add Strategisk-Indkøb columns to dim.IlsVare + art/gruppering swap rename (#28149)
--
-- Runs automatically on BOTH staging and DSO via the centralized migrations
-- runner (staging_migrations / dso_migrations). Idempotent — safe to re-run.
-- The ext.vare_enriched_ils drop (section 3) is guarded and is a no-op on DSO,
-- where the ext staging table does not exist.

-- 1) Rename swapped columns to SydAX-correct semantics (ILS vareart = gruppering, varegrp = art).
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Vareart')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Gruppering')
BEGIN
    EXEC sp_rename 'dim.IlsVare.Vareart', 'Gruppering', 'COLUMN';
    PRINT 'Renamed dim.IlsVare.Vareart -> Gruppering';
END
GO
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Vareart_Navn')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Gruppering_Navn')
BEGIN
    EXEC sp_rename 'dim.IlsVare.Vareart_Navn', 'Gruppering_Navn', 'COLUMN';
    PRINT 'Renamed dim.IlsVare.Vareart_Navn -> Gruppering_Navn';
END
GO
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Varegruppe')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Art')
BEGIN
    EXEC sp_rename 'dim.IlsVare.Varegruppe', 'Art', 'COLUMN';
    PRINT 'Renamed dim.IlsVare.Varegruppe -> Art';
END
GO
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Varegruppe_Navn')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA='dim' AND TABLE_NAME='IlsVare' AND COLUMN_NAME='Art_Navn')
BEGIN
    EXEC sp_rename 'dim.IlsVare.Varegruppe_Navn', 'Art_Navn', 'COLUMN';
    PRINT 'Renamed dim.IlsVare.Varegruppe_Navn -> Art_Navn';
END
GO

-- 2) Add the new columns (each guarded).
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'FaellesVarenummer')
    ALTER TABLE [dim].[IlsVare] ADD [FaellesVarenummer] NVARCHAR(100) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'EAN_Varenummer')
    ALTER TABLE [dim].[IlsVare] ADD [EAN_Varenummer] NVARCHAR(100) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'Billede')
    ALTER TABLE [dim].[IlsVare] ADD [Billede] NVARCHAR(MAX) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'OprBruger')
    ALTER TABLE [dim].[IlsVare] ADD [OprBruger] NVARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'OprBruger_Navn')
    ALTER TABLE [dim].[IlsVare] ADD [OprBruger_Navn] NVARCHAR(255) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'Weblink')
    ALTER TABLE [dim].[IlsVare] ADD [Weblink] NVARCHAR(MAX) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'MaaIkkeLukkes')
    ALTER TABLE [dim].[IlsVare] ADD [MaaIkkeLukkes] NVARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'Hygiejne')
    ALTER TABLE [dim].[IlsVare] ADD [Hygiejne] NVARCHAR(50) NULL;
GO

-- 2b) Drop the computed Varenavn_Concat column — redundant (= Varenavn + Varenavn2,
--     which both exist); end users can concatenate if needed. Guarded for re-run.
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'IlsVare' AND COLUMN_NAME = 'Varenavn_Concat')
BEGIN
    ALTER TABLE [dim].[IlsVare] DROP COLUMN [Varenavn_Concat];
    PRINT 'Dropped redundant dim.IlsVare.Varenavn_Concat';
END
GO

-- 3) Drop the enriched staging table so it is recreated with the new column set
--    on the next ext_vare_enriched_ils materialization (table is truncate/reload).
--    Guarded — no-op on DSO, where ext.vare_enriched_ils does not exist.
IF EXISTS (SELECT * FROM sys.objects
           WHERE object_id = OBJECT_ID(N'[ext].[vare_enriched_ils]') AND type = 'U')
BEGIN
    DROP TABLE [ext].[vare_enriched_ils];
    PRINT 'Dropped ext.vare_enriched_ils for reload with new schema';
END
GO
