-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: warehouse attributes on dim.Lager
--
-- Test feedback from DOL asked for `lgrejer`, "a requisitioner used for goods
-- movements into the warehouse" (Strategisk Indkoeb). The same
-- source highlights `rekvsvind` and `deflevdage`, which were missing too.
--
-- Two things happen here:
--
-- 1) RENAME of adr/postdi/tlf to spelled-out names. The columns could be renamed
--    with no data risk because they were ALWAYS NULL: stg_dim_Lager pivoted the
--    property keys adr/land/postdi/tlf out of codesystem.LagerProperties, but
--    cs_lager never wrote them. They are populated for the first time by this
--    change. `land` keeps its name -- it is already a spelled-out word.
--
-- 2) NEW columns: the lagerejer/svind requisitioner numbers,
--    standard_leveringsdage, the source's own active flag (aktiv), and the FKs
--    dw_ek_Rekvirent_lagerejer / dw_ek_Rekvirent_svind (resolved against
--    map.Rekvirent in dim_Lager).
--
-- Runs automatically on BOTH staging and DSO via the central migrations runner.
-- Idempotent -- every ALTER is guarded on the column's presence, and the renames
-- run only while the old name exists and the new one does not. The CREATE TABLE
-- in tables/erp/02_dims.sql already declares the end state, so on a fresh install
-- the script is a no-op.
-- 1) Rename the three abbreviated columns.
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
           WHERE t.name = 'Lager' AND SCHEMA_NAME(t.schema_id) = 'dim'
             AND c.name COLLATE Latin1_General_100_CS_AS = 'adr')
   AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
           WHERE t.name = 'Lager' AND SCHEMA_NAME(t.schema_id) = 'dim'
             AND c.name COLLATE Latin1_General_100_CS_AS = 'adresse')
BEGIN
    EXEC sp_rename 'dim.Lager.adr', 'adresse', 'COLUMN';
    PRINT 'Renamed dim.Lager.adr -> adresse';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
           WHERE t.name = 'Lager' AND SCHEMA_NAME(t.schema_id) = 'dim'
             AND c.name COLLATE Latin1_General_100_CS_AS = 'postdi')
   AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
           WHERE t.name = 'Lager' AND SCHEMA_NAME(t.schema_id) = 'dim'
             AND c.name COLLATE Latin1_General_100_CS_AS = 'postnummer')
BEGIN
    EXEC sp_rename 'dim.Lager.postdi', 'postnummer', 'COLUMN';
    PRINT 'Renamed dim.Lager.postdi -> postnummer';
END
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
           WHERE t.name = 'Lager' AND SCHEMA_NAME(t.schema_id) = 'dim'
             AND c.name COLLATE Latin1_General_100_CS_AS = 'tlf')
   AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
           WHERE t.name = 'Lager' AND SCHEMA_NAME(t.schema_id) = 'dim'
             AND c.name COLLATE Latin1_General_100_CS_AS = 'telefon')
BEGIN
    EXEC sp_rename 'dim.Lager.tlf', 'telefon', 'COLUMN';
    PRINT 'Renamed dim.Lager.tlf -> telefon';
END
GO

-- 2) New columns.
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Lager'
             AND COLUMN_NAME = 'lagerejer_rekvirentnummer')
    ALTER TABLE [dim].[Lager] ADD [lagerejer_rekvirentnummer] NVARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Lager'
             AND COLUMN_NAME = 'svind_rekvirentnummer')
    ALTER TABLE [dim].[Lager] ADD [svind_rekvirentnummer] NVARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Lager'
             AND COLUMN_NAME = 'standard_leveringsdage')
    ALTER TABLE [dim].[Lager] ADD [standard_leveringsdage] INT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Lager'
             AND COLUMN_NAME = 'aktiv')
    ALTER TABLE [dim].[Lager] ADD [aktiv] BIT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Lager'
             AND COLUMN_NAME = 'dw_ek_Rekvirent_lagerejer')
    ALTER TABLE [dim].[Lager] ADD [dw_ek_Rekvirent_lagerejer] INT NOT NULL
        CONSTRAINT [DF_Lager_dw_ek_Rekvirent_lagerejer] DEFAULT -1;
GO
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Lager'
             AND COLUMN_NAME = 'dw_ek_Rekvirent_svind')
    ALTER TABLE [dim].[Lager] ADD [dw_ek_Rekvirent_svind] INT NOT NULL
        CONSTRAINT [DF_Lager_dw_ek_Rekvirent_svind] DEFAULT -1;
GO

-- 3) The staging table is truncate/reload (DROP + CREATE in stg_dim_Lager), so it
--    is rebuilt with the new column set on the next materialization. Guarded drop
--    so an old column set cannot survive on staging; a no-op on DSO.
IF EXISTS (SELECT * FROM sys.objects
           WHERE object_id = OBJECT_ID(N'[stg].[dim_Lager]') AND type = 'U')
BEGIN
    DROP TABLE [stg].[dim_Lager];
    PRINT 'Dropped stg.dim_Lager for reload with the new column set';
END
GO

-- 4) The old property keys in codesystem.LagerProperties (adr/land/postdi/tlf)
--    are no longer written: the address fields are populated outside the
--    CodeSystem now. cs_lager MERGEs on (code, property_code) and never cleans up
--    after itself, so the stale rows would otherwise linger. Guarded: a no-op if
--    the table does not exist (DSO receives it via sync). In practice the whole
--    DELETE is a no-op — the keys were declared but never written (the bug this
--    change fixes) — but land is included for consistency with its removal from
--    codesystem.json and PROPERTY_COLUMN_MAP.
IF OBJECT_ID(N'[codesystem].[LagerProperties]', 'U') IS NOT NULL
BEGIN
    DELETE FROM [codesystem].[LagerProperties]
    WHERE [property_code] IN (N'adr', N'land', N'postdi', N'tlf');
    PRINT 'Removed superseded lager property keys (adr/land/postdi/tlf)';
END
GO
