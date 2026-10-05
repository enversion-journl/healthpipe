-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Rename dim.Vare -> dim.Leverandoervare (repo-wide), incl. full key cascade
-- (dw_ek_Vare -> dw_ek_Leverandoervare, clean_code_Vare -> clean_code_Leverandoervare)
-- through map, bridge (VareILSVare -> LeverandoervareILSVare) and the three facts
-- (Fakturalinje, Vareudlevering, AftaleVarePris).
--
-- Idempotent, case-sensitive guards. Runs against BOTH staging and DSO databases;
-- absent objects no-op via the IF EXISTS guards. Views/functions are re-issued from
-- base DDL, so the old ones are dropped here (not renamed).

-- ---------------------------------------------------------------------------
-- 1. Drop old view + security function (recreated with new names by base DDL)
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[vw_dim].[Vare]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[Vare];
GO

IF OBJECT_ID(N'[sec].[BeskytDimVare_v1]', 'IF') IS NOT NULL
    DROP FUNCTION [sec].[BeskytDimVare_v1];
GO

-- ---------------------------------------------------------------------------
-- 2. Rename tables
-- ---------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'dim' AND t.name COLLATE Latin1_General_100_CS_AS = 'Vare') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'dim' AND t.name COLLATE Latin1_General_100_CS_AS = 'Leverandoervare')
    EXEC sp_rename 'dim.Vare', 'Leverandoervare';
GO

IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'map' AND t.name COLLATE Latin1_General_100_CS_AS = 'Vare') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'map' AND t.name COLLATE Latin1_General_100_CS_AS = 'Leverandoervare')
    EXEC sp_rename 'map.Vare', 'Leverandoervare';
GO

IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'bridge' AND t.name COLLATE Latin1_General_100_CS_AS = 'VareILSVare') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'bridge' AND t.name COLLATE Latin1_General_100_CS_AS = 'LeverandoervareILSVare')
    EXEC sp_rename 'bridge.VareILSVare', 'LeverandoervareILSVare';
GO

-- ---------------------------------------------------------------------------
-- 3. Rename columns (dw_ek_Vare / clean_code_Vare). Tables referenced by NEW name.
-- ---------------------------------------------------------------------------
-- dim.Leverandoervare.dw_ek_Vare
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Leverandoervare' AND SCHEMA_NAME(t.schema_id) = 'dim' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Vare') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Leverandoervare' AND SCHEMA_NAME(t.schema_id) = 'dim' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Leverandoervare')
    EXEC sp_rename 'dim.Leverandoervare.dw_ek_Vare', 'dw_ek_Leverandoervare', 'COLUMN';
GO

-- map.Leverandoervare.dw_ek_Vare
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Leverandoervare' AND SCHEMA_NAME(t.schema_id) = 'map' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Vare') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Leverandoervare' AND SCHEMA_NAME(t.schema_id) = 'map' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Leverandoervare')
    EXEC sp_rename 'map.Leverandoervare.dw_ek_Vare', 'dw_ek_Leverandoervare', 'COLUMN';
GO

-- map.Leverandoervare.clean_code_Vare
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Leverandoervare' AND SCHEMA_NAME(t.schema_id) = 'map' AND c.name COLLATE Latin1_General_100_CS_AS = 'clean_code_Vare') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Leverandoervare' AND SCHEMA_NAME(t.schema_id) = 'map' AND c.name COLLATE Latin1_General_100_CS_AS = 'clean_code_Leverandoervare')
    EXEC sp_rename 'map.Leverandoervare.clean_code_Vare', 'clean_code_Leverandoervare', 'COLUMN';
GO

-- bridge.LeverandoervareILSVare.dw_ek_Vare
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'LeverandoervareILSVare' AND SCHEMA_NAME(t.schema_id) = 'bridge' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Vare') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'LeverandoervareILSVare' AND SCHEMA_NAME(t.schema_id) = 'bridge' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Leverandoervare')
    EXEC sp_rename 'bridge.LeverandoervareILSVare.dw_ek_Vare', 'dw_ek_Leverandoervare', 'COLUMN';
GO

-- fct.Fakturalinje.dw_ek_Vare
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Fakturalinje' AND SCHEMA_NAME(t.schema_id) = 'fct' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Vare') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Fakturalinje' AND SCHEMA_NAME(t.schema_id) = 'fct' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Leverandoervare')
    EXEC sp_rename 'fct.Fakturalinje.dw_ek_Vare', 'dw_ek_Leverandoervare', 'COLUMN';
GO

-- fct.Vareudlevering.dw_ek_Vare
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Vareudlevering' AND SCHEMA_NAME(t.schema_id) = 'fct' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Vare') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'Vareudlevering' AND SCHEMA_NAME(t.schema_id) = 'fct' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Leverandoervare')
    EXEC sp_rename 'fct.Vareudlevering.dw_ek_Vare', 'dw_ek_Leverandoervare', 'COLUMN';
GO

-- fct.AftaleVarePris.dw_ek_Vare
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'AftaleVarePris' AND SCHEMA_NAME(t.schema_id) = 'fct' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Vare') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'AftaleVarePris' AND SCHEMA_NAME(t.schema_id) = 'fct' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Leverandoervare')
    EXEC sp_rename 'fct.AftaleVarePris.dw_ek_Vare', 'dw_ek_Leverandoervare', 'COLUMN';
GO

-- ---------------------------------------------------------------------------
-- 4. Rename PK constraints
-- ---------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.objects WHERE name COLLATE Latin1_General_100_CS_AS = 'PK_Dim_Vare' AND type = 'PK') AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE name COLLATE Latin1_General_100_CS_AS = 'PK_Dim_Leverandoervare' AND type = 'PK')
    EXEC sp_rename 'dim.PK_Dim_Vare', 'PK_Dim_Leverandoervare';
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE name COLLATE Latin1_General_100_CS_AS = 'PK_Map_Vare' AND type = 'PK') AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE name COLLATE Latin1_General_100_CS_AS = 'PK_Map_Leverandoervare' AND type = 'PK')
    EXEC sp_rename 'map.PK_Map_Vare', 'PK_Map_Leverandoervare';
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE name COLLATE Latin1_General_100_CS_AS = 'PK_Bridge_VareILSVare' AND type = 'PK') AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE name COLLATE Latin1_General_100_CS_AS = 'PK_Bridge_LeverandoervareILSVare' AND type = 'PK')
    EXEC sp_rename 'bridge.PK_Bridge_VareILSVare', 'PK_Bridge_LeverandoervareILSVare';
GO

-- ---------------------------------------------------------------------------
-- 5. Rename indexes
-- ---------------------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'map' AND t.name = 'Leverandoervare' AND i.name = 'IX_Map_Vare_DW_EK') AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'map' AND t.name = 'Leverandoervare' AND i.name = 'IX_Map_Leverandoervare_DW_EK')
    EXEC sp_rename 'map.Leverandoervare.IX_Map_Vare_DW_EK', 'IX_Map_Leverandoervare_DW_EK', 'INDEX';
GO

IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'bridge' AND t.name = 'LeverandoervareILSVare' AND i.name = 'IX_Bridge_VareILSVare_ILSVare') AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'bridge' AND t.name = 'LeverandoervareILSVare' AND i.name = 'IX_Bridge_LeverandoervareILSVare_ILSVare')
    EXEC sp_rename 'bridge.LeverandoervareILSVare.IX_Bridge_VareILSVare_ILSVare', 'IX_Bridge_LeverandoervareILSVare_ILSVare', 'INDEX';
GO

IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'fct' AND t.name = 'Vareudlevering' AND i.name = 'IX_Fct_Vareudlevering_Vare') AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'fct' AND t.name = 'Vareudlevering' AND i.name = 'IX_Fct_Vareudlevering_Leverandoervare')
    EXEC sp_rename 'fct.Vareudlevering.IX_Fct_Vareudlevering_Vare', 'IX_Fct_Vareudlevering_Leverandoervare', 'INDEX';
GO

IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'fct' AND t.name = 'AftaleVarePris' AND i.name = 'IX_Fct_AftaleVarePris_Vare') AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'fct' AND t.name = 'AftaleVarePris' AND i.name = 'IX_Fct_AftaleVarePris_Leverandoervare')
    EXEC sp_rename 'fct.AftaleVarePris.IX_Fct_AftaleVarePris_Vare', 'IX_Fct_AftaleVarePris_Leverandoervare', 'INDEX';
GO

-- ---------------------------------------------------------------------------
-- 6. Migrate DFM checkpoint/log + DQA state rows (delta chain keys on these)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
BEGIN
    EXEC('UPDATE meta.DFMState SET table_name = ''Leverandoervare'' WHERE schema_name = ''dim'' AND table_name = ''Vare''');
    EXEC('UPDATE meta.DFMState SET table_name = ''dim_Leverandoervare'' WHERE schema_name = ''stg'' AND table_name = ''dim_Vare''');
    EXEC('UPDATE meta.DFMState SET table_name = ''LeverandoervareILSVare'' WHERE schema_name = ''bridge'' AND table_name = ''VareILSVare''');
END
GO

IF OBJECT_ID('meta.DFMRunLog', 'U') IS NOT NULL
BEGIN
    EXEC('UPDATE meta.DFMRunLog SET table_name = ''Leverandoervare'' WHERE schema_name = ''dim'' AND table_name = ''Vare''');
    EXEC('UPDATE meta.DFMRunLog SET table_name = ''dim_Leverandoervare'' WHERE schema_name = ''stg'' AND table_name = ''dim_Vare''');
    EXEC('UPDATE meta.DFMRunLog SET table_name = ''LeverandoervareILSVare'' WHERE schema_name = ''bridge'' AND table_name = ''VareILSVare''');
END
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
BEGIN
    EXEC('UPDATE dqa.Results SET subject = ''dim.Leverandoervare'' WHERE subject = ''dim.Vare''');
    EXEC('UPDATE dqa.Results SET subject = ''map.Leverandoervare'' WHERE subject = ''map.Vare''');
    EXEC('UPDATE dqa.Results SET subject = ''bridge.LeverandoervareILSVare'' WHERE subject = ''bridge.VareILSVare''');
    EXEC('UPDATE dqa.Results SET name_suffix = ''dw_ek_Leverandoervare'' WHERE name_suffix = ''dw_ek_Vare''');
    EXEC('UPDATE dqa.Results SET name_suffix = ''clean_code_Leverandoervare'' WHERE name_suffix = ''clean_code_Vare''');
END
GO
