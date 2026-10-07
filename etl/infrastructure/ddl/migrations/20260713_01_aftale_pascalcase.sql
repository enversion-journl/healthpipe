-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Aftale / map.Aftale skal vaere PascalCase (tabelnavne-standard).
-- Naming-overhaul'et (20260708_rename_*) lowercasede dem ved en fejl paa de
-- servere hvor det er koert; denne migration renamer tabeller, indeks,
-- DFM-state og DQA-referencer tilbage. Paa friske servere er alle steps
-- no-ops (DDL'en skaber tabellerne med korrekt casing).
-- Alle steps er case-sensitivt guarded. Koerer paa staging og DSO.
-- Rename-kortet (scripts/naming_overhaul/rename_map.yml) er rettet
-- tilsvarende, saa fremtidige genereringer ikke gentager fejlen.
-- ============================================================================
SET QUOTED_IDENTIFIER ON;
GO

-- 1) Tabeller tilbage til PascalCase
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'dim' AND t.name COLLATE Latin1_General_100_CS_AS = 'aftale')
    EXEC sp_rename 'dim.aftale', 'Aftale';
GO
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'map' AND t.name COLLATE Latin1_General_100_CS_AS = 'aftale')
    EXEC sp_rename 'map.aftale', 'Aftale';
GO

-- 2) Indeks-/constraintnavne tilbage
IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'dim' AND t.name = 'Aftale' AND i.name COLLATE Latin1_General_100_CS_AS = 'PK_Dim_aftale')
    EXEC sp_rename 'dim.Aftale.PK_Dim_aftale', 'PK_Dim_Aftale', 'INDEX';
GO
IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'dim' AND t.name = 'Aftale' AND i.name COLLATE Latin1_General_100_CS_AS = 'IX_Dim_aftale_Current')
    EXEC sp_rename 'dim.Aftale.IX_Dim_aftale_Current', 'IX_Dim_Aftale_Current', 'INDEX';
GO
IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'map' AND t.name = 'Aftale' AND i.name COLLATE Latin1_General_100_CS_AS = 'PK_Map_aftale')
    EXEC sp_rename 'map.Aftale.PK_Map_aftale', 'PK_Map_Aftale', 'INDEX';
GO
IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'map' AND t.name = 'Aftale' AND i.name COLLATE Latin1_General_100_CS_AS = 'IX_Map_aftale_DW_EK')
    EXEC sp_rename 'map.Aftale.IX_Map_aftale_DW_EK', 'IX_Map_Aftale_DW_EK', 'INDEX';
GO

-- 3) DFM-state og watermark-raekker tilbage (CS-sammenligning saa kun lowercase-raekker rammes)
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
BEGIN
    EXEC('UPDATE meta.DFMState SET table_name = ''Aftale'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''aftale''');
    EXEC('UPDATE meta.DFMState SET table_name = ''dim.Aftale'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''dim.aftale''');
    EXEC('UPDATE meta.DFMState SET table_name = ''map.Aftale'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''map.aftale''');
    EXEC('UPDATE meta.DFMState SET table_name = ''Aftale_flow_start'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''aftale_flow_start''');
END
GO
IF OBJECT_ID('meta.DFMRunLog', 'U') IS NOT NULL
BEGIN
    EXEC('UPDATE meta.DFMRunLog SET table_name = ''Aftale'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''aftale''');
    EXEC('UPDATE meta.DFMRunLog SET table_name = ''dim.Aftale'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''dim.aftale''');
    EXEC('UPDATE meta.DFMRunLog SET table_name = ''map.Aftale'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''map.aftale''');
    EXEC('UPDATE meta.DFMRunLog SET table_name = ''Aftale_flow_start'' WHERE table_name COLLATE Latin1_General_100_CS_AS = ''aftale_flow_start''');
END
GO
IF OBJECT_ID('meta.SyncWatermarks', 'U') IS NOT NULL
    EXEC('UPDATE meta.SyncWatermarks SET table_name = ''Aftale'' WHERE schema_name IN (''dim'', ''map'') AND table_name COLLATE Latin1_General_100_CS_AS = ''aftale''');
GO

-- 4) DQA-referencer tilbage
IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
BEGIN
    EXEC('UPDATE dqa.Results SET subject = ''dim.Aftale'' WHERE subject COLLATE Latin1_General_100_CS_AS = ''dim.aftale''');
    EXEC('UPDATE dqa.Results SET subject = ''map.Aftale'' WHERE subject COLLATE Latin1_General_100_CS_AS = ''map.aftale''');
END
GO
IF OBJECT_ID('dqa.RemediationQueue', 'U') IS NOT NULL
    EXEC('UPDATE dqa.RemediationQueue SET replay_entity = ''Aftale'' WHERE replay_entity COLLATE Latin1_General_100_CS_AS = ''aftale''');
GO
