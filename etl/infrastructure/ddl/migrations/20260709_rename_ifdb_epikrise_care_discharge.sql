-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Hand-authored to match scripts/naming_overhaul/gen_migrations.py output.
-- Converges the 5 IFDB epikrise/udskrivningsrapport arc tables (merged via [internal ticket]
-- AFTER this branch's rename baseline) to the naming convention: table names
-- <source>_<table> prefix (matching the 4 sibling ifdb_* tables) + lowercase DWH
-- audit columns. Source columns stay verbatim; PK/IX names keep the <entity>_ifdb
-- form (sibling convention). ext tables are transient (extractor recreates), so not
-- migrated here — same as the sibling ifdb tables (0 ext entries in rename_map.yml).
-- Idempotent + guarded: no-op on a fresh DB where base DDL already created the
-- prefix/lowercase objects.

IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'epikrise_ifdb') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'ifdb_epikrise')
    EXEC sp_rename 'arc.epikrise_ifdb', 'ifdb_epikrise';
GO
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'epikrise_modtager_ifdb') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'ifdb_epikrise_modtager')
    EXEC sp_rename 'arc.epikrise_modtager_ifdb', 'ifdb_epikrise_modtager';
GO
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'epikrise_tekst_ifdb') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'ifdb_epikrise_tekst')
    EXEC sp_rename 'arc.epikrise_tekst_ifdb', 'ifdb_epikrise_tekst';
GO
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'care_discharge_report_ifdb') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'ifdb_care_discharge_report')
    EXEC sp_rename 'arc.care_discharge_report_ifdb', 'ifdb_care_discharge_report';
GO
IF EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'care_discharge_report_receiver_ifdb') AND NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name COLLATE Latin1_General_100_CS_AS = 'ifdb_care_discharge_report_receiver')
    EXEC sp_rename 'arc.care_discharge_report_receiver_ifdb', 'ifdb_care_discharge_report_receiver';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_insert')
    EXEC sp_rename 'arc.ifdb_epikrise.DW_Timestamp_Insert', 'dw_timestamp_insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_update')
    EXEC sp_rename 'arc.ifdb_epikrise.DW_Timestamp_Update', 'dw_timestamp_update', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsDeleted') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_deleted')
    EXEC sp_rename 'arc.ifdb_epikrise.DW_IsDeleted', 'dw_is_deleted', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_modtager' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_modtager' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_insert')
    EXEC sp_rename 'arc.ifdb_epikrise_modtager.DW_Timestamp_Insert', 'dw_timestamp_insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_modtager' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_modtager' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_update')
    EXEC sp_rename 'arc.ifdb_epikrise_modtager.DW_Timestamp_Update', 'dw_timestamp_update', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_modtager' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsDeleted') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_modtager' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_deleted')
    EXEC sp_rename 'arc.ifdb_epikrise_modtager.DW_IsDeleted', 'dw_is_deleted', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_tekst' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_tekst' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_insert')
    EXEC sp_rename 'arc.ifdb_epikrise_tekst.DW_Timestamp_Insert', 'dw_timestamp_insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_tekst' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_tekst' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_update')
    EXEC sp_rename 'arc.ifdb_epikrise_tekst.DW_Timestamp_Update', 'dw_timestamp_update', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_tekst' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsDeleted') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_epikrise_tekst' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_deleted')
    EXEC sp_rename 'arc.ifdb_epikrise_tekst.DW_IsDeleted', 'dw_is_deleted', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_insert')
    EXEC sp_rename 'arc.ifdb_care_discharge_report.DW_Timestamp_Insert', 'dw_timestamp_insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_update')
    EXEC sp_rename 'arc.ifdb_care_discharge_report.DW_Timestamp_Update', 'dw_timestamp_update', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsDeleted') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_deleted')
    EXEC sp_rename 'arc.ifdb_care_discharge_report.DW_IsDeleted', 'dw_is_deleted', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report_receiver' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report_receiver' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_insert')
    EXEC sp_rename 'arc.ifdb_care_discharge_report_receiver.DW_Timestamp_Insert', 'dw_timestamp_insert', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report_receiver' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report_receiver' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_update')
    EXEC sp_rename 'arc.ifdb_care_discharge_report_receiver.DW_Timestamp_Update', 'dw_timestamp_update', 'COLUMN';
GO
IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report_receiver' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsDeleted') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'ifdb_care_discharge_report_receiver' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_deleted')
    EXEC sp_rename 'arc.ifdb_care_discharge_report_receiver.DW_IsDeleted', 'dw_is_deleted', 'COLUMN';
GO
