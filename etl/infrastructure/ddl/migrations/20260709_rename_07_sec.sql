-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Manual gap-fix for the 20260708 naming overhaul (same class as the 06_stg
-- gap): "sec" was missing from DOMAINS in scripts/naming_overhaul/gen_migrations.py,
-- so the 6 sec.* column entries in rename_map.yml were never emitted into any
-- migration file. Index renames (90) and dqa state rows (95) DID cover sec,
-- only the column sp_renames were dropped. Without this, 05_dpa_functions.sql
-- fails on existing databases: sec.HentBrugerRolleDomaene references
-- dw_is_deleted, which the live sec.RolleDomaene still calls DW_IsDeleted.
-- Statements match the gen_migrations.py column_stmt output exactly.

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RBOrganisation' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'Bruger') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RBOrganisation' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'bruger')
    EXEC sp_rename 'sec.RBOrganisation.Bruger', 'bruger', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RBOrganisation' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsDeleted') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RBOrganisation' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_deleted')
    EXEC sp_rename 'sec.RBOrganisation.DW_IsDeleted', 'dw_is_deleted', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RBOrganisation' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RBOrganisation' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_update')
    EXEC sp_rename 'sec.RBOrganisation.DW_Timestamp_Update', 'dw_timestamp_update', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RolleDomaene' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'Bruger') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RolleDomaene' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'bruger')
    EXEC sp_rename 'sec.RolleDomaene.Bruger', 'bruger', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RolleDomaene' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsDeleted') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RolleDomaene' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_deleted')
    EXEC sp_rename 'sec.RolleDomaene.DW_IsDeleted', 'dw_is_deleted', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RolleDomaene' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Timestamp_Update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'RolleDomaene' AND SCHEMA_NAME(t.schema_id) = 'sec' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_timestamp_update')
    EXEC sp_rename 'sec.RolleDomaene.DW_Timestamp_Update', 'dw_timestamp_update', 'COLUMN';
GO
