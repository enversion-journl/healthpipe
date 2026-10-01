-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 1. Rename legacy audit columns from DW_Insert_DateTime / DW_Update_DateTime
--    to DW_Timestamp_Insert / DW_Timestamp_Update (standard naming convention).
--
--    Applies to: map.Organization, dim.Organization
--    Safe to re-run: IF EXISTS guards on old column names.
-- ============================================================================

-- ---- map.Organization ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organization]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename '[map].[Organization].[DW_Insert_DateTime]', 'DW_Timestamp_Insert', 'COLUMN';
    PRINT 'Renamed DW_Insert_DateTime -> DW_Timestamp_Insert on map.Organization';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organization]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename '[map].[Organization].[DW_Update_DateTime]', 'DW_Timestamp_Update', 'COLUMN';
    PRINT 'Renamed DW_Update_DateTime -> DW_Timestamp_Update on map.Organization';
END
GO

-- ---- dim.Organization ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organization]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename '[dim].[Organization].[DW_Insert_DateTime]', 'DW_Timestamp_Insert', 'COLUMN';
    PRINT 'Renamed DW_Insert_DateTime -> DW_Timestamp_Insert on dim.Organization';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organization]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename '[dim].[Organization].[DW_Update_DateTime]', 'DW_Timestamp_Update', 'COLUMN';
    PRINT 'Renamed DW_Update_DateTime -> DW_Timestamp_Update on dim.Organization';
END
GO
