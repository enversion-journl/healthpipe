-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename legacy audit columns from DW_Insert_DateTime / DW_Update_DateTime
-- to DW_Timestamp_Insert / DW_Timestamp_Update (standard naming convention).
--
-- Applies to: map.LagerbeholdningEgenskaber, map.AfdelingLagerbeholdningEgenskaber,
--             map.Lagerbeholdning, map.AfdelingLagerbeholdning
-- Safe to re-run: IF EXISTS guards on old column names.
-- Task: #27720
-- ============================================================================

-- ---- map.LagerbeholdningEgenskaber ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename '[map].[LagerbeholdningEgenskaber].[DW_Insert_DateTime]', 'DW_Timestamp_Insert', 'COLUMN';
    PRINT 'Renamed DW_Insert_DateTime -> DW_Timestamp_Insert on map.LagerbeholdningEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename '[map].[LagerbeholdningEgenskaber].[DW_Update_DateTime]', 'DW_Timestamp_Update', 'COLUMN';
    PRINT 'Renamed DW_Update_DateTime -> DW_Timestamp_Update on map.LagerbeholdningEgenskaber';
END
GO

-- ---- map.AfdelingLagerbeholdningEgenskaber ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename '[map].[AfdelingLagerbeholdningEgenskaber].[DW_Insert_DateTime]', 'DW_Timestamp_Insert', 'COLUMN';
    PRINT 'Renamed DW_Insert_DateTime -> DW_Timestamp_Insert on map.AfdelingLagerbeholdningEgenskaber';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename '[map].[AfdelingLagerbeholdningEgenskaber].[DW_Update_DateTime]', 'DW_Timestamp_Update', 'COLUMN';
    PRINT 'Renamed DW_Update_DateTime -> DW_Timestamp_Update on map.AfdelingLagerbeholdningEgenskaber';
END
GO

-- ---- map.Lagerbeholdning ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename '[map].[Lagerbeholdning].[DW_Insert_DateTime]', 'DW_Timestamp_Insert', 'COLUMN';
    PRINT 'Renamed DW_Insert_DateTime -> DW_Timestamp_Insert on map.Lagerbeholdning';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename '[map].[Lagerbeholdning].[DW_Update_DateTime]', 'DW_Timestamp_Update', 'COLUMN';
    PRINT 'Renamed DW_Update_DateTime -> DW_Timestamp_Update on map.Lagerbeholdning';
END
GO

-- ---- map.AfdelingLagerbeholdning ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND name = 'DW_Insert_DateTime')
BEGIN
    EXEC sp_rename '[map].[AfdelingLagerbeholdning].[DW_Insert_DateTime]', 'DW_Timestamp_Insert', 'COLUMN';
    PRINT 'Renamed DW_Insert_DateTime -> DW_Timestamp_Insert on map.AfdelingLagerbeholdning';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND name = 'DW_Update_DateTime')
BEGIN
    EXEC sp_rename '[map].[AfdelingLagerbeholdning].[DW_Update_DateTime]', 'DW_Timestamp_Update', 'COLUMN';
    PRINT 'Renamed DW_Update_DateTime -> DW_Timestamp_Update on map.AfdelingLagerbeholdning';
END
GO
