-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename DW_EK_Organization -> DW_EK_Organisation on fct.Finanskladdelinje.
--
-- Follow-up to 20260427_rename_organization_to_organisation.sql, which missed
-- fct.Finanskladdelinje because that table was added on a parallel branch.
-- Also renames the supporting nonclustered index.
-- Safe to re-run: every step is guarded.
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = 'DW_EK_Organization')
BEGIN
    EXEC sp_rename '[fct].[Finanskladdelinje].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
    PRINT 'Renamed DW_EK_Organization -> DW_EK_Organisation on fct.Finanskladdelinje';
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_Fct_Finanskladdelinje_Organization'
      AND object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
)
BEGIN
    EXEC sp_rename
        N'[fct].[Finanskladdelinje].[IX_Fct_Finanskladdelinje_Organization]',
        N'IX_Fct_Finanskladdelinje_Organisation',
        N'INDEX';
    PRINT 'Renamed index IX_Fct_Finanskladdelinje_Organization -> IX_Fct_Finanskladdelinje_Organisation';
END
GO
