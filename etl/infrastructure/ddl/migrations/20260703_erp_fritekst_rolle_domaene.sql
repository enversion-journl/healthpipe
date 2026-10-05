-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP fritekst security: migrate vw_dim.Vare/Fakturalinje from BrugerRoller.Fritekst
-- to sec.RolleDomaene (DPA_ERP, Rolle U/K).
-- ============================================================================

IF OBJECT_ID(N'[sec].[HentFritekstAdgang_v1]', 'IF') IS NOT NULL
BEGIN
    DROP FUNCTION [sec].[HentFritekstAdgang_v1];
    PRINT 'Dropped function sec.HentFritekstAdgang_v1';
END
GO

DECLARE @df_fritekst SYSNAME;
SELECT @df_fritekst = [dc].[name]
FROM [sys].[default_constraints] AS [dc]
INNER JOIN [sys].[columns] AS [c]
    ON [dc].[parent_object_id] = [c].[object_id]
   AND [dc].[parent_column_id] = [c].[column_id]
WHERE [dc].[parent_object_id] = OBJECT_ID(N'[sec].[BrugerRoller]')
  AND [c].[name] = N'Fritekst';

IF @df_fritekst IS NOT NULL
BEGIN
    EXEC(N'ALTER TABLE [sec].[BrugerRoller] DROP CONSTRAINT [' + @df_fritekst + N']');
    PRINT 'Dropped default constraint on sec.BrugerRoller.Fritekst';
END
GO

IF EXISTS (
    SELECT 1
    FROM [sys].[columns]
    WHERE [object_id] = OBJECT_ID(N'[sec].[BrugerRoller]')
      AND [name] = N'Fritekst'
)
BEGIN
    ALTER TABLE [sec].[BrugerRoller] DROP COLUMN [Fritekst];
    PRINT 'Dropped column sec.BrugerRoller.Fritekst';
END
GO
