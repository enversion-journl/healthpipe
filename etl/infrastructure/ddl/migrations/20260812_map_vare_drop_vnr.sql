-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- OPS-28148: map.Vare — drop vnr-kolonnen.
-- vnr var en *:1-denormalisering af Vare <-> ILSVare-relationen; relationen
-- bor nu i bridge.VareILSVare. Eneste forbruger (vnr-fallback-opslaget i
-- fct_Vareudlevering) er fjernet; dw_ek_Vare opløses alene via den
-- konstruerede LEVVARE-cleancode fra kilderækken.
-- Kolonnen bryder desuden map-konventionen (kun DW_*/CleanCode_*/bk_*).
-- map.Vare findes kun i staging-laget; DSO-kørslen springes over af guarden.
-- Idempotent: safe to re-run.
-- ============================================================================

IF OBJECT_ID(N'[map].[Vare]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[map].[Vare]', 'vnr') IS NOT NULL
BEGIN
    -- Drop evt. auto-navngiven default-constraint foer kolonnedrop
    DECLARE @df SYSNAME;
    SELECT @df = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c
        ON c.object_id = dc.parent_object_id
       AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Vare]')
      AND c.name = N'vnr';
    IF @df IS NOT NULL
        EXEC(N'ALTER TABLE [map].[Vare] DROP CONSTRAINT ' + @df);

    EXEC(N'ALTER TABLE [map].[Vare] DROP COLUMN [vnr]');
    PRINT 'Dropped vnr from [map].[Vare]';
END
GO
