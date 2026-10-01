-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Widen dim.Kreditor.Cvrnr from NVARCHAR(20) to NVARCHAR(50)
--
-- DimUpd_kreditor derives Cvrnr as COALESCE(JuridiskEnhed, ils.se_nr/cvr_nr,
-- LeverandorCVR). Those same source values are also stored in the sibling
-- columns Cvrnr_Ils / Cvrnr_MySupply / Cvrnr_SydAX, which are already
-- NVARCHAR(50). Cvrnr was under-sized at NVARCHAR(20), so a source CVR longer
-- than 20 chars (e.g. a concatenated SE/DK identifier like '556872124401DK...')
-- overflowed only the derived column and aborted the MERGE with
-- "String or binary data would be truncated ... column 'Cvrnr'".
--
-- Widening to NVARCHAR(50) aligns Cvrnr with its source-specific siblings: any
-- value that fits those columns now also fits Cvrnr. Widening preserves data.
--
-- Safe to re-run: guarded on the column still being narrower than NVARCHAR(50).
-- CHARACTER_MAXIMUM_LENGTH is -1 for NVARCHAR(MAX), so the guard uses BETWEEN
-- 1 AND 49 — a drifted MAX column is left alone instead of being narrowed.
-- ============================================================================

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim'
      AND TABLE_NAME   = 'Kreditor'
      AND COLUMN_NAME  = 'Cvrnr'
      AND DATA_TYPE    = 'nvarchar'
      AND CHARACTER_MAXIMUM_LENGTH BETWEEN 1 AND 49
)
BEGIN
    ALTER TABLE [dim].[Kreditor]
        ALTER COLUMN [Cvrnr] NVARCHAR(50) NULL;
    PRINT 'Widened dim.Kreditor.Cvrnr to NVARCHAR(50)';
END
GO
