-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Function: dbo.fn_DateTime_To_TimeKey
-- Converts datetime2 to time dimension key (HHmm as INT)
-- Example: 14:30:22 -> 1430
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.fn_DateTime_To_TimeKey
(
    @DateTime datetime2(7)
)
RETURNS INT
AS
BEGIN
    DECLARE @TimeKey INT;

    SET @TimeKey = CASE
        WHEN @DateTime IS NULL THEN NULL
        ELSE CAST(FORMAT(@DateTime, 'HHmm') AS INT)
    END;

    RETURN @TimeKey;
END;
GO
