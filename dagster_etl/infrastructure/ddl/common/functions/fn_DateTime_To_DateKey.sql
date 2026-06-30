-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Function: dbo.fn_DateTime_To_DateKey
-- Converts datetime2 to date dimension key (YYYYMMDD as INT)
-- Example: 2025-10-15 14:30:22 -> 20251015
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.fn_DateTime_To_DateKey
(
    @DateTime datetime2(7)
)
RETURNS INT
AS
BEGIN
    DECLARE @DateKey INT;

    SET @DateKey = CASE
        WHEN @DateTime IS NULL THEN NULL
        ELSE CAST(CONVERT(CHAR(8), @DateTime, 112) AS INT)
    END;

    RETURN @DateKey;
END;
GO
