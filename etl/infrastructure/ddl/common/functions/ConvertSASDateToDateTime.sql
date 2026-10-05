-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Function: dbo.ConvertSASDateToDateTime
-- Converts SAS datetime format to DATETIME2
-- Format: 05DEC2023:10:26:00 -> 2023-12-05 10:26:00
-- NOTE: Temporary - remove when source data is fixed
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.ConvertSASDateToDateTime
(
    @SASDateTime NVARCHAR(50)
)
RETURNS DATETIME2
AS
BEGIN
    -- Format: 05DEC2023:10:26:00
    -- Returns: 2023-12-05 10:26:00
    RETURN TRY_CONVERT(DATETIME2,
        SUBSTRING(@SASDateTime, 3, 3) + ' ' +  -- Month (DEC)
        SUBSTRING(@SASDateTime, 1, 2) + ' ' +  -- Day (05)
        SUBSTRING(@SASDateTime, 6, 4) + ' ' +  -- Year (2023)
        SUBSTRING(@SASDateTime, 11, 8)         -- Time (10:26:00)
    );
END;
GO
