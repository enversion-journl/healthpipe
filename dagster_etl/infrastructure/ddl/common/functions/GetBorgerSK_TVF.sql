-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Function: dbo.GetBorgerSK_TVF
-- Table-valued function for temporal lookup of DW_SK_Borger
-- Used by views to resolve surrogate keys for Type 2 SCD
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.GetBorgerSK_TVF
(
    @DW_EK_Borger INT,
    @PointInTime DATETIME2
)
RETURNS TABLE
AS
RETURN
(
    SELECT TOP 1
        DW_SK_Borger
    FROM dim.Borger WITH (NOLOCK)
    WHERE DW_EK_Borger = @DW_EK_Borger
      AND @PointInTime >= DW_ValidFrom
      AND @PointInTime < DW_ValidTo
);
GO
