-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Function: dbo.GetBorgerSK_TVF
-- Table-valued function for temporal lookup of dw_sk_Borger
-- Used by views to resolve surrogate keys for Type 2 SCD
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.GetBorgerSK_TVF
(
    @dw_ek_Borger INT,
    @PointInTime DATETIME2
)
RETURNS TABLE
AS
RETURN
(
    SELECT TOP 1
        dw_sk_Borger
    FROM dim.Borger WITH (NOLOCK)
    WHERE dw_ek_Borger = @dw_ek_Borger
      AND @PointInTime >= dw_valid_from
      AND @PointInTime < dw_valid_to
);
GO
