-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Security Functions
-- DPA_ERP fritekst-maskering (kolonne-maskering). sec.HentBrugerRolleDomaene
-- ligger i 05_dpa_functions.sql (efter 04_dpa_tables.sql, så sec.RolleDomaene
-- findes ved fresh-install DDL-apply).
-- ============================================================================

-- sec.BeskytDimLeverandoervare_v1 - Mask PII-risk free-text columns in dim.Leverandoervare based on role.
-- Free-text columns (varenavn/varenavn_mysupply) and the supplier item number
-- (varenummer_kreditor, which can be CPR-formatted) are masked unless the caller
-- has Rolle = 'U' in the security domain. Downstream reporting joins use the
-- surrogate key dw_ek_Leverandoervare, so masking these columns does not break joins.
CREATE OR ALTER FUNCTION [sec].[BeskytDimLeverandoervare_v1] (
    @Rolle CHAR(1),
    @varenummer_kreditor NVARCHAR(255),
    @varenavn NVARCHAR(MAX),
    @varenavn_mysupply NVARCHAR(MAX)
)
RETURNS TABLE
AS
RETURN (
    SELECT
        [varenummer_kreditor] = CASE
            WHEN @Rolle = 'U' THEN @varenummer_kreditor
            ELSE N'[MASKERET]'
        END,
        [varenavn] = CASE
            WHEN @Rolle = 'U' THEN @varenavn
            ELSE N'[MASKERET]'
        END,
        [varenavn_mysupply] = CASE
            WHEN @Rolle = 'U' THEN @varenavn_mysupply
            ELSE N'[MASKERET]'
        END,
        [DPASikkerhed] = CASE WHEN @Rolle = 'U' THEN 'UB' ELSE 'KB' END
);
GO
