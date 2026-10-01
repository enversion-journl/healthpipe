-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Function: dbo.fn_GreatestInt3  (legacy / dev-only)
-- Returns the greatest of three nullable INT values (NULL-safe).
--
-- Deployed only when staging_infra_functions / dso_infra_functions detect that
-- the target engine lacks native GREATEST() (SQL Server 2019 dev containers).
-- Production SQL Server 2022+ uses GREATEST() directly and skips this file.
--
-- PERSISTED computed columns cannot reference user-defined functions — arc
-- tables use inline CASE in kmd_uddata/00_archive.sql instead.
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.fn_GreatestInt3
(
    @A INT,
    @B INT,
    @C INT
)
RETURNS INT
AS
BEGIN
    IF @A IS NULL AND @B IS NULL AND @C IS NULL
        RETURN NULL;

    IF @A >= COALESCE(@B, @A) AND @A >= COALESCE(@C, @A)
        RETURN @A;

    IF @B >= COALESCE(@C, @B)
        RETURN @B;

    RETURN @C;
END;
GO
