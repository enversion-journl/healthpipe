-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop the unused vw_dm schema
-- Only drops if the schema exists and contains no objects.
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'vw_dm')
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID('vw_dm')
    )
    BEGIN
        EXEC('DROP SCHEMA [vw_dm]');
        PRINT 'Dropped empty schema [vw_dm]';
    END
    ELSE
    BEGIN
        PRINT 'Schema [vw_dm] still contains objects — skipping drop';
    END
END
ELSE
BEGIN
    PRINT 'Schema [vw_dm] does not exist — nothing to drop';
END
GO
