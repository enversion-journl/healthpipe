-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop unused map tables that are no longer referenced in the repository
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Markoerfejl' AND SCHEMA_NAME(schema_id) = 'map')
BEGIN
    DROP TABLE [map].[Markoerfejl];
    PRINT 'Dropped [map].[Markoerfejl]';
END
ELSE
BEGIN
    PRINT '[map].[Markoerfejl] does not exist - nothing to drop';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'MarkoerforloebDetail' AND SCHEMA_NAME(schema_id) = 'map'
)
BEGIN
    DROP TABLE [map].[MarkoerforloebDetail];
    PRINT 'Dropped [map].[MarkoerforloebDetail]';
END
ELSE
BEGIN
    PRINT '[map].[MarkoerforloebDetail] does not exist - nothing to drop';
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Organisation' AND SCHEMA_NAME(schema_id) = 'map')
BEGIN
    DROP TABLE [map].[Organisation];
    PRINT 'Dropped [map].[Organisation]';
END
ELSE
BEGIN
    PRINT '[map].[Organisation] does not exist - nothing to drop';
END
GO
