-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop the old PascalCase [conceptmap].[*_To_SOR] tables on DSO.
--
-- DSO conceptmap targets are now lowercase ([conceptmap].[*_to_sor], created by
-- tables/conceptmap/dso/) to match the staging naming. Data re-derives from the
-- staging conceptmap.vw_* views via dso_sync_conceptmap_*, so a plain DROP is safe.
-- On case-insensitive collations OBJECT_ID('[conceptmap].[Rekvirent_To_SOR]')
-- also matches the new lowercase table, so guard on the exact PascalCase name
-- via sys.tables with a case-sensitive collation to avoid dropping the replacement.
-- DSO-only migration.
-- ============================================================================

DECLARE @pascal TABLE (name NVARCHAR(128));
INSERT INTO @pascal (name) VALUES
    (N'Rekvirent_To_SOR'), (N'Lager_To_SOR'), (N'EAN_To_SOR'), (N'Okosted_To_SOR');

DECLARE @name NVARCHAR(128), @sql NVARCHAR(MAX);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @pascal;
OPEN c;
FETCH NEXT FROM c INTO @name;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Match the exact PascalCase name (case/accent-sensitive) so we never drop
    -- the new lowercase table on a case-insensitive server.
    IF EXISTS (
        SELECT 1 FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = N'conceptmap'
          AND t.name = @name COLLATE Latin1_General_CS_AS
    )
    BEGIN
        SET @sql = N'DROP TABLE [conceptmap].[' + @name + N'];';
        EXEC sp_executesql @sql;
        PRINT 'Dropped conceptmap.' + @name;
    END
    FETCH NEXT FROM c INTO @name;
END
CLOSE c;
DEALLOCATE c;
GO
