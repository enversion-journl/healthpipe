-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Widen sec.RBOrganisation*.SOR_kode to VARCHAR(50) so it matches
-- dim.Organisation.kode. The hierarchy expansion feeds
-- SORHierarchy.child_code (NVARCHAR(100)) into this column; a code wider than
-- the column would fail the whole nightly load with "String or binary data
-- would be truncated".
--
-- All three swap tables are altered in the same run — SWITCH requires
-- identical column definitions. SOR_kode is in no index, so ALTER COLUMN is a
-- metadata-only change.
--
-- Idempotent: each ALTER is guarded on the current width; missing tables are
-- skipped. Runs on both staging and DSO.
-- ============================================================================

DECLARE @tables TABLE (table_name SYSNAME NOT NULL PRIMARY KEY);
INSERT INTO @tables (table_name) VALUES
    (N'RBOrganisation'),
    (N'RBOrganisation_New'),
    (N'RBOrganisation_Old');

DECLARE @t SYSNAME, @sql NVARCHAR(MAX), @widened INT = 0, @already INT = 0, @absent INT = 0;

DECLARE table_cursor CURSOR LOCAL FAST_FORWARD FOR SELECT table_name FROM @tables;
OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @t;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(N'[sec].' + QUOTENAME(@t), 'U') IS NULL
    BEGIN
        SET @absent = @absent + 1;
    END
    ELSE IF EXISTS (
        SELECT 1
        FROM sys.columns c
        WHERE c.object_id = OBJECT_ID(N'[sec].' + QUOTENAME(@t))
          AND c.name = N'SOR_kode'
          AND c.max_length < 50
    )
    BEGIN
        SET @sql = N'ALTER TABLE [sec].' + QUOTENAME(@t)
                 + N' ALTER COLUMN [SOR_kode] VARCHAR(50) NOT NULL;';
        EXEC sp_executesql @sql;
        SET @widened = @widened + 1;
    END
    ELSE
    BEGIN
        SET @already = @already + 1;
    END

    FETCH NEXT FROM table_cursor INTO @t;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

PRINT 'sec.RBOrganisation*.SOR_kode -> VARCHAR(50): ' + CAST(@widened AS NVARCHAR(10))
    + ' widened, ' + CAST(@already AS NVARCHAR(10)) + ' already wide enough, '
    + CAST(@absent AS NVARCHAR(10)) + ' table(s) not on this database (of 3).';
GO
