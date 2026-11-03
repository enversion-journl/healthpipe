-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DEFAULTs on dw_timestamp_* columns read UTC: SYSDATETIME() becomes
-- SYSUTCDATETIME() and GETDATE() becomes GETUTCDATE().
--
-- DSO stamps its own writes in UTC, so a DSO stamp is when DSO wrote the row in
-- one clock that only moves forward. The DSO sync sets the stamps explicitly;
-- the DEFAULTs cover writers that leave them out. The staging server runs UTC,
-- so on staging no value changes.
--
-- Each constraint keeps its name and its CAST; only the clock function is
-- swapped. A constraint that already reads UTC is not matched, so the file can
-- be rerun.
--
-- The migration matches every dw_timestamp_* DEFAULT in the database, also on
-- ext.* tables that DSI code creates; those keep GETDATE() in their CREATE
-- TABLE, which on staging (UTC) gives the same value.
--
-- Base DDL: every dw_timestamp_* DEFAULT under tables/. Runs on staging and DSO.
-- ============================================================================

SET XACT_ABORT ON;
GO

DECLARE @sql NVARCHAR(MAX);

SELECT @sql = STRING_AGG(
    CAST(
        N'ALTER TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
        + N' DROP CONSTRAINT ' + QUOTENAME(dc.name) + N'; '
        + N'ALTER TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
        + N' ADD CONSTRAINT ' + QUOTENAME(dc.name) + N' DEFAULT '
        + REPLACE(REPLACE(dc.definition, N'sysdatetime()', N'sysutcdatetime()'),
                  N'getdate()', N'getutcdate()')
        + N' FOR ' + QUOTENAME(c.name) + N';'
        AS NVARCHAR(MAX)),
    NCHAR(10))
FROM sys.default_constraints AS dc
INNER JOIN sys.tables AS t ON t.object_id = dc.parent_object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
INNER JOIN sys.columns AS c
    ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE c.name LIKE N'dw[_]timestamp[_]%'
  AND (dc.definition LIKE N'%sysdatetime()%' OR dc.definition LIKE N'%getdate()%');

IF @sql IS NOT NULL
BEGIN
    BEGIN TRANSACTION;
    EXEC sp_executesql @sql;
    COMMIT;
    PRINT 'Switched dw_timestamp_* DEFAULTs to UTC';
END
GO
