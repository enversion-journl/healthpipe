-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ref.FakturalinjeMatchKeys.dw_ek_Dato_faktura: DF_Ref_... -> DF_...
--
-- 20260825b_name_default_constraints.sql put all 504 column defaults on
-- DF_<table>_<column>. dw_ek_Dato_faktura escaped it: the column was added on a
-- parallel branch (20260824_matchkeys_dato_faktura_and_delta_index.sql) as
-- DF_Ref_FakturalinjeMatchKeys_dw_ek_Dato_faktura, and 20260825b works from a
-- fixed (schema, table, column) list that predates the merge, so the rename pass
-- never visits it. 20260824 sorts BEFORE 20260825b, so on an already-deployed
-- database the constraint exists under the old name by the time the naming pass
-- runs, and is skipped.
--
-- tables/ref/01_tables.sql now declares the new name, which fixes freshly
-- provisioned databases. This migration brings deployed ones to the same name.
--
-- Only the constraint NAME changes; DEFAULT -1 is untouched.
--
--   * table not on this database          -> skip (staging/DSO split)
--   * no default constraint on the column -> report, leave alone
--   * already on the new name             -> no-op
--   * anything else                       -> sp_rename to the new name
--
-- On a fresh database 20260824's ADD COLUMN is guarded by NOT EXISTS and never
-- runs, so the old name is never created and this migration is a no-op.
--
-- Name matching is case-sensitive (Latin1_General_100_CS_AS), consistent with
-- 20260825b, so a rename differing only in case is not read as already-applied.
-- Every branch is guarded, so re-running is a no-op.
-- ============================================================================

DECLARE @schema SYSNAME = N'ref',
        @table  SYSNAME = N'FakturalinjeMatchKeys',
        @column SYSNAME = N'dw_ek_Dato_faktura',
        @new    SYSNAME = N'DF_FakturalinjeMatchKeys_dw_ek_Dato_faktura';

DECLARE @object_id INT, @old SYSNAME, @qualified NVARCHAR(776);

SELECT @object_id = t.object_id
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = @schema AND t.name = @table;

IF @object_id IS NULL
BEGIN
    PRINT 'Table not on this database, skipping: ' + @schema + '.' + @table;
END
ELSE
BEGIN
    SELECT @old = dc.name
    FROM sys.default_constraints dc
    JOIN sys.columns c ON c.object_id = dc.parent_object_id
                      AND c.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = @object_id AND c.name = @column;

    IF @old IS NULL
        PRINT 'No default constraint to rename: '
            + @schema + '.' + @table + '.' + @column;
    ELSE IF @old COLLATE Latin1_General_100_CS_AS = @new COLLATE Latin1_General_100_CS_AS
        PRINT 'Default constraint already named ' + @new + ', no-op.';
    ELSE
    BEGIN
        SET @qualified = QUOTENAME(@schema) + N'.' + QUOTENAME(@old);
        EXEC sp_rename @qualified, @new, 'OBJECT';
        PRINT 'Renamed ' + @old + ' -> ' + @new;
    END
END
GO
