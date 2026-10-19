-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Refresh views over dim.Borger after the rebuild.
--
-- 20260924_dim_borger_fhir_patient_id.sql recreates dim.Borger with a new
-- column at position 3. A view that is not schemabound keeps the column list
-- from its creation; a view with SELECT * therefore shows shifted columns
-- until it is refreshed.
--
-- Covers all views in the database that depend on dim.Borger. Views in other
-- databases must be refreshed there. Runs on staging and DSO and can be rerun.
-- ============================================================================

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
BEGIN
    DECLARE @view NVARCHAR(517);
    DECLARE views CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT QUOTENAME(OBJECT_SCHEMA_NAME(d.referencing_id)) + N'.'
                      + QUOTENAME(OBJECT_NAME(d.referencing_id))
        FROM sys.sql_expression_dependencies AS d
        INNER JOIN sys.views AS v ON v.object_id = d.referencing_id
        WHERE d.referenced_id = OBJECT_ID(N'[dim].[Borger]')
          AND OBJECTPROPERTY(d.referencing_id, 'IsSchemaBound') = 0;
    OPEN views;
    FETCH NEXT FROM views INTO @view;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT N'Refreshing ' + @view;
        EXEC sys.sp_refreshview @view;
        FETCH NEXT FROM views INTO @view;
    END
    CLOSE views;
    DEALLOCATE views;
END
GO
