-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- dw_is_deleted på XML/SoF-barnetabellerne: bring DEFAULT-constraintens navn på
-- den fælles form DF_<tabel>_<kolonne>.
--
-- Kolonnen blev første gang tilføjet med den gamle navneform
-- (DF_FHIR_<Tabel>_DwIsDeleted). Databaser, der nåede at køre den udgave, står
-- derfor tilbage med et navn, ingen anden constraint i modellen bruger længere,
-- mens en frisk database får det nye navn fra tables/. De to veje skal ende
-- samme sted, ellers kan constrainten ikke slås op på navn.
--
-- Opslaget sker på KOLONNEN, ikke på det gamle navn: en database, hvor
-- constrainten aldrig fik et eksplicit navn, bærer et systemopfundet navn
-- (DF__Procedure__dw_is__1A2B3C4D), og det skal med. Kun navnet ændres —
-- DEFAULT-udtrykket røres ikke.
--
-- Pr. (skema, tabel, kolonne), så en genkørsel er en no-op:
--   * tabellen findes ikke her          -> spring over (staging/DSO-opdelingen)
--   * ingen DEFAULT på kolonnen         -> rapportér, lad den være
--   * navnet er allerede det rigtige    -> no-op
--   * alt andet                         -> sp_rename til det nye navn
--
-- Navnesammenligningen er versalfølsom (Latin1_General_100_CS_AS), så en
-- omdøbning der kun adskiller sig i store/små bogstaver ikke forveksles med
-- "allerede kørt". Kører på både staging og DSO via den fælles migrations-mappe.
-- =============================================================================

DECLARE @defaults TABLE (
    schema_name  SYSNAME NOT NULL,
    table_name   SYSNAME NOT NULL,
    column_name  SYSNAME NOT NULL,
    new_name     SYSNAME NOT NULL,
    PRIMARY KEY (schema_name, table_name, column_name)
);

INSERT INTO @defaults (schema_name, table_name, column_name, new_name) VALUES
    (N'fhir', N'Procedure_tillaegskode', N'dw_is_deleted', N'DF_Procedure_tillaegskode_dw_is_deleted'),
    (N'fhir', N'Condition_tillaegskode', N'dw_is_deleted', N'DF_Condition_tillaegskode_dw_is_deleted'),
    (N'fhir', N'ServiceRequest_tillaegskode', N'dw_is_deleted', N'DF_ServiceRequest_tillaegskode_dw_is_deleted'),
    (N'fhir', N'QuestionnaireResponse_item', N'dw_is_deleted', N'DF_QuestionnaireResponse_item_dw_is_deleted');

DECLARE @schema SYSNAME, @table SYSNAME, @column SYSNAME, @new SYSNAME;
DECLARE @old SYSNAME, @object_id INT, @qualified NVARCHAR(776);
DECLARE @renamed INT = 0, @already INT = 0, @absent INT = 0, @no_default INT = 0;

DECLARE child_default_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT schema_name, table_name, column_name, new_name FROM @defaults;

OPEN child_default_cursor;
FETCH NEXT FROM child_default_cursor INTO @schema, @table, @column, @new;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @object_id = NULL, @old = NULL;

    SELECT @object_id = t.object_id
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = @schema AND t.name = @table;

    IF @object_id IS NULL
    BEGIN
        SET @absent = @absent + 1;
    END
    ELSE
    BEGIN
        SELECT @old = dc.name
        FROM sys.default_constraints dc
        JOIN sys.columns c ON c.object_id = dc.parent_object_id
                          AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = @object_id AND c.name = @column;

        IF @old IS NULL
        BEGIN
            SET @no_default = @no_default + 1;
            PRINT 'No default constraint to rename: ' + @schema + '.' + @table + '.' + @column;
        END
        ELSE IF @old COLLATE Latin1_General_100_CS_AS = @new COLLATE Latin1_General_100_CS_AS
        BEGIN
            SET @already = @already + 1;
        END
        ELSE
        BEGIN
            SET @qualified = QUOTENAME(@schema) + N'.' + QUOTENAME(@old);
            EXEC sp_rename @qualified, @new, 'OBJECT';
            SET @renamed = @renamed + 1;
        END
    END

    FETCH NEXT FROM child_default_cursor INTO @schema, @table, @column, @new;
END

CLOSE child_default_cursor;
DEALLOCATE child_default_cursor;

PRINT 'XML/SoF child-grain dw_is_deleted defaults: ' + CAST(@renamed AS NVARCHAR(10))
    + ' renamed, ' + CAST(@already AS NVARCHAR(10)) + ' already correct, '
    + CAST(@absent AS NVARCHAR(10)) + ' table(s) not on this database, '
    + CAST(@no_default AS NVARCHAR(10)) + ' column(s) without a default (of 4 known).';
GO
