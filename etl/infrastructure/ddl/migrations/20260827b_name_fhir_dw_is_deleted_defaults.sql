-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.*.dw_is_deleted defaults: DF_FHIR_<Table>_DwIsDeleted -> DF_<Table>_dw_is_deleted
--
-- 20260825b_name_default_constraints.sql names every column default
-- DF_<table>_<column>. Its original list only had three fhir dw_is_deleted
-- rows (Patient, Patient_Address, Organization). This branch then added the
-- remaining 21 fhir flags in 20260825_fhir_dw_is_deleted_not_null.sql under the
-- old names (DF_FHIR_Encounter_DwIsDeleted, ...).
--
-- On Dev DSA that order already happened, and 20260825b is stamped:
--   20260825b_name_default_constraints.sql          2026-08-26 14:42
--   20260825_fhir_dw_is_deleted_not_null.sql        2026-08-27 06:59
-- Editing 20260825b after that stamp is a no-op. Patient was renamed to
-- DF_Patient_dw_is_deleted on the 26th, then 20260825_fhir dropped the default
-- for the BIT -> INT change and re-added DF_FHIR_Patient_DwIsDeleted. The other
-- tables that gained the column on the 27th never went through the naming pass.
--
-- tables/fhir/01_tables.sql now declares DF_<Table>_dw_is_deleted, which fixes
-- freshly provisioned databases. This migration brings already-deployed ones
-- (Dev DSA and any peer that applied 20260825b before 20260825_fhir) to the
-- same names.
--
-- Only the constraint NAME changes; DEFAULT 0 is untouched. sp_rename is
-- metadata-only.
--
-- Per table, guarded so a re-run is a no-op:
--   * table not on this database          -> skip (staging/DSO split)
--   * no default constraint on the column -> report, leave alone
--   * already on the new name             -> no-op
--   * anything else                       -> sp_rename to the new name
--
-- Name matching is case-sensitive (Latin1_General_100_CS_AS), consistent with
-- 20260825b. Runs on BOTH staging and DSO via the shared migrations/ directory.
-- ============================================================================

DECLARE @defaults TABLE (
    table_name SYSNAME NOT NULL PRIMARY KEY,
    new_name   SYSNAME NOT NULL
);

INSERT INTO @defaults (table_name, new_name) VALUES
    (N'Appointment', N'DF_Appointment_dw_is_deleted'),
    (N'CarePlan', N'DF_CarePlan_dw_is_deleted'),
    (N'CarePlan_Activity', N'DF_CarePlan_Activity_dw_is_deleted'),
    (N'Condition', N'DF_Condition_dw_is_deleted'),
    (N'Condition_tillaegskode', N'DF_Condition_tillaegskode_dw_is_deleted'),
    (N'DocumentReference', N'DF_DocumentReference_dw_is_deleted'),
    (N'DocumentReference_Recipient', N'DF_DocumentReference_Recipient_dw_is_deleted'),
    (N'Encounter', N'DF_Encounter_dw_is_deleted'),
    (N'Encounter_Location', N'DF_Encounter_Location_dw_is_deleted'),
    (N'EpisodeOfCare', N'DF_EpisodeOfCare_dw_is_deleted'),
    (N'Location', N'DF_Location_dw_is_deleted'),
    (N'Observation', N'DF_Observation_dw_is_deleted'),
    (N'Organization', N'DF_Organization_dw_is_deleted'),
    (N'Patient', N'DF_Patient_dw_is_deleted'),
    (N'Patient_Address', N'DF_Patient_Address_dw_is_deleted'),
    (N'Procedure', N'DF_Procedure_dw_is_deleted'),
    (N'Procedure_tillaegskode', N'DF_Procedure_tillaegskode_dw_is_deleted'),
    (N'Questionnaire', N'DF_Questionnaire_dw_is_deleted'),
    (N'QuestionnaireResponse', N'DF_QuestionnaireResponse_dw_is_deleted'),
    (N'QuestionnaireResponse_item', N'DF_QuestionnaireResponse_item_dw_is_deleted'),
    (N'Questionnaire_item', N'DF_Questionnaire_item_dw_is_deleted'),
    (N'ServiceRequest', N'DF_ServiceRequest_dw_is_deleted'),
    (N'ServiceRequest_Recipient', N'DF_ServiceRequest_Recipient_dw_is_deleted'),
    (N'ServiceRequest_tillaegskode', N'DF_ServiceRequest_tillaegskode_dw_is_deleted');

DECLARE @table SYSNAME, @new SYSNAME;
DECLARE @old SYSNAME, @object_id INT, @qualified NVARCHAR(776);
DECLARE @renamed INT = 0, @already INT = 0, @absent INT = 0, @no_default INT = 0;

DECLARE default_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT table_name, new_name FROM @defaults;

OPEN default_cursor;
FETCH NEXT FROM default_cursor INTO @table, @new;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @object_id = NULL, @old = NULL;

    SELECT @object_id = t.object_id
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = N'fhir' AND t.name = @table;

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
        WHERE dc.parent_object_id = @object_id AND c.name = N'dw_is_deleted';

        IF @old IS NULL
        BEGIN
            SET @no_default = @no_default + 1;
            PRINT 'No default constraint to rename: fhir.' + @table + '.dw_is_deleted';
        END
        ELSE IF @old COLLATE Latin1_General_100_CS_AS = @new COLLATE Latin1_General_100_CS_AS
        BEGIN
            SET @already = @already + 1;
        END
        ELSE
        BEGIN
            SET @qualified = N'[fhir].' + QUOTENAME(@old);
            EXEC sp_rename @qualified, @new, 'OBJECT';
            SET @renamed = @renamed + 1;
            PRINT 'Renamed ' + @old + ' -> ' + @new;
        END
    END

    FETCH NEXT FROM default_cursor INTO @table, @new;
END

CLOSE default_cursor;
DEALLOCATE default_cursor;

PRINT 'fhir dw_is_deleted default naming: '
    + CAST(@renamed AS NVARCHAR(10)) + ' renamed, '
    + CAST(@already AS NVARCHAR(10)) + ' already correct, '
    + CAST(@absent AS NVARCHAR(10)) + ' table(s) not on this database, '
    + CAST(@no_default AS NVARCHAR(10)) + ' column(s) without a default.';
GO

-- === Slutkontrol: fejl HOEJT hvis en omdoebning ikke naaede at koere ===
--
-- Loekken ovenfor ligger i ÉN batch. Det er samme moenster som
-- 20260825b_name_default_constraints.sql og fint til 24 hurtige sp_rename-kald
-- -- men paa Dev DSA stoppede en tilsvarende loekke i
-- 20260825_fhir_dw_is_deleted_not_null.sql efter én tabel, mens migrationen
-- rapporterede succes. Derfor denne kontrol: er der stadig et forkert navn
-- tilbage, fejler filen, apply_migrations ruller tilbage og stempler den ikke
-- som anvendt. Bedre at fejle end at rapportere et arbejde der ikke er gjort.

DECLARE @tilbage NVARCHAR(MAX) = N'';

SELECT @tilbage = @tilbage + N' ' + t.name
FROM sys.default_constraints AS dc
JOIN sys.tables AS t
  ON t.object_id = dc.parent_object_id
JOIN sys.columns AS c
  ON c.object_id = dc.parent_object_id
 AND c.column_id = dc.parent_column_id
WHERE t.schema_id = SCHEMA_ID(N'fhir')
  AND c.name = N'dw_is_deleted'
  AND dc.name COLLATE Latin1_General_100_CS_AS
      <> (N'DF_' + t.name + N'_dw_is_deleted') COLLATE Latin1_General_100_CS_AS;

IF @tilbage <> N''
BEGIN
    DECLARE @msg NVARCHAR(2048) =
        N'dw_is_deleted-defaults har stadig forkert navn paa:' + @tilbage
        + N' -- omdoebningen er ikke gjort faerdig.';
    RAISERROR(@msg, 16, 1);
END
GO
