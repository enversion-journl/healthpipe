-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.*_tillaegskode -> fhir.*_SupplementaryCode
--
--   fhir.Condition_tillaegskode      -> fhir.Condition_SupplementaryCode
--   fhir.Procedure_tillaegskode      -> fhir.Procedure_SupplementaryCode
--   fhir.ServiceRequest_tillaegskode -> fhir.ServiceRequest_SupplementaryCode
--
-- og kolonnerne tillaegskode_kode/_system/_display -> supplementary_code,
-- supplementary_code_system, supplementary_code_display.
--
-- HVORFOR: fhir-laget er engelsk. Tabellerne er FHIR-ressourcenavne, og
-- child-tabellerne baerer FHIR-elementnavne (Patient_Address, CarePlan_Activity,
-- DocumentReference_Recipient, Questionnaire_item). Disse tre var de eneste med
-- et dansk ord, og ordet fulgte dem gennem hvert lag -- view, SoF-asset,
-- DSO-noegle, lineage-filnavn. "SupplementaryCode" er den etablerede engelske
-- term for tillaegskoder i SKS og ICD-10, og daekker praecis det, grainet
-- indeholder: de kodninger paa ressourcens code, som brugeren IKKE valgte
-- (userSelected != true).
--
-- ext- og arc-laget roeres IKKE: de spejler kilden 1:1, saa
-- arc.ifdb_diagnose_tillaegskode og kildens egne kolonnenavne bliver staaende.
-- Det samme gaelder dim/stg-laget (DRG-broerne), hvor navngivningsstandarden
-- foreskriver danske forretningsbegreber.
--
-- sp_rename omdoeber IKKE constraints og indeks. De omdoebes derfor eksplicit
-- nedenfor -- ellers ender en deployet database med PK_FHIR_Condition_tillaegskode
-- mens en frisk database faar PK_FHIR_Condition_SupplementaryCode. Det er
-- praecis den drift 20260825b_name_default_constraints.sql ryddede op i for 504
-- constraints, og en senere guarded migration, der slaar op paa constraint-navn,
-- ville stille no-oppe paa den ene af de to.
--
-- Per objekt, guarded saa en re-run er en no-op og staging/DSO-splittet er
-- sikkert (et objekt, der ikke findes paa denne database, springes over).
-- Data bevares: dette er rene omdoebninger, ingen kopiering.
--
-- RAEKKEFOELGE: migrationen koeres af full_infrastructure_setup, som koeres ved
-- hvert deploy. Men natjobbet indeholder staging_infra_fhir, og naar det naar at
-- koere mellem deploy og full_infrastructure_setup, opretter base-DDL'en de nye
-- tabeller TOMME, foer migrationen har omdoebt de gamle. Migrationen taaler det:
-- en tom maaltabel droppes, saa den rigtige kan omdoebes (se afsnittet
-- «Tom maaltabel» nedenfor), og checkpoint-omdoebningen springer en raekke
-- over, hvis maalnavnet allerede har en.
-- ============================================================================
SET NOCOUNT ON;
GO

-- ---------------------------------------------------------------------------
-- Tom maaltabel fra base-DDL'en: fjernes, saa den rigtige kan omdoebes
--
-- Naar natjobbet naar at koere mellem deploy og full_infrastructure_setup,
-- opretter staging_infra_fhir de nye tabeller tomme (base-DDL'en kender kun de
-- nye navne). Saa ville omdoebningen nedenfor springe over, fordi maalet
-- findes, og den gamle tabel -- den med data -- blev staaende som en
-- foraeldreloes rest, mens SoF genindlaeste alt under det nye navn.
-- En tom maaltabel har intet at miste: den droppes sammen med en eventuel
-- checkpoint-raekke, og den gamle tabel og dens checkpoint omdoebes som normalt.
-- Har maaltabellen raekker, roeres ingen af de to: saa er der allerede
-- indlaest under det nye navn, og det er ikke migrationens sag at vaelge
-- mellem to datasaet.
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[Condition_SupplementaryCode]', N'U') IS NOT NULL
BEGIN
    DECLARE @tom BIT;
    EXEC sp_executesql
        N'SELECT @t = CASE WHEN EXISTS (SELECT 1 FROM [fhir].[Condition_SupplementaryCode]) THEN 0 ELSE 1 END',
        N'@t BIT OUTPUT', @t = @tom OUTPUT;
    IF @tom = 1
    BEGIN
        DROP TABLE [fhir].[Condition_SupplementaryCode];
        IF OBJECT_ID(N'[meta].[DFMState]', N'U') IS NOT NULL
            DELETE FROM [meta].[DFMState]
             WHERE [schema_name] = 'fhir' AND [table_name] = N'Condition_SupplementaryCode';
        PRINT 'Droppede tom fhir.Condition_SupplementaryCode, saa den eksisterende tabel kan omdoebes';
    END
    ELSE
        PRINT 'ADVARSEL: baade fhir.Condition_tillaegskode og en ikke-tom fhir.Condition_SupplementaryCode findes -- ingen af dem roeres';
END
GO

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[Procedure_SupplementaryCode]', N'U') IS NOT NULL
BEGIN
    DECLARE @tom BIT;
    EXEC sp_executesql
        N'SELECT @t = CASE WHEN EXISTS (SELECT 1 FROM [fhir].[Procedure_SupplementaryCode]) THEN 0 ELSE 1 END',
        N'@t BIT OUTPUT', @t = @tom OUTPUT;
    IF @tom = 1
    BEGIN
        DROP TABLE [fhir].[Procedure_SupplementaryCode];
        IF OBJECT_ID(N'[meta].[DFMState]', N'U') IS NOT NULL
            DELETE FROM [meta].[DFMState]
             WHERE [schema_name] = 'fhir' AND [table_name] = N'Procedure_SupplementaryCode';
        PRINT 'Droppede tom fhir.Procedure_SupplementaryCode, saa den eksisterende tabel kan omdoebes';
    END
    ELSE
        PRINT 'ADVARSEL: baade fhir.Procedure_tillaegskode og en ikke-tom fhir.Procedure_SupplementaryCode findes -- ingen af dem roeres';
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[ServiceRequest_SupplementaryCode]', N'U') IS NOT NULL
BEGIN
    DECLARE @tom BIT;
    EXEC sp_executesql
        N'SELECT @t = CASE WHEN EXISTS (SELECT 1 FROM [fhir].[ServiceRequest_SupplementaryCode]) THEN 0 ELSE 1 END',
        N'@t BIT OUTPUT', @t = @tom OUTPUT;
    IF @tom = 1
    BEGIN
        DROP TABLE [fhir].[ServiceRequest_SupplementaryCode];
        IF OBJECT_ID(N'[meta].[DFMState]', N'U') IS NOT NULL
            DELETE FROM [meta].[DFMState]
             WHERE [schema_name] = 'fhir' AND [table_name] = N'ServiceRequest_SupplementaryCode';
        PRINT 'Droppede tom fhir.ServiceRequest_SupplementaryCode, saa den eksisterende tabel kan omdoebes';
    END
    ELSE
        PRINT 'ADVARSEL: baade fhir.ServiceRequest_tillaegskode og en ikke-tom fhir.ServiceRequest_SupplementaryCode findes -- ingen af dem roeres';
END
GO

-- ---------------------------------------------------------------------------
-- Condition
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[Condition_SupplementaryCode]', N'U') IS NULL
    EXEC sp_rename 'fhir.Condition_tillaegskode', 'Condition_SupplementaryCode';
GO

IF COL_LENGTH(N'[fhir].[Condition_SupplementaryCode]', N'tillaegskode_kode') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition_SupplementaryCode]', N'supplementary_code') IS NULL
    EXEC sp_rename 'fhir.Condition_SupplementaryCode.tillaegskode_kode', 'supplementary_code', 'COLUMN';
GO

IF COL_LENGTH(N'[fhir].[Condition_SupplementaryCode]', N'tillaegskode_system') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition_SupplementaryCode]', N'supplementary_code_system') IS NULL
    EXEC sp_rename 'fhir.Condition_SupplementaryCode.tillaegskode_system', 'supplementary_code_system', 'COLUMN';
GO

IF COL_LENGTH(N'[fhir].[Condition_SupplementaryCode]', N'tillaegskode_display') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition_SupplementaryCode]', N'supplementary_code_display') IS NULL
    EXEC sp_rename 'fhir.Condition_SupplementaryCode.tillaegskode_display', 'supplementary_code_display', 'COLUMN';
GO

-- ---------------------------------------------------------------------------
-- Procedure
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[Procedure_SupplementaryCode]', N'U') IS NULL
    EXEC sp_rename 'fhir.Procedure_tillaegskode', 'Procedure_SupplementaryCode';
GO

IF COL_LENGTH(N'[fhir].[Procedure_SupplementaryCode]', N'tillaegskode_kode') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure_SupplementaryCode]', N'supplementary_code') IS NULL
    EXEC sp_rename 'fhir.Procedure_SupplementaryCode.tillaegskode_kode', 'supplementary_code', 'COLUMN';
GO

IF COL_LENGTH(N'[fhir].[Procedure_SupplementaryCode]', N'tillaegskode_system') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure_SupplementaryCode]', N'supplementary_code_system') IS NULL
    EXEC sp_rename 'fhir.Procedure_SupplementaryCode.tillaegskode_system', 'supplementary_code_system', 'COLUMN';
GO

IF COL_LENGTH(N'[fhir].[Procedure_SupplementaryCode]', N'tillaegskode_display') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure_SupplementaryCode]', N'supplementary_code_display') IS NULL
    EXEC sp_rename 'fhir.Procedure_SupplementaryCode.tillaegskode_display', 'supplementary_code_display', 'COLUMN';
GO

-- ---------------------------------------------------------------------------
-- ServiceRequest
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[ServiceRequest_SupplementaryCode]', N'U') IS NULL
    EXEC sp_rename 'fhir.ServiceRequest_tillaegskode', 'ServiceRequest_SupplementaryCode';
GO

IF COL_LENGTH(N'[fhir].[ServiceRequest_SupplementaryCode]', N'tillaegskode_kode') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_SupplementaryCode]', N'supplementary_code') IS NULL
    EXEC sp_rename 'fhir.ServiceRequest_SupplementaryCode.tillaegskode_kode', 'supplementary_code', 'COLUMN';
GO

IF COL_LENGTH(N'[fhir].[ServiceRequest_SupplementaryCode]', N'tillaegskode_system') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_SupplementaryCode]', N'supplementary_code_system') IS NULL
    EXEC sp_rename 'fhir.ServiceRequest_SupplementaryCode.tillaegskode_system', 'supplementary_code_system', 'COLUMN';
GO

IF COL_LENGTH(N'[fhir].[ServiceRequest_SupplementaryCode]', N'tillaegskode_display') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_SupplementaryCode]', N'supplementary_code_display') IS NULL
    EXEC sp_rename 'fhir.ServiceRequest_SupplementaryCode.tillaegskode_display', 'supplementary_code_display', 'COLUMN';
GO

-- ---------------------------------------------------------------------------
-- Constraints og indeks: sp_rename tager dem ikke med
-- ---------------------------------------------------------------------------
DECLARE @renames TABLE (old_name SYSNAME, new_name SYSNAME, kind NVARCHAR(20), tbl SYSNAME);
INSERT INTO @renames (old_name, new_name, kind, tbl) VALUES
    (N'PK_FHIR_Condition_tillaegskode', N'PK_FHIR_Condition_SupplementaryCode', N'OBJECT', N'Condition_SupplementaryCode'),
    (N'DF_Condition_tillaegskode_dw_is_deleted', N'DF_Condition_SupplementaryCode_dw_is_deleted', N'OBJECT', N'Condition_SupplementaryCode'),
    (N'DF_Condition_tillaegskode_dw_timestamp_insert', N'DF_Condition_SupplementaryCode_dw_timestamp_insert', N'OBJECT', N'Condition_SupplementaryCode'),
    (N'DF_Condition_tillaegskode_dw_timestamp_update', N'DF_Condition_SupplementaryCode_dw_timestamp_update', N'OBJECT', N'Condition_SupplementaryCode'),
    (N'PK_FHIR_Procedure_tillaegskode', N'PK_FHIR_Procedure_SupplementaryCode', N'OBJECT', N'Procedure_SupplementaryCode'),
    (N'DF_Procedure_tillaegskode_dw_is_deleted', N'DF_Procedure_SupplementaryCode_dw_is_deleted', N'OBJECT', N'Procedure_SupplementaryCode'),
    (N'DF_Procedure_tillaegskode_dw_timestamp_insert', N'DF_Procedure_SupplementaryCode_dw_timestamp_insert', N'OBJECT', N'Procedure_SupplementaryCode'),
    (N'DF_Procedure_tillaegskode_dw_timestamp_update', N'DF_Procedure_SupplementaryCode_dw_timestamp_update', N'OBJECT', N'Procedure_SupplementaryCode'),
    (N'PK_FHIR_ServiceRequest_tillaegskode', N'PK_FHIR_ServiceRequest_SupplementaryCode', N'OBJECT', N'ServiceRequest_SupplementaryCode'),
    (N'DF_ServiceRequest_tillaegskode_dw_is_deleted', N'DF_ServiceRequest_SupplementaryCode_dw_is_deleted', N'OBJECT', N'ServiceRequest_SupplementaryCode'),
    (N'DF_ServiceRequest_tillaegskode_dw_timestamp_insert', N'DF_ServiceRequest_SupplementaryCode_dw_timestamp_insert', N'OBJECT', N'ServiceRequest_SupplementaryCode'),
    (N'DF_ServiceRequest_tillaegskode_dw_timestamp_update', N'DF_ServiceRequest_SupplementaryCode_dw_timestamp_update', N'OBJECT', N'ServiceRequest_SupplementaryCode'),
    (N'IX_FHIR_ConditionTillaegskode_ConditionId', N'IX_FHIR_Condition_SupplementaryCode_ConditionId', N'INDEX', N'Condition_SupplementaryCode'),
    (N'IX_FHIR_ConditionTillaegskode_LastUpdated', N'IX_FHIR_Condition_SupplementaryCode_LastUpdated', N'INDEX', N'Condition_SupplementaryCode'),
    (N'IX_FHIR_ProcedureTillaegskode_ProcedureId', N'IX_FHIR_Procedure_SupplementaryCode_ProcedureId', N'INDEX', N'Procedure_SupplementaryCode'),
    (N'IX_FHIR_ProcedureTillaegskode_LastUpdated', N'IX_FHIR_Procedure_SupplementaryCode_LastUpdated', N'INDEX', N'Procedure_SupplementaryCode'),
    (N'IX_FHIR_ServiceRequestTillaegskode_SrId', N'IX_FHIR_ServiceRequest_SupplementaryCode_SrId', N'INDEX', N'ServiceRequest_SupplementaryCode'),
    (N'IX_FHIR_ServiceRequestTillaegskode_LastUpdated', N'IX_FHIR_ServiceRequest_SupplementaryCode_LastUpdated', N'INDEX', N'ServiceRequest_SupplementaryCode');

DECLARE @old SYSNAME, @new SYSNAME, @kind NVARCHAR(20), @tbl SYSNAME, @target NVARCHAR(400);
DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT old_name, new_name, kind, tbl FROM @renames;
OPEN cur;
FETCH NEXT FROM cur INTO @old, @new, @kind, @tbl;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF @kind = N'OBJECT'
    BEGIN
        -- Constraint: findes den gamle og ikke den nye, saa omdoeb.
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = @old AND SCHEMA_NAME(schema_id) = N'fhir')
           AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE name = @new AND SCHEMA_NAME(schema_id) = N'fhir')
        BEGIN
            SET @target = N'[fhir].[' + @old + N']';
            EXEC sp_rename @target, @new, N'OBJECT';
        END
    END
    ELSE
    BEGIN
        -- Indeks: navnet er kun unikt pr. tabel, saa target er tabel.indeks.
        IF OBJECT_ID(N'[fhir].[' + @tbl + N']', N'U') IS NOT NULL
           AND EXISTS (SELECT 1 FROM sys.indexes WHERE name = @old
                       AND object_id = OBJECT_ID(N'[fhir].[' + @tbl + N']'))
           AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = @new
                           AND object_id = OBJECT_ID(N'[fhir].[' + @tbl + N']'))
        BEGIN
            SET @target = N'[fhir].[' + @tbl + N'].[' + @old + N']';
            EXEC sp_rename @target, @new, N'INDEX';
        END
    END
    FETCH NEXT FROM cur INTO @old, @new, @kind, @tbl;
END
CLOSE cur;
DEALLOCATE cur;
GO

-- ----------------------------------------------------------------------------
-- DFM-tilstanden foelger tabelnavnet
--
-- Baade SoF-projektionen og DSO-synken noegler deres checkpoint paa tabellens
-- navn (meta.DFMState: scope/target_type/schema_name/table_name). Uden denne
-- omskrivning ville den omdoebte tabel staa uden checkpoint -- foerste
-- koersel efter udrulningen ville lave en fuld genindlaesning fra Aidbox, og
-- den gamle raekke ville blive staaende som en foraeldreloes rest.
-- Samme greb som 20260708_rename_95_state_rows brugte ved arkivets omdoebning.
-- Uafhaengig af scope, saa baade staging- og DSO-databasens egne raekker
-- rammes, naar migrationen koerer paa dem hver isaer.
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[meta].[DFMState]', N'U') IS NOT NULL
BEGIN
    -- Hegnet mod et maalnavn, der allerede har en raekke (PK er scope/
    -- target_type/schema_name/table_name): uden hegnet ville en raekke, som
    -- SoF har oprettet under det nye navn, give primaernoeglebrud og standse
    -- alle efterfoelgende migrationer.
    UPDATE s
       SET [table_name] = REPLACE(s.[table_name], '_tillaegskode', '_SupplementaryCode')
      FROM [meta].[DFMState] AS s
     WHERE s.[schema_name] = 'fhir'
       AND s.[table_name] IN (N'Condition_tillaegskode',
                              N'Procedure_tillaegskode',
                              N'ServiceRequest_tillaegskode')
       AND NOT EXISTS (
            SELECT 1 FROM [meta].[DFMState] AS n
             WHERE n.[scope] = s.[scope]
               AND n.[target_type] = s.[target_type]
               AND n.[schema_name] = s.[schema_name]
               AND n.[table_name] = REPLACE(s.[table_name], '_tillaegskode', '_SupplementaryCode'));
    PRINT CONCAT(N'Omdoebte ', @@ROWCOUNT, N' checkpoint-raekker i meta.DFMState');
END
GO

IF OBJECT_ID(N'[meta].[DFMRunLog]', N'U') IS NOT NULL
BEGIN
    UPDATE [meta].[DFMRunLog]
       SET [table_name] = REPLACE([table_name], '_tillaegskode', '_SupplementaryCode')
     WHERE [table_name] IN (N'Condition_tillaegskode',
                            N'Procedure_tillaegskode',
                            N'ServiceRequest_tillaegskode');
    PRINT CONCAT(N'Omdoebte ', @@ROWCOUNT, N' raekker i meta.DFMRunLog');
END
GO
