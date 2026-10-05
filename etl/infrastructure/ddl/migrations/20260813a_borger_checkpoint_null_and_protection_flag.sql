-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Borger chain: NULL checkpoints + address_protection NOT NULL
--
-- WHY:
--   1. 20260812 reset fhir.Patient_Address / ref / dim to '1900-01-01'. That
--      still takes the SoF delta path (full_load is partition_start is None),
--      so the first scheduled run does 1-2M parent-id inserts against an empty
--      table. NULL is a true full_load.
--   2. fhir/Patient was not reset. address_protection (and district) only fill
--      as each citizen's Aidbox row churns; dormant records stay NULL forever.
--   3. address_protection / adressebeskyttelse were BIT NULL, so `= 0` dropped
--      every unprotected row. Same hazard dw_is_deleted was hardened against.
--
-- RUNS ON BOTH DATABASES. Checkpoint reset no-ops where meta.DFMState is
-- absent (DSO). Column alters apply wherever the tables exist.
--
-- Base DDL in tables/fhir/01_tables.sql and tables/shared_dimensions/02_dims.sql
-- is the fresh-database source of truth.
-- =============================================================================

-- ---------- address_protection / adressebeskyttelse → NOT NULL DEFAULT 0 ----------
IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
         AND name = N'address_protection'
         AND is_nullable = 1
   )
BEGIN
    EXEC(N'UPDATE [fhir].[Patient] SET [address_protection] = 0 WHERE [address_protection] IS NULL');
    IF NOT EXISTS (
        SELECT 1
        FROM sys.default_constraints dc
        INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
        WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient]')
          AND c.name = N'address_protection'
    )
        ALTER TABLE [fhir].[Patient]
            ADD CONSTRAINT [DF_FHIR_Patient_AddressProtection] DEFAULT 0 FOR [address_protection];
    ALTER TABLE [fhir].[Patient] ALTER COLUMN [address_protection] BIT NOT NULL;
END
GO

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
         AND name = N'adressebeskyttelse'
         AND is_nullable = 1
   )
BEGIN
    EXEC(N'UPDATE [dim].[Borger] SET [adressebeskyttelse] = 0 WHERE [adressebeskyttelse] IS NULL');
    IF NOT EXISTS (
        SELECT 1
        FROM sys.default_constraints dc
        INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
        WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Borger]')
          AND c.name = N'adressebeskyttelse'
    )
        ALTER TABLE [dim].[Borger]
            ADD CONSTRAINT [DF_Borger_Adressebeskyttelse] DEFAULT 0 FOR [adressebeskyttelse];
    ALTER TABLE [dim].[Borger] ALTER COLUMN [adressebeskyttelse] BIT NOT NULL;
END
GO

-- ---------- DFM checkpoints → NULL (true full_load on the next scheduled run) ----------
IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL
    THROW 50011,
        N'meta.DFMState exists but meta.usp_dfm_reset does not - materialise staging_infra_dfm before this migration. Skipping the checkpoint reset would leave fhir.Patient / Patient_Address with a current checkpoint.',
        1;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NULL
    PRINT 'No meta.DFMState on this database (expected on DSO) - no checkpoints to reset.';
ELSE
    EXEC sp_executesql N'
        DECLARE @reason NVARCHAR(200) = N''NULL checkpoints so the next scheduled run is a true full_load of the Borger chain, including fhir.Patient for address_protection.'';
        DECLARE @reset INT = 0;

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''fhir'' AND [table_name] = ''Patient'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir'', @table_name = ''Patient'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''fhir'' AND [table_name] = ''Patient_Address'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir'', @table_name = ''Patient_Address'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''ref'' AND [table_name] = ''Patient_Address_To_Borger'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''ref'', @table_name = ''Patient_Address_To_Borger'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''dim'' AND [table_name] = ''Borger'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''dim'', @table_name = ''Borger'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        PRINT CONCAT(''Reset '', @reset, '' DFM checkpoint(s) to NULL for the Borger chain.'');';
GO
