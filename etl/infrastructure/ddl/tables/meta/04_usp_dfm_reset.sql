-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DFM Reset Stored Procedure
--
-- Atomically resets a checkpoint and logs the reset in DFMRunLog.
-- Callable from SSMS, scripts, or the Dagster dfm_reset asset.
-- ============================================================================

CREATE OR ALTER PROCEDURE [meta].[usp_dfm_reset]
    @schema_name    NVARCHAR(128),
    @table_name     NVARCHAR(128),
    @new_value      NVARCHAR(50),
    @reason         NVARCHAR(MAX) = NULL,
    @scope          NVARCHAR(50)  = 'staging'
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;

    DECLARE @target_type NVARCHAR(50) = CASE @schema_name
        WHEN 'ext'         THEN 'extract'
        WHEN 'arc'         THEN 'archive'
        WHEN 'pre_stg'     THEN 'pre_staging'
        WHEN 'stg'         THEN 'staging'
        WHEN 'dim'         THEN 'dimension'
        WHEN 'fct'         THEN 'fact'
        WHEN 'dm'          THEN 'datamart'
        WHEN 'map'         THEN 'mapping'
        WHEN 'bridge'      THEN 'bridge'
        WHEN 'fhir_stg'    THEN 'fhir_staging'
        WHEN 'fhir'        THEN 'fhir_table'
        WHEN 'fhir_server' THEN 'fhir_resource'
        WHEN 'codesystem'  THEN 'codesystem'
        WHEN 'conceptmap'  THEN 'conceptmap'
        WHEN 'ref'         THEN 'reference'
        WHEN 'sec'         THEN 'security'
        ELSE @schema_name
    END;

    DECLARE @old_value NVARCHAR(50);

    SELECT @old_value = [checkpoint_value]
    FROM [meta].[DFMState]
    WHERE [scope]       = @scope
      AND [target_type] = @target_type
      AND [schema_name] = @schema_name
      AND [table_name]  = @table_name;

    MERGE [meta].[DFMState] AS tgt
    USING (SELECT @scope, @target_type, @schema_name, @table_name)
        AS src (scope, target_type, schema_name, table_name)
    ON  tgt.[scope]       = src.[scope]
    AND tgt.[target_type] = src.[target_type]
    AND tgt.[schema_name] = src.[schema_name]
    AND tgt.[table_name]  = src.[table_name]
    WHEN MATCHED THEN
        UPDATE SET
            [checkpoint_value] = @new_value,
            [last_mode]        = 'reset',
            [updated_at]       = CAST(SYSDATETIME() AS datetime2(0))
    WHEN NOT MATCHED THEN
        INSERT ([scope], [target_type], [schema_name], [table_name],
                [checkpoint_value], [last_mode])
        VALUES (@scope, @target_type, @schema_name, @table_name,
                @new_value, 'reset');

    INSERT INTO [meta].[DFMRunLog]
        ([scope], [target_type], [schema_name], [table_name],
         [mode], [status],
         [checkpoint_before], [checkpoint_after],
         [error_message],
         [run_started_at], [run_completed_at])
    VALUES
        (@scope, @target_type, @schema_name, @table_name,
         'reset', 'success',
         @old_value, @new_value,
         @reason,
         CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0)));

    COMMIT;
END
GO
