-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Provenance columns on fhir.Location, and a reset of both Location checkpoints
--
-- The SOR-keyed Location is assembled by DSA/FHIR/Location/location_builder.py
-- from one contribution per source (BOM_IFDB, IFDB). meta_source is the builder's
-- URI; each source_* flag mirrors one meta.tag and says whether that source
-- contributed to the location.
--
-- A delta asset reposts a Location only when its archive row changes, so the
-- Locations already in the FHIR server keep the old resource without meta. With
-- the fhir_server / Location and fhir_server / Location_SOR checkpoints at
-- 1900-01-01, the next ordinary run of both assets rebuilds every SOR code, and
-- the SQL-on-FHIR projection fills the new columns.
--
-- In-place ALTER. RUNS ON BOTH DATABASES. No-op where the table is absent or the
-- column already exists. vw_fhir.Location is SELECT *, so it is refreshed to the
-- new column list. The DFM state for fhir_server lives in staging, so each reset
-- is fenced on its checkpoint existing; it is a no-op on DSO and on a database
-- where the asset has not run, whose first run is a full load. The file fails
-- before it changes anything if meta.DFMState exists without meta.usp_dfm_reset.
--
-- Base DDL in tables/fhir/01_tables.sql is the fresh-database source of truth.
-- =============================================================================

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL
    THROW 50014,
        N'meta.DFMState exists but meta.usp_dfm_reset does not - materialise staging_infra_dfm before this migration. Without the reset, the Locations in the FHIR server never get their provenance.',
        1;
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Location'
         AND COLUMN_NAME = N'meta_source'
   )
    ALTER TABLE [fhir].[Location] ADD [meta_source] NVARCHAR(255) NULL;
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Location'
         AND COLUMN_NAME = N'source_bom_ifdb'
   )
    ALTER TABLE [fhir].[Location] ADD [source_bom_ifdb] BIT NULL;
GO

IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'fhir' AND TABLE_NAME = N'Location'
         AND COLUMN_NAME = N'source_ifdb'
   )
    ALTER TABLE [fhir].[Location] ADD [source_ifdb] BIT NULL;
GO

IF OBJECT_ID(N'[vw_fhir].[Location]', 'V') IS NOT NULL
    EXEC sp_refreshview N'[vw_fhir].[Location]';
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NULL
    PRINT 'No meta.DFMState on this database (expected on DSO) - no checkpoint to reset.';
ELSE
    EXEC sp_executesql N'
        DECLARE @reason NVARCHAR(200) = N''Location built by field ownership with provenance: every SOR code is rebuilt by the ordinary delta.'';

        IF EXISTS (SELECT 1 FROM [meta].[DFMState]
                   WHERE [scope] = ''staging'' AND [target_type] = ''fhir_resource''
                     AND [schema_name] = ''fhir_server'' AND [table_name] = ''Location'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir_server'', @table_name = ''Location'',
                 @new_value = ''1900-01-01 00:00:00'', @reason = @reason, @scope = ''staging'';
            PRINT ''Location checkpoint reset to 1900-01-01; the next ordinary run rebuilds every SOR code.'';
        END
        ELSE
            PRINT ''Location has not run on this database - its first run is a full load.'';

        IF EXISTS (SELECT 1 FROM [meta].[DFMState]
                   WHERE [scope] = ''staging'' AND [target_type] = ''fhir_resource''
                     AND [schema_name] = ''fhir_server'' AND [table_name] = ''Location_SOR'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir_server'', @table_name = ''Location_SOR'',
                 @new_value = ''1900-01-01 00:00:00'', @reason = @reason, @scope = ''staging'';
            PRINT ''Location_SOR checkpoint reset to 1900-01-01; the next ordinary run rebuilds every SOR code.'';
        END
        ELSE
            PRINT ''Location_SOR has not run on this database - its first run is a full load.'';';
GO
