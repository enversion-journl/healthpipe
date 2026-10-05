-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: EncounterLocations PK from 2-column to 3-column
-- WI: Part of Encounter pipeline (pre-existing, moved from 01_tables.sql)
-- PR: Extracted per WI 27263 migration convention
--
-- Migrates PK from (encounter_id, location_id) to
-- (encounter_id, location_id, location_period_start).
-- Handles: sentinel NULLs, dedup of colliding rows, column NOT NULL, PK swap.
-- Idempotent: skips if the 3-column PK is already in place.
-- =============================================================================

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fhir].[EncounterLocations]') AND type = 'U')
BEGIN
    -- Check if the correct 3-column PK already exists
    IF NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints kc
        JOIN sys.index_columns ic
            ON kc.parent_object_id = ic.object_id
           AND kc.unique_index_id = ic.index_id
        JOIN sys.columns c
            ON c.object_id = ic.object_id
           AND c.column_id = ic.column_id
        WHERE kc.parent_object_id = OBJECT_ID(N'[fhir].[EncounterLocations]')
          AND kc.type = 'PK'
        GROUP BY kc.name
        HAVING COUNT(*) = 3
           AND MAX(CASE WHEN ic.key_ordinal = 1 AND c.name = N'encounter_id' THEN 1 ELSE 0 END) = 1
           AND MAX(CASE WHEN ic.key_ordinal = 2 AND c.name = N'location_id' THEN 1 ELSE 0 END) = 1
           AND MAX(CASE WHEN ic.key_ordinal = 3 AND c.name = N'location_period_start' THEN 1 ELSE 0 END) = 1
    )
    BEGIN
        -- Step 1: Replace NULLs with sentinel value in PK columns
        UPDATE [fhir].[EncounterLocations]
        SET [location_id] = N''
        WHERE [location_id] IS NULL;

        UPDATE [fhir].[EncounterLocations]
        SET [location_period_start] = N''
        WHERE [location_period_start] IS NULL;

        -- Step 2: Deduplicate rows that would collide on the new 3-column PK.
        -- Keeps the row with the latest location_period_end, then latest dw_timestamp_update.
        WITH cte AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [encounter_id], [location_id], [location_period_start]
                       ORDER BY [location_period_end] DESC, [dw_timestamp_update] DESC
                   ) AS rn
            FROM [fhir].[EncounterLocations]
        )
        DELETE FROM cte WHERE rn > 1;

        -- Step 3: Ensure NOT NULL on PK columns
        IF EXISTS (
            SELECT 1
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(N'[fhir].[EncounterLocations]')
              AND c.name = N'location_id'
              AND c.is_nullable = 1
        )
        BEGIN
            ALTER TABLE [fhir].[EncounterLocations]
            ALTER COLUMN [location_id] NVARCHAR(255) NOT NULL;
        END

        IF EXISTS (
            SELECT 1
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(N'[fhir].[EncounterLocations]')
              AND c.name = N'location_period_start'
              AND c.is_nullable = 1
        )
        BEGIN
            ALTER TABLE [fhir].[EncounterLocations]
            ALTER COLUMN [location_period_start] NVARCHAR(50) NOT NULL;
        END

        -- Step 4: Drop existing PK (whatever shape it is)
        DECLARE @EncLocPkName SYSNAME;
        DECLARE @DropEncLocPkSql NVARCHAR(400);

        SELECT TOP (1) @EncLocPkName = kc.name
        FROM sys.key_constraints kc
        WHERE kc.parent_object_id = OBJECT_ID(N'[fhir].[EncounterLocations]')
          AND kc.type = 'PK';

        IF @EncLocPkName IS NOT NULL
        BEGIN
            SET @DropEncLocPkSql =
                N'ALTER TABLE [fhir].[EncounterLocations] DROP CONSTRAINT '
                + QUOTENAME(@EncLocPkName) + N';';
            EXEC sp_executesql @DropEncLocPkSql;
        END

        -- Step 5: Create new 3-column PK
        ALTER TABLE [fhir].[EncounterLocations]
        ADD PRIMARY KEY CLUSTERED ([encounter_id], [location_id], [location_period_start]);
    END
END
GO
