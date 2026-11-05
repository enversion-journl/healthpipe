-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: remove dim.Kreditor members minted from ILS giro = 'Person' rows
--
-- stg_dim_Kreditor minted CVR_{se_nr} for every arc.ils_lev row with a se_nr,
-- whatever its giro. For giro = 'Person' (home patients, employees with
-- expenses) se_nr is a birth date or CPR number, so map.Kreditor/dim.Kreditor
-- gained members keyed on personal data, with a person's name and address in
-- the *_ils columns. The ILS chains now give those rows no clean_code_Kreditor
-- (utils.sql.tax_id_sql.sql_ils_kreditor_cleancode); this migration cleans up
-- what they already wrote.
--
-- A member is removed when its clean_code_Kreditor comes ONLY from ILS
-- giro = 'Person' rows: no non-Person ILS row, no SydAX and no MySupply row
-- yields the same code. The CleanCode formulas below are inlined copies of
-- utils.sql.tax_id_sql (se_nr / JuridiskEnhed / LeverandorCVR) as they stand on
-- this date.
--
-- 1) Every dim/fct/ref/bridge reference to a removed member is repointed to
--    -1, with dw_timestamp_update stamped so the DSO watermark sync carries it.
-- 2) The members are deleted from dim.Kreditor and map.Kreditor. The DSO sync
--    does not delete, so DSO keeps them until the ERP data there is reset and
--    reloaded.
-- 3) Kept members whose ILS presence came only from Person rows get
--    dw_source_ils = 0 (the map MERGE keeps flags sticky, it would never clear it).
-- 4) The staging-scope dim.Kreditor checkpoint is cleared, so the next
--    dim_Kreditor run reprocesses every member and drops the *_ils attributes
--    a Person row had supplied. The DSO sync watermark row ('dso' scope) is kept.
--
-- Runs on BOTH staging and DSO via the central migrations runner; it does its
-- work only where the archives live (staging) and is a no-op on DSO. Idempotent:
-- a rerun finds no Person-only members left.

SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'[arc].[ils_lev]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[arc].[sydax_kreditor]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[arc].[mysupply_metadata]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[Kreditor]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[Kreditor]', 'U') IS NOT NULL
BEGIN
    DROP TABLE IF EXISTS #ils_codes;
    DROP TABLE IF EXISTS #other_codes;
    DROP TABLE IF EXISTS #person_only;

    -- ILS codes per row, flagged Person / non-Person
    SELECT DISTINCT
        'CVR_' + CASE WHEN p.packed LIKE 'DK[0-9]%'
                      THEN SUBSTRING(p.packed, 3, LEN(p.packed))
                      ELSE p.packed END AS clean_code_kreditor,
        CASE WHEN lev.giro = 'Person' THEN 1 ELSE 0 END AS er_person
    INTO #ils_codes
    FROM [arc].[ils_lev] lev
    CROSS APPLY (
        SELECT REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') AS packed
    ) p
    WHERE lev.se_nr IS NOT NULL
      AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> '';

    -- Codes that keep a member: non-Person ILS, SydAX, MySupply
    SELECT clean_code_kreditor
    INTO #other_codes
    FROM #ils_codes
    WHERE er_person = 0

    UNION

    SELECT 'CVR_' + CASE WHEN t.trimmed LIKE 'DK[0-9]%'
                         THEN SUBSTRING(t.trimmed, 3, LEN(t.trimmed))
                         ELSE t.trimmed END
    FROM [arc].[sydax_kreditor] k
    CROSS APPLY (SELECT LTRIM(RTRIM(k.JuridiskEnhed)) AS trimmed) t
    WHERE k.JuridiskEnhed IS NOT NULL AND t.trimmed <> ''

    UNION

    SELECT 'CVR_' + CASE WHEN t.trimmed LIKE '[A-Z][A-Z][0-9]%'
                         THEN SUBSTRING(t.trimmed, 3, LEN(t.trimmed))
                         ELSE t.trimmed END
    FROM [arc].[mysupply_metadata] m
    CROSS APPLY (SELECT LTRIM(RTRIM(m.LeverandorCVR)) AS trimmed) t
    WHERE m.LeverandorCVR IS NOT NULL AND t.trimmed <> '';

    SELECT mk.dw_ek_Kreditor
    INTO #person_only
    FROM [map].[Kreditor] mk
    WHERE mk.dw_ek_Kreditor <> -1
      AND EXISTS (
          SELECT 1 FROM #ils_codes i
          WHERE i.er_person = 1 AND i.clean_code_kreditor = mk.clean_code_kreditor
      )
      AND NOT EXISTS (
          SELECT 1 FROM #other_codes o
          WHERE o.clean_code_kreditor = mk.clean_code_kreditor
      );

    CREATE UNIQUE CLUSTERED INDEX IX_person_only ON #person_only (dw_ek_Kreditor);

    DECLARE @removed INT = (SELECT COUNT(*) FROM #person_only);
    PRINT CONCAT('Person-only Kreditor members: ', @removed);

    -- 1) Repoint every reference (dim.Kreditor itself is deleted below)
    DECLARE @sql NVARCHAR(MAX);

    SELECT @sql = STRING_AGG(
        CAST(
            N'UPDATE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
            + N' SET [dw_ek_Kreditor] = -1'
            + CASE WHEN ts.column_id IS NOT NULL
                   THEN N', [dw_timestamp_update] = CAST(SYSUTCDATETIME() AS datetime2(0))'
                   ELSE N'' END
            + N' WHERE [dw_ek_Kreditor] IN (SELECT dw_ek_Kreditor FROM #person_only);'
            + N' PRINT CONCAT(''Repointed ' + s.name + N'.' + t.name + N': '', @@ROWCOUNT);'
        AS NVARCHAR(MAX)),
        NCHAR(10)
    )
    FROM sys.columns c
    JOIN sys.tables t ON t.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    LEFT JOIN sys.columns ts
        ON ts.object_id = t.object_id AND ts.name = N'dw_timestamp_update'
    WHERE c.name = N'dw_ek_Kreditor'
      AND s.name IN (N'dim', N'fct', N'ref', N'bridge')
      AND NOT (s.name = N'dim' AND t.name = N'Kreditor');

    IF @removed > 0 AND @sql IS NOT NULL
        EXEC sys.sp_executesql @sql;

    -- 2) Delete the members
    DELETE d
    FROM [dim].[Kreditor] d
    INNER JOIN #person_only p ON p.dw_ek_Kreditor = d.dw_ek_Kreditor;
    PRINT CONCAT('Deleted from dim.Kreditor: ', @@ROWCOUNT);

    DELETE mk
    FROM [map].[Kreditor] mk
    INNER JOIN #person_only p ON p.dw_ek_Kreditor = mk.dw_ek_Kreditor;
    PRINT CONCAT('Deleted from map.Kreditor: ', @@ROWCOUNT);

    -- 3) Clear the sticky ILS flag where only Person rows supplied it
    UPDATE mk
    SET mk.dw_source_ils = 0,
        mk.dw_timestamp_update = CAST(SYSUTCDATETIME() AS datetime2(0))
    FROM [map].[Kreditor] mk
    WHERE mk.dw_source_ils = 1
      AND mk.dw_ek_Kreditor <> -1
      AND NOT EXISTS (
          SELECT 1 FROM #ils_codes i
          WHERE i.er_person = 0 AND i.clean_code_kreditor = mk.clean_code_kreditor
      );
    PRINT CONCAT('Cleared map.Kreditor.dw_source_ils: ', @@ROWCOUNT);

    -- 4) Full dim_Kreditor reprocess on the next run (see the header)
    IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
        DELETE FROM [meta].[DFMState]
        WHERE [scope] = 'staging' AND [target_type] = 'dimension'
          AND [schema_name] = 'dim' AND [table_name] = 'Kreditor';

    DROP TABLE IF EXISTS #ils_codes;
    DROP TABLE IF EXISTS #other_codes;
    DROP TABLE IF EXISTS #person_only;
END
ELSE
    PRINT 'No ILS/Kreditor archives in this database (DSO) - nothing to clean up';
GO
