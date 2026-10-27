-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.EpisodeOfCare.period_start / period_end: NVARCHAR(50) -> DATETIME2(0).
--
-- The text is first rewritten as 'yyyy-mm-dd hh:mi:ss', the same rule the SoF load
-- applies (normalize_fhir_datetime with keep_wall_clock): the clock time as IFDB sent
-- it, which is Danish local time, with any offset dropped and not converted,
-- sub-seconds truncated, YYYY / YYYY-MM padded. Then the
-- column type is changed in place, so the column keeps its position. A value that
-- cannot be parsed stops the migration with a count before the type change, and
-- its text is kept. Rerunning the rewrite on already rewritten text leaves it
-- unchanged.
--
-- Needs the SoF load to normalise the columns (normalized_datetime_columns for
-- episode_of_care_view); code without it fails to load the table.
--
-- Base DDL: tables/fhir/01_tables.sql. Runs on staging and DSO and can be rerun.
-- ============================================================================

SET XACT_ABORT ON;
GO

-- 1. EpisodeOfCare period columns to DATETIME2(0).
IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
         AND name = N'period_start'
         AND system_type_id = TYPE_ID(N'nvarchar')
   )
BEGIN
    EXEC(N'
        UPDATE e
        SET [period_start] = CONVERT(NVARCHAR(19), v.ts, 120)
        FROM [fhir].[EpisodeOfCare] AS e
        CROSS APPLY (SELECT CASE
            WHEN e.[period_start] LIKE N''[0-9][0-9][0-9][0-9]''
                THEN TRY_CONVERT(DATETIME2(0), e.[period_start] + N''-01-01'', 120)
            WHEN e.[period_start] LIKE N''[0-9][0-9][0-9][0-9]-[0-9][0-9]''
                THEN TRY_CONVERT(DATETIME2(0), e.[period_start] + N''-01'', 120)
            ELSE TRY_CONVERT(DATETIME2(0), REPLACE(LEFT(e.[period_start], 19), N''T'', N'' ''), 120)
        END) AS v(ts)
        WHERE e.[period_start] IS NOT NULL AND v.ts IS NOT NULL;

        DECLARE @unparseable BIGINT = (
            SELECT COUNT_BIG(*) FROM [fhir].[EpisodeOfCare]
            WHERE [period_start] IS NOT NULL AND TRY_CONVERT(DATETIME2(0), [period_start], 120) IS NULL
        );
        IF @unparseable > 0
        BEGIN
            DECLARE @message NVARCHAR(2048) = CONCAT(
                N''fhir.EpisodeOfCare.period_start: '', @unparseable,
                N'' values cannot be parsed as a date and time; the column type is not changed.'');
            THROW 50000, @message, 1;
        END
    ');
    EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [period_start] DATETIME2(0) NULL;');
END
GO

IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
         AND name = N'period_end'
         AND system_type_id = TYPE_ID(N'nvarchar')
   )
BEGIN
    EXEC(N'
        UPDATE e
        SET [period_end] = CONVERT(NVARCHAR(19), v.ts, 120)
        FROM [fhir].[EpisodeOfCare] AS e
        CROSS APPLY (SELECT CASE
            WHEN e.[period_end] LIKE N''[0-9][0-9][0-9][0-9]''
                THEN TRY_CONVERT(DATETIME2(0), e.[period_end] + N''-01-01'', 120)
            WHEN e.[period_end] LIKE N''[0-9][0-9][0-9][0-9]-[0-9][0-9]''
                THEN TRY_CONVERT(DATETIME2(0), e.[period_end] + N''-01'', 120)
            ELSE TRY_CONVERT(DATETIME2(0), REPLACE(LEFT(e.[period_end], 19), N''T'', N'' ''), 120)
        END) AS v(ts)
        WHERE e.[period_end] IS NOT NULL AND v.ts IS NOT NULL;

        DECLARE @unparseable BIGINT = (
            SELECT COUNT_BIG(*) FROM [fhir].[EpisodeOfCare]
            WHERE [period_end] IS NOT NULL AND TRY_CONVERT(DATETIME2(0), [period_end], 120) IS NULL
        );
        IF @unparseable > 0
        BEGIN
            DECLARE @message NVARCHAR(2048) = CONCAT(
                N''fhir.EpisodeOfCare.period_end: '', @unparseable,
                N'' values cannot be parsed as a date and time; the column type is not changed.'');
            THROW 50000, @message, 1;
        END
    ');
    EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [period_end] DATETIME2(0) NULL;');
END
GO

-- 2. SELECT * views keep the column list from their creation.
IF OBJECT_ID(N'[vw_fhir].[EpisodeOfCare]', 'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[EpisodeOfCare]';
GO
